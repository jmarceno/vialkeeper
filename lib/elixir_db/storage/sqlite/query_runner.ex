defmodule ElixirDB.Storage.SQLite.QueryRunner do
  @moduledoc """
  Query execution and explain helpers for the Version 1 SQLite adapter.

  Owns candidate gathering, selector filtering, sort/cursor application, and
  projection. Index catalog reads go through `IndexCatalog`; planner selection
  remains storage-neutral in `ElixirDB.Query.Planner`.
  """

  alias ElixirDB.JSON.Pointer
  alias ElixirDB.JSON.StrictDecoder
  alias ElixirDB.MapAccess
  alias ElixirDB.Observability.Instrumentation.SQLite
  alias ElixirDB.Query.{Plan, Planner, Predicate, Projection}
  alias ElixirDB.Query.Selector
  alias ElixirDB.Storage.SQLite.Adapter
  alias ElixirDB.Storage.SQLite.{Connection, FullTextIndexes, IndexCatalog, QueryCompiler}

  @doc """
  Executes a normalized query request against an open adapter.
  """
  @spec execute(map(), map()) :: {:ok, map()} | {:error, ElixirDB.Error.t()}
  def execute(adapter, request), do: execute(adapter, request, nil)

  @spec execute(map(), map(), map() | nil) :: {:ok, map()} | {:error, ElixirDB.Error.t()}
  def execute(adapter, request, supplied_identity) do
    started_native = System.monotonic_time()

    identity =
      supplied_identity ||
        SQLite.trace_sqlite_phase(:query_identity, fn -> adapter_identity(adapter) end)

    deadline = query_deadline(identity, started_native)

    with {:ok, indexes} <-
           SQLite.trace_sqlite_phase(:query_index_catalog, fn ->
             IndexCatalog.list(adapter.conn)
           end),
         :ok <- check_deadline(deadline),
         {:ok, plan} <-
           SQLite.trace_sqlite_phase(:query_plan, fn -> plan_request(indexes, request) end),
         :ok <- check_deadline(deadline),
         :ok <- validate_bookmark_plan(request, plan),
         limit <-
           value_or_default(
             MapAccess.get(request, :limit),
             get_in(identity, [:config, "queries", "default_limit"]) || 50
           ),
         {:ok, documents, examined} <-
           SQLite.trace_sqlite_phase(
             :query_candidates,
             [
               plan_kind: plan.kind,
               selected_index_count: length(Plan.index_bindings(plan))
             ],
             fn -> candidate_documents(adapter, plan, indexes, request, deadline, limit) end
           ),
         {:ok, matched} <-
           SQLite.trace_sqlite_phase(:query_filter, [entries: length(documents)], fn ->
             filter_query(documents, request, plan, deadline)
           end),
         :ok <- check_deadline(deadline),
         :ok <- enforce_scan_limit(plan, examined, identity),
         {:ok, ordered} <-
           SQLite.trace_sqlite_phase(:query_sort, [entries: length(matched)], fn ->
             sort_documents(matched, request, deadline)
           end),
         {:ok, ordered} <-
           SQLite.trace_sqlite_phase(:query_cursor, [entries: length(ordered)], fn ->
             apply_after_cursor(ordered, request, deadline)
           end),
         values <- Enum.take(ordered, limit),
         :ok <- check_deadline(deadline),
         {:ok, projected} <-
           SQLite.trace_sqlite_phase(:query_project, [entries: length(values)], fn ->
             project_documents(values, request, deadline)
           end),
         selected_metadata <- selected_metadata(plan) do
      {:ok,
       %{
         results: projected,
         documents: projected,
         bookmark: nil,
         has_more: length(ordered) > limit,
         examined: examined,
         sequence: identity.current_sequence,
         selected_index: selected_metadata.index_id,
         index_digest: selected_metadata.definition_digest,
         plan_kind: plan.kind,
         plan_digest: plan.digest,
         index_bindings: Plan.index_bindings(plan),
         selected_indexes: Enum.map(Plan.index_bindings(plan), & &1.index_id),
         last_ordering_key: ordering_key(List.last(values), request)
       }}
    else
      {:error, %ElixirDB.Error{} = error} -> {:error, error}
      {:error, reason} -> {:error, normalize_error(reason)}
    end
  end

  defp plan_request(indexes, request) do
    case Planner.plan(indexes, request) do
      {:ok, plan} ->
        {:ok, plan}

      {:error, error} ->
        case MapAccess.get(request, :bookmark_payload) do
          nil -> {:error, error}
          _ -> {:error, ElixirDB.Error.invalid_bookmark("bookmark plan cannot be reproduced")}
        end
    end
  end

  defp validate_bookmark_plan(request, %Plan{} = plan) do
    case MapAccess.get(request, :bookmark_payload) do
      nil ->
        :ok

      bookmark ->
        expected_bindings =
          Enum.map(Plan.index_bindings(plan), fn binding ->
            %{
              "index_id" => binding.index_id,
              "definition_digest" => binding.definition_digest
            }
          end)

        actual_bindings = MapAccess.get(bookmark, :index_bindings)
        actual_digest = MapAccess.get(bookmark, :plan_digest)

        case {actual_digest, actual_bindings} do
          {digest, bindings} when digest == plan.digest and bindings == expected_bindings ->
            :ok

          _ ->
            {:error, ElixirDB.Error.invalid_bookmark("bookmark is bound to another plan")}
        end
    end
  end

  @doc """
  Builds an explain payload for a query request.
  """
  @spec explain(map(), map()) :: {:ok, map()} | {:error, ElixirDB.Error.t()}
  def explain(adapter, request) do
    with {:ok, indexes} <- IndexCatalog.list(adapter.conn),
         {:ok, plan} <- Planner.plan(indexes, request),
         {:ok, [[count]]} <-
           Connection.query(
             adapter.conn,
             "SELECT count(*) FROM documents WHERE winning_deleted = 0"
           ),
         identity <- adapter_identity(adapter),
         scan_threshold <- get_in(identity, [:config, "queries", "scan_threshold"]) || 1_000,
         {:ok, candidate_count} <-
           explain_candidate_count(
             adapter,
             plan,
             indexes,
             request,
             count,
             query_deadline(identity, System.monotonic_time())
           ) do
      {:ok,
       %{
         plan_kind: plan.kind,
         plan_digest: plan.digest,
         selected_indexes: Enum.map(Plan.index_bindings(plan), & &1.index_id),
         selected_index: first_binding_id(plan),
         union_branches: Enum.map(plan.scans, &Map.take(&1, ["branch", "index_id"])),
         candidate_indexes: Enum.map(indexes, & &1["index_id"]),
         rejected_index_reasons: rejected_index_reasons(indexes, plan, request),
         pushdown_predicates: plan_pushdowns(plan),
         post_filter_predicates:
           Predicate.post_filter_predicates(
             MapAccess.get(request, :predicate, :match_all),
             plan_pushdowns(plan)
           ),
         full_scan: plan.kind == :bounded_scan,
         candidate_count: candidate_count,
         scan_allowed: plan.kind != :bounded_scan or candidate_count < scan_threshold,
         selector: MapAccess.get(request, :selector, %{}),
         sort: MapAccess.get(request, :sort, []),
         pagination: plan.pagination,
         sort_compatible: plan.sort_compatible?
       }}
    else
      {:error, reason} -> {:error, normalize_error(reason)}
    end
  end

  defp explain_candidate_count(
         _adapter,
         %Plan{kind: :bounded_scan},
         _indexes,
         _request,
         count,
         _deadline
       ),
       do: {:ok, count}

  defp explain_candidate_count(adapter, plan, indexes, request, _count, deadline) do
    case candidate_documents(adapter, plan, indexes, request, deadline) do
      {:ok, _documents, examined} -> {:ok, examined}
      {:error, _} = error -> error
    end
  end

  defp candidate_documents(adapter, plan, indexes, request, deadline),
    do: candidate_documents(adapter, plan, indexes, request, deadline, nil)

  defp candidate_documents(
         adapter,
         %Plan{kind: :bounded_scan},
         _indexes,
         _request,
         deadline,
         _limit
       ) do
    with {:ok, [[count]]} <-
           Connection.query(
             adapter.conn,
             "SELECT count(*) FROM documents WHERE winning_deleted = 0"
           ),
         :ok <- check_deadline(deadline),
         :ok <-
           if(
             count <
               (get_in(adapter_identity(adapter), [:config, "queries", "scan_threshold"]) || 1_000),
             do: :ok,
             else:
               {:error,
                ElixirDB.Error.index_required("query requires a compatible index", %{
                  candidate_count: count,
                  threshold:
                    get_in(adapter_identity(adapter), [:config, "queries", "scan_threshold"]) ||
                      1_000
                })}
           ),
         :ok <- check_deadline(deadline),
         {:ok, rows} <-
           Connection.query(
             adapter.conn,
             "SELECT document_id, winning_revision, winning_body_json FROM documents WHERE winning_deleted = 0 ORDER BY document_id"
           ),
         :ok <- check_deadline(deadline),
         {:ok, documents} <- decode_query_documents(rows) do
      {:ok, documents, count}
    end
  end

  defp candidate_documents(
         adapter,
         %Plan{kind: :full_text} = plan,
         indexes,
         request,
         deadline,
         _limit
       ) do
    selected = selected_index(indexes, plan)

    if selected && MapAccess.get(request, :search) do
      search = MapAccess.get(request, :search)
      metadata = Map.merge(selected, selected["_metadata"] || %{})

      with {:ok, rows} <-
             FullTextIndexes.search(
               adapter.conn,
               metadata,
               MapAccess.get(search, :text),
               MapAccess.get(search, :mode, "all"),
               deadline
             ),
           :ok <- check_deadline(deadline) do
        {:ok, full_text_documents(rows), length(rows)}
      end
    else
      {:error, ElixirDB.Error.invalid_index_hint("full-text plan cannot be executed", %{})}
    end
  end

  defp candidate_documents(
         adapter,
         %Plan{kind: :single} = plan,
         indexes,
         request,
         deadline,
         limit
       ) do
    selected = selected_index(indexes, plan)
    scan = List.first(plan.scans)

    with {:ok, rows} <- structured_candidate_rows(adapter, selected, scan),
         :ok <- check_deadline(deadline),
         {:ok, documents} <-
           decode_query_documents(candidate_rows_for_decode(rows, plan, request, limit)) do
      {:ok, documents, length(rows)}
    end
  end

  defp candidate_documents(
         adapter,
         %Plan{kind: :union} = plan,
         indexes,
         _request,
         deadline,
         _limit
       ) do
    Enum.reduce_while(plan.scans, {:ok, [], 0}, fn scan, {:ok, candidates, examined} ->
      selected = selected_index(indexes, scan["index_id"])

      with :ok <- check_deadline(deadline),
           {:ok, rows} <- structured_candidate_rows(adapter, selected, scan),
           :ok <- check_deadline(deadline) do
        {:cont, {:ok, [rows | candidates], examined + length(rows)}}
      else
        {:error, _} = error -> {:halt, error}
      end
    end)
    |> case do
      {:ok, rows, _examined} ->
        rows = rows |> Enum.reverse() |> Enum.concat() |> Enum.uniq_by(&List.first/1)
        unique_examined = length(rows)

        with :ok <- check_deadline(deadline),
             {:ok, documents} <- decode_query_documents(rows),
             :ok <- check_deadline(deadline) do
          {:ok, documents, unique_examined}
        end

      {:error, _} = error ->
        error
    end
  end

  defp full_text_documents(rows),
    do:
      Enum.map(rows, fn row ->
        %{id: row.id, revision: row.revision, body: row.body, rank: row.rank}
      end)

  defp selected_index(indexes, %Plan{selected_indexes: [binding | _]}),
    do: selected_index(indexes, binding.index_id)

  defp selected_index(indexes, index_id),
    do: Enum.find(indexes, &(&1["index_id"] == index_id))

  defp structured_candidate_rows(_adapter, nil, _scan),
    do: {:error, ElixirDB.Error.invalid_index_hint("planned index no longer exists", %{})}

  defp structured_candidate_rows(adapter, selected, scan) do
    fields = selected["fields"] || []

    with {:ok, conditions} <- QueryCompiler.compile_scan(scan, fields) do
      where = ["winning_deleted = 0" | Enum.map(conditions, &elem(&1, 0))] |> Enum.join(" AND ")
      params = Enum.flat_map(conditions, fn {_sql, value} -> List.wrap(value) end)

      Connection.query(
        adapter.conn,
        "SELECT document_id, winning_revision, winning_body_json FROM documents WHERE #{where} ORDER BY document_id",
        params
      )
    end
  end

  defp rejected_index_reasons(indexes, plan, request) do
    selected = MapSet.new(Enum.map(Plan.index_bindings(plan), & &1.index_id))

    Enum.map(indexes, fn index ->
      {index["index_id"],
       if(MapSet.member?(selected, index["index_id"]), do: :selected, else: :incompatible)}
    end)
    |> Map.new()
    |> Map.put(:request, MapAccess.get(request, :index))
  end

  defp enforce_scan_limit(%Plan{kind: kind}, _examined, _identity) when kind != :bounded_scan,
    do: :ok

  defp enforce_scan_limit(%Plan{kind: :bounded_scan}, examined, identity) do
    threshold = get_in(identity, [:config, "queries", "scan_threshold"]) || 1_000

    if examined < threshold,
      do: :ok,
      else:
        {:error,
         ElixirDB.Error.index_required("query requires a compatible index", %{
           candidate_count: examined,
           threshold: threshold
         })}
  end

  defp project_documents(values, request, deadline) do
    with :ok <- check_deadline(deadline),
         projected <- Enum.map(values, &Projection.project(&1, request)),
         :ok <- check_deadline(deadline) do
      {:ok, projected}
    end
  end

  defp decode_query_documents(rows) do
    Enum.reduce_while(rows, {:ok, []}, fn [id, revision, body_json], {:ok, acc} ->
      case StrictDecoder.decode(body_json) do
        {:ok, body} -> {:cont, {:ok, [%{id: id, revision: revision, body: body} | acc]}}
        {:error, error} -> {:halt, {:error, error}}
      end
    end)
    |> case do
      {:ok, values} -> {:ok, Enum.reverse(values)}
      error -> error
    end
  end

  defp candidate_rows_for_decode(rows, %Plan{kind: :single} = plan, request, limit)
       when is_integer(limit) and limit >= 0 do
    if fully_pushed_default_order_query?(plan, request),
      do: Enum.take(rows, limit + 1),
      else: rows
  end

  defp candidate_rows_for_decode(rows, _plan, _request, _limit), do: rows

  defp fully_pushed_default_order_query?(plan, request) do
    is_nil(MapAccess.get(request, :search)) and
      MapAccess.get(request, :sort, []) == [] and
      is_nil(MapAccess.get(request, :after_id)) and
      is_nil(MapAccess.get(request, :after_ordering)) and
      no_post_filter?(plan, request)
  end

  defp no_post_filter?(plan, request) do
    case MapAccess.get(request, :predicate) do
      nil -> false
      predicate -> Predicate.post_filter_predicates(predicate, plan_pushdowns(plan)) == []
    end
  end

  defp filter_query(documents, request, plan, deadline) do
    predicate = MapAccess.get(request, :predicate)

    if is_nil(predicate) do
      {:error, ElixirDB.Error.invalid_request("normalized predicate is required")}
    else
      if no_post_filter?(plan, request),
        do: {:ok, documents},
        else: filter_documents(documents, predicate, deadline)
    end
  end

  defp filter_documents(documents, predicate, deadline) do
    documents
    |> Enum.with_index()
    |> Enum.reduce_while({:ok, []}, &filter_document(&1, &2, predicate, deadline))
    |> case do
      {:ok, values} -> {:ok, Enum.reverse(values)}
      error -> error
    end
  end

  defp filter_document({document, index}, {:ok, acc}, predicate, deadline) do
    case periodic_deadline_check(deadline, index) do
      :ok -> filter_document_match(Selector.matches?(document.body, predicate), document, acc)
      {:error, _} = error -> {:halt, error}
    end
  end

  defp filter_document_match({:ok, true}, document, acc),
    do: {:cont, {:ok, [document | acc]}}

  defp filter_document_match({:ok, false}, _document, acc), do: {:cont, {:ok, acc}}
  defp filter_document_match({:error, error}, _document, _acc), do: {:halt, {:error, error}}

  defp sort_documents(documents, request, deadline) do
    sort = MapAccess.get(request, :sort, [])

    with :ok <- check_deadline(deadline) do
      sort_documents_after_check(documents, sort, request, deadline)
    end
  end

  defp sort_documents_after_check(documents, sort, request, deadline) do
    sorted =
      if sort == [] and not is_nil(MapAccess.get(request, :search)) do
        documents
      else
        Enum.sort(documents, fn left, right -> compare_documents(left, right, sort) end)
      end

    case check_deadline(deadline) do
      :ok -> {:ok, sorted}
      {:error, _} = error -> error
    end
  end

  defp apply_after_cursor(documents, request, deadline) do
    with :ok <- check_deadline(deadline) do
      values = cursor_values(documents, request)

      case check_deadline(deadline) do
        :ok -> {:ok, values}
        {:error, _} = error -> error
      end
    end
  end

  defp cursor_values(documents, request) do
    case MapAccess.get(request, :after_ordering) do
      after_ordering when is_map(after_ordering) ->
        sort = MapAccess.get(request, :sort, [])

        Enum.drop_while(documents, fn document ->
          compare_ordering_keys(ordering_key(document, request), after_ordering, sort) != :gt
        end)

      _ ->
        case MapAccess.get(request, :after_id) do
          nil -> documents
          after_id -> Enum.drop_while(documents, &(&1.id <= after_id))
        end
    end
  end

  defp compare_ordering_keys(left, right, []) do
    case {Map.get(left, "rank"), Map.get(right, "rank")} do
      {left_rank, right_rank} when is_number(left_rank) and is_number(right_rank) ->
        case compare_values({:ok, left_rank}, {:ok, right_rank}) do
          :eq -> compare_ids(left["id"], right["id"])
          comparison -> comparison
        end

      _ ->
        compare_ids(left["id"], right["id"])
    end
  end

  defp compare_ordering_keys(left, right, [sort | rest]) do
    left_value = ordering_value(first_ordering_value(left))
    right_value = ordering_value(first_ordering_value(right))

    case compare_values(left_value, right_value) do
      :eq ->
        compare_ordering_keys(drop_ordering_key(left), drop_ordering_key(right), rest)

      comparison ->
        apply_ordering_direction(comparison, sort)
    end
  end

  defp first_ordering_value(%{"sort" => [value | _]}), do: value
  defp first_ordering_value(_), do: nil

  defp drop_ordering_key(value),
    do:
      %{"sort" => Enum.drop(value["sort"] || [], 1), "id" => value["id"]}
      |> maybe_put_rank(Map.get(value, "rank"))

  defp maybe_put_rank(value, rank) when is_number(rank), do: Map.put(value, "rank", rank)
  defp maybe_put_rank(value, _rank), do: value

  defp apply_ordering_direction(:lt, sort),
    do: if(MapAccess.get(sort, :direction, "asc") == "asc", do: :lt, else: :gt)

  defp apply_ordering_direction(:gt, sort),
    do: if(MapAccess.get(sort, :direction, "asc") == "asc", do: :gt, else: :lt)

  defp compare_ids(left, right) when left == right, do: :eq
  defp compare_ids(left, right) when left < right, do: :lt
  defp compare_ids(_left, _right), do: :gt

  defp ordering_value(%{"present" => true, "value" => value}), do: {:ok, value}
  defp ordering_value(_), do: :missing

  defp ordering_key(nil, _request), do: nil

  defp ordering_key(document, request) do
    sort = MapAccess.get(request, :sort, [])

    values =
      Enum.map(sort, fn sort_field ->
        path = MapAccess.get(sort_field, :path)

        case Pointer.get(document.body, path) do
          {:ok, value} -> %{"present" => true, "value" => value}
          :missing -> %{"present" => false}
        end
      end)

    %{"sort" => values, "id" => document.id}
    |> maybe_put_search_rank(document, sort)
  end

  defp maybe_put_search_rank(ordering_key, %{rank: rank}, []) when is_number(rank),
    do: Map.put(ordering_key, "rank", rank)

  defp maybe_put_search_rank(ordering_key, _document, _sort), do: ordering_key

  defp compare_documents(left, right, []), do: left.id <= right.id

  defp compare_documents(left, right, [sort | rest]) do
    path = MapAccess.get(sort, :path)
    direction = MapAccess.get(sort, :direction, "asc")
    left_value = Pointer.get(left.body, path)
    right_value = Pointer.get(right.body, path)

    case compare_values(left_value, right_value) do
      :eq -> compare_documents(left, right, rest)
      :lt -> direction == "asc"
      :gt -> direction == "desc"
    end
  end

  defp compare_values(:missing, :missing), do: :eq
  defp compare_values(:missing, _), do: :gt
  defp compare_values(_, :missing), do: :lt
  defp compare_values({:ok, left}, {:ok, right}) when left == right, do: :eq
  defp compare_values({:ok, left}, {:ok, right}) when left < right, do: :lt
  defp compare_values({:ok, _}, {:ok, _}), do: :gt
  defp compare_values(_, _), do: :eq

  defp adapter_identity(adapter) do
    case Adapter.identity(adapter) do
      {:ok, value} -> value
      _ -> %{current_sequence: 0, config: ElixirDB.Config.defaults()}
    end
  end

  defp query_deadline(identity, started_native) do
    maximum_ms = get_in(identity, [:config, "queries", "max_execution_ms"]) || 5_000
    {started_native + System.convert_time_unit(maximum_ms, :millisecond, :native), maximum_ms}
  end

  defp check_deadline({deadline, maximum_ms}) do
    if System.monotonic_time() < deadline do
      :ok
    else
      {:error,
       ElixirDB.Error.resource_limit("query execution exceeded the configured limit", %{
         maximum_ms: maximum_ms
       })}
    end
  end

  defp periodic_deadline_check(deadline, index) do
    if rem(index, 32) == 0, do: check_deadline(deadline), else: :ok
  end

  defp normalize_error(%ElixirDB.Error{} = error), do: error

  defp normalize_error(reason),
    do: ElixirDB.Error.internal_error("SQLite operation failed", %{cause: inspect(reason)})

  defp selected_metadata(%Plan{selected_indexes: [binding | _]}), do: binding
  defp selected_metadata(%Plan{}), do: %{index_id: nil, definition_digest: nil}

  defp first_binding_id(%Plan{selected_indexes: [binding | _]}), do: binding.index_id
  defp first_binding_id(%Plan{}), do: nil

  defp plan_pushdowns(%Plan{scans: scans}),
    do: Enum.flat_map(scans, &Map.get(&1, "constraints", []))

  defp value_or_default(nil, default), do: default
  defp value_or_default(value, _default), do: value
end
