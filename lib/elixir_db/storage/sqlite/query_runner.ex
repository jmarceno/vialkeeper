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
  alias ElixirDB.Query.{Planner, Projection}
  alias ElixirDB.Query.Selector
  alias ElixirDB.Storage.SQLite.Adapter
  alias ElixirDB.Storage.SQLite.{Connection, FullTextIndexes, IndexCatalog, QueryCompiler}

  @doc """
  Executes a normalized query request against an open adapter.
  """
  @spec execute(map(), map()) :: {:ok, map()} | {:error, ElixirDB.Error.t()}
  def execute(adapter, request) do
    with {:ok, indexes} <- IndexCatalog.list(adapter.conn),
         {:ok, selected} <- Planner.select_index(indexes, request),
         {:ok, documents, examined} <- candidate_documents(adapter, selected, request),
         {:ok, matched} <- filter_query(documents, request),
         identity <- adapter_identity(adapter),
         :ok <- enforce_scan_limit(selected, examined, identity),
         limit <-
           MapAccess.get(request, :limit) ||
             get_in(identity, [:config, "queries", "default_limit"]) || 50,
         ordered <- matched |> sort_documents(request) |> apply_after_cursor(request),
         values <- Enum.take(ordered, limit),
         {:ok, projected} <- project_documents(values, request) do
      {:ok,
       %{
         results: projected,
         documents: projected,
         bookmark: nil,
         has_more: length(ordered) > limit,
         examined: examined,
         sequence: identity.current_sequence,
         selected_index: selected && selected["index_id"],
         index_digest: selected && selected["definition_digest"],
         last_ordering_key: ordering_key(List.last(values), request)
       }}
    else
      {:error, %ElixirDB.Error{} = error} -> {:error, error}
      {:error, reason} -> {:error, normalize_error(reason)}
    end
  end

  @doc """
  Builds an explain payload for a query request.
  """
  @spec explain(map(), map()) :: {:ok, map()} | {:error, ElixirDB.Error.t()}
  def explain(adapter, request) do
    with {:ok, indexes} <- IndexCatalog.list(adapter.conn),
         {:ok, selected} <- Planner.select_index(indexes, request),
         {:ok, [[count]]} <-
           Connection.query(
             adapter.conn,
             "SELECT count(*) FROM documents WHERE winning_deleted = 0"
           ),
         identity <- adapter_identity(adapter),
         scan_threshold <- get_in(identity, [:config, "queries", "scan_threshold"]) || 1_000 do
      {:ok,
       %{
         selected_index: selected && selected["index_id"],
         candidate_indexes: Enum.map(indexes, & &1["index_id"]),
         rejected_index_reasons: rejected_index_reasons(indexes, selected, request),
         full_scan: is_nil(selected),
         candidate_count: count,
         scan_allowed: not is_nil(selected) or count < scan_threshold,
         selector: MapAccess.get(request, :selector, %{}),
         sort: MapAccess.get(request, :sort, []),
         pagination: if(selected, do: :indexed, else: :bounded_scan)
       }}
    else
      {:error, reason} -> {:error, normalize_error(reason)}
    end
  end

  defp candidate_documents(adapter, nil, _request) do
    with {:ok, [[count]]} <-
           Connection.query(
             adapter.conn,
             "SELECT count(*) FROM documents WHERE winning_deleted = 0"
           ),
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
         {:ok, rows} <-
           Connection.query(
             adapter.conn,
             "SELECT document_id, winning_revision, winning_body_json FROM documents WHERE winning_deleted = 0 ORDER BY document_id"
           ),
         {:ok, documents} <- decode_query_documents(rows) do
      {:ok, documents, count}
    end
  end

  defp candidate_documents(adapter, selected, request) do
    if selected["type"] == "full_text" and MapAccess.get(request, :search) do
      search = MapAccess.get(request, :search)
      metadata = Map.merge(selected, selected["_metadata"] || %{})

      with {:ok, rows} <-
             FullTextIndexes.search(
               adapter.conn,
               metadata,
               MapAccess.get(search, :text),
               MapAccess.get(search, :mode, "all")
             ) do
        {:ok, full_text_documents(rows), length(rows)}
      end
    else
      with {:ok, rows} <- structured_candidate_rows(adapter, selected, request),
           {:ok, documents} <- decode_query_documents(rows) do
        {:ok, documents, length(documents)}
      end
    end
  end

  defp full_text_documents(rows),
    do:
      Enum.map(rows, fn row ->
        %{id: row.id, revision: row.revision, body: row.body, rank: row.rank}
      end)

  defp structured_candidate_rows(adapter, selected, request) do
    fields = selected["fields"] || []
    selector = MapAccess.get(request, :selector, %{})

    with {:ok, conditions} <- QueryCompiler.structured_conditions(selector, fields) do
      where = ["winning_deleted = 0" | Enum.map(conditions, &elem(&1, 0))] |> Enum.join(" AND ")
      params = Enum.flat_map(conditions, fn {_sql, value} -> List.wrap(value) end)

      Connection.query(
        adapter.conn,
        "SELECT document_id, winning_revision, winning_body_json FROM documents WHERE #{where} ORDER BY document_id",
        params
      )
    end
  end

  defp rejected_index_reasons(indexes, selected, request) do
    Enum.map(indexes, fn index ->
      {index["index_id"],
       if(selected && selected["index_id"] == index["index_id"], do: :selected, else: :incompatible)}
    end)
    |> Map.new()
    |> Map.put(:request, MapAccess.get(request, :index))
  end

  defp enforce_scan_limit(selected, _examined, _identity) when not is_nil(selected), do: :ok

  defp enforce_scan_limit(nil, examined, identity) do
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

  defp project_documents(values, request),
    do: {:ok, Enum.map(values, &Projection.project(&1, request))}

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

  defp filter_query(documents, request) do
    selector = MapAccess.get(request, :selector, %{})

    result =
      Enum.reduce_while(documents, {:ok, []}, fn document, {:ok, acc} ->
        case Selector.matches?(document.body, selector) do
          {:ok, true} -> {:cont, {:ok, [document | acc]}}
          {:ok, false} -> {:cont, {:ok, acc}}
          {:error, error} -> {:halt, {:error, error}}
        end
      end)

    with {:ok, values} <- result do
      values = values |> Enum.reverse() |> sort_documents(request)

      {:ok, values}
    end
  end

  defp sort_documents(documents, request) do
    sort = MapAccess.get(request, :sort, [])

    if sort == [] and not is_nil(MapAccess.get(request, :search)) do
      documents
    else
      Enum.sort(documents, fn left, right -> compare_documents(left, right, sort) end)
    end
  end

  defp apply_after_cursor(documents, request) do
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

  defp compare_ordering_keys(left, right, []), do: compare_ids(left["id"], right["id"])

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
    do: %{"sort" => Enum.drop(value["sort"] || [], 1), "id" => value["id"]}

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
  end

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

  defp normalize_error(%ElixirDB.Error{} = error), do: error

  defp normalize_error(reason),
    do: ElixirDB.Error.internal_error("SQLite operation failed", %{cause: inspect(reason)})
end
