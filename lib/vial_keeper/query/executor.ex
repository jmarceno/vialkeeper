defmodule VialKeeper.Query.Executor do
  @moduledoc """
  Shared query execution after candidate retrieval.

  Owns deadline handling, bookmark validation, selector post-filtering,
  canonical ordering, cursors, projection, limit/has-more assembly, and public
  explain shaping. Full-text matching is authoritative in the search engine
  that produced the candidates. Candidate gathering stays behind storage ports;
  this module never assumes an engine query language, physical identifiers, or
  scan order.
  """

  alias VialKeeper.MapAccess
  alias VialKeeper.Query.{Ordering, Plan, Predicate, Projection, Selector}

  @type deadline :: {integer(), pos_integer()}
  @type document :: map()

  @doc "Builds a query deadline from identity config and a native start time."
  @spec deadline(map(), integer()) :: deadline()
  def deadline(identity, started_native) when is_map(identity) and is_integer(started_native) do
    maximum_ms = get_in(identity, [:config, "queries", "max_execution_ms"]) || 5_000
    {started_native + System.convert_time_unit(maximum_ms, :millisecond, :native), maximum_ms}
  end

  @doc "Returns `:ok` while the deadline has not elapsed."
  @spec check_deadline(deadline()) :: :ok | {:error, VialKeeper.Error.t()}
  def check_deadline({deadline, maximum_ms}) do
    if System.monotonic_time() < deadline do
      :ok
    else
      {:error,
       VialKeeper.Error.resource_limit("query execution exceeded the configured limit", %{
         maximum_ms: maximum_ms
       })}
    end
  end

  @doc "Checks the deadline every 32nd document during long scans."
  @spec periodic_deadline_check(deadline(), non_neg_integer()) ::
          :ok | {:error, VialKeeper.Error.t()}
  def periodic_deadline_check(deadline, index) when is_integer(index) do
    if rem(index, 32) == 0, do: check_deadline(deadline), else: :ok
  end

  @doc "Validates that a bookmark payload still matches the planned digest."
  @spec validate_bookmark_plan(map(), Plan.t()) :: :ok | {:error, VialKeeper.Error.t()}
  def validate_bookmark_plan(request, %Plan{} = plan) when is_map(request) do
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
            {:error, VialKeeper.Error.invalid_bookmark("bookmark is bound to another plan")}
        end
    end
  end

  @doc "Turns a planner failure into an invalid-bookmark error when a bookmark is present."
  @spec plan_error(map(), VialKeeper.Error.t()) :: {:error, VialKeeper.Error.t()}
  def plan_error(request, error) when is_map(request) do
    case MapAccess.get(request, :bookmark_payload) do
      nil -> {:error, error}
      _ -> {:error, VialKeeper.Error.invalid_bookmark("bookmark plan cannot be reproduced")}
    end
  end

  @doc "Enforces the configured bounded-scan threshold against examined candidates."
  @spec enforce_scan_limit(Plan.t(), non_neg_integer(), map()) ::
          :ok | {:error, VialKeeper.Error.t()}
  def enforce_scan_limit(%Plan{kind: kind}, _examined, _identity) when kind != :bounded_scan,
    do: :ok

  def enforce_scan_limit(%Plan{kind: :bounded_scan}, examined, identity)
      when is_integer(examined) and is_map(identity) do
    threshold = scan_threshold(identity)

    if examined < threshold,
      do: :ok,
      else:
        {:error,
         VialKeeper.Error.index_required("query requires a compatible index", %{
           candidate_count: examined,
           threshold: threshold
         })}
  end

  @doc "Returns the configured scan threshold."
  @spec scan_threshold(map()) :: pos_integer()
  def scan_threshold(identity) when is_map(identity) do
    get_in(identity, [:config, "queries", "scan_threshold"]) || 1_000
  end

  @doc "Returns the configured default query limit."
  @spec default_limit(map()) :: pos_integer()
  def default_limit(identity) when is_map(identity) do
    get_in(identity, [:config, "queries", "default_limit"]) || 50
  end

  @doc """
  Filters candidates with selector post-filters. Full-text plans trust the
  search engine's candidate set.
  """
  @spec filter_query([document()], map(), Plan.t(), deadline()) ::
          {:ok, [document()]} | {:error, VialKeeper.Error.t()}
  def filter_query(documents, request, %Plan{} = plan, deadline)
      when is_list(documents) and is_map(request) do
    case maybe_filter_full_text(documents, request, plan, deadline) do
      {:ok, documents} -> maybe_filter_selector(documents, request, plan, deadline)
      {:error, _} = error -> error
    end
  end

  @doc "Sorts documents with shared Ordering semantics regardless of candidate order."
  @spec sort_documents([document()], map(), deadline()) ::
          {:ok, [document()]} | {:error, VialKeeper.Error.t()}
  def sort_documents(documents, request, deadline)
      when is_list(documents) and is_map(request) do
    sort = Ordering.compile_sort(MapAccess.get(request, :sort, []))

    with :ok <- check_deadline(deadline),
         sorted <- Ordering.sort_documents(documents, sort),
         :ok <- check_deadline(deadline) do
      {:ok, sorted}
    end
  end

  @doc "Applies after_id / after_ordering cursors."
  @spec apply_after_cursor([document()], map(), deadline()) ::
          {:ok, [document()]} | {:error, VialKeeper.Error.t()}
  def apply_after_cursor(documents, request, deadline)
      when is_list(documents) and is_map(request) do
    with :ok <- check_deadline(deadline),
         values <- cursor_values(documents, request),
         :ok <- check_deadline(deadline) do
      {:ok, values}
    end
  end

  @doc "Projects documents according to the request field list."
  @spec project_documents([document()], map(), deadline()) ::
          {:ok, [document()]} | {:error, VialKeeper.Error.t()}
  def project_documents(values, request, deadline)
      when is_list(values) and is_map(request) do
    with :ok <- check_deadline(deadline),
         {:ok, fields} <- Projection.compile_fields(MapAccess.get(request, :fields)),
         projected <- Enum.map(values, &Projection.project_compiled(&1, fields)),
         :ok <- check_deadline(deadline) do
      {:ok, projected}
    end
  end

  @doc """
  Assembles the public execute_query result map.

  `ordered` is the full post-cursor list; `projected` is the limited page.
  """
  @spec assemble_result([document()], [document()], Plan.t(), map(), map(), non_neg_integer()) ::
          map()
  def assemble_result(projected, ordered, %Plan{} = plan, request, identity, examined)
      when is_list(projected) and is_list(ordered) and is_map(request) and is_map(identity) and
             is_integer(examined) do
    limit = page_limit(request, identity)
    selected_metadata = selected_metadata(plan)
    page_source = Enum.take(ordered, limit)
    index_bindings = Plan.index_bindings(plan)

    %{
      results: projected,
      documents: projected,
      bookmark: nil,
      has_more: length(ordered) > limit,
      examined: examined,
      sequence: Map.get(identity, :current_sequence, 0),
      selected_index: selected_metadata.index_id,
      index_digest: selected_metadata.definition_digest,
      plan_kind: plan.kind,
      plan_digest: plan.digest,
      index_bindings: index_bindings,
      selected_indexes: Enum.map(index_bindings, & &1.index_id),
      last_ordering_key:
        Ordering.ordering_key(
          List.last(page_source),
          Ordering.compile_sort(MapAccess.get(request, :sort, []))
        )
    }
  end

  @doc "Assembles a subscription membership snapshot result."
  @spec assemble_subscription_result([document()], map()) :: map()
  def assemble_subscription_result(projected, identity)
      when is_list(projected) and is_map(identity) do
    %{
      documents: projected,
      member_ids: Enum.map(projected, & &1.id),
      sequence: Map.get(identity, :current_sequence, 0)
    }
  end

  @doc "Enforces the subscription membership bound."
  @spec enforce_subscription_membership_bound([document()], pos_integer()) ::
          :ok | {:error, VialKeeper.Error.t()}
  def enforce_subscription_membership_bound(ordered, max_members)
      when is_list(ordered) and is_integer(max_members) do
    if length(ordered) > max_members do
      {:error,
       VialKeeper.Error.resource_limit("subscription membership exceeds max_members", %{
         maximum: max_members
       })}
    else
      :ok
    end
  end

  @doc "Builds the public explain payload from plan and catalog facts."
  @spec explain_payload(Plan.t(), [map()], map(), map(), non_neg_integer()) :: map()
  def explain_payload(%Plan{} = plan, indexes, request, identity, candidate_count)
      when is_list(indexes) and is_map(request) and is_map(identity) and is_integer(candidate_count) do
    threshold = scan_threshold(identity)

    %{
      plan_kind: plan.kind,
      plan_digest: plan.digest,
      selected_indexes: Enum.map(Plan.index_bindings(plan), & &1.index_id),
      selected_index: first_binding_id(plan),
      union_branches: Enum.map(plan.scans, &Map.take(&1, ["branch", "index_id"])),
      candidate_indexes: Enum.map(indexes, &index_id/1),
      rejected_index_reasons: rejected_index_reasons(indexes, plan, request),
      pushdown_predicates: plan_pushdowns(plan),
      post_filter_predicates:
        Predicate.post_filter_predicates(
          MapAccess.get(request, :predicate, :match_all),
          plan_pushdowns(plan)
        ),
      full_scan: plan.kind == :bounded_scan,
      candidate_count: candidate_count,
      scan_allowed: plan.kind != :bounded_scan or candidate_count < threshold,
      selector: MapAccess.get(request, :selector, %{}),
      sort: MapAccess.get(request, :sort, []),
      pagination: plan.pagination,
      sort_compatible: plan.sort_compatible?,
      backend_detail: %{
        ready_index_count: length(indexes),
        candidate_retrieval: plan.pagination
      }
    }
  end

  @doc "True when the plan has no remaining selector post-filters."
  @spec no_post_filter?(Plan.t(), map()) :: boolean()
  def no_post_filter?(%Plan{} = plan, request) when is_map(request) do
    case MapAccess.get(request, :predicate) do
      nil -> false
      predicate -> Predicate.post_filter_predicates(predicate, plan_pushdowns(plan)) == []
    end
  end

  @doc "Returns pushdown constraints from the plan scans."
  @spec plan_pushdowns(Plan.t()) :: [term()]
  def plan_pushdowns(%Plan{scans: scans}),
    do: Enum.flat_map(scans, &Map.get(&1, "constraints", []))

  @doc "Resolves the effective page limit for a request."
  @spec page_limit(map(), map()) :: pos_integer()
  def page_limit(request, identity) when is_map(request) and is_map(identity) do
    case MapAccess.get(request, :limit) do
      nil -> default_limit(identity)
      limit -> limit
    end
  end

  defp maybe_filter_full_text(documents, request, %Plan{kind: :full_text}, deadline) do
    case MapAccess.get(request, :search) do
      nil ->
        {:error, VialKeeper.Error.invalid_index_hint("full-text plan cannot be executed", %{})}

      _search ->
        with :ok <- check_deadline(deadline) do
          {:ok, documents}
        end
    end
  end

  defp maybe_filter_full_text(documents, _request, _plan, _deadline), do: {:ok, documents}

  defp maybe_filter_selector(documents, request, plan, deadline) do
    predicate = MapAccess.get(request, :predicate)

    if is_nil(predicate) do
      {:error, VialKeeper.Error.invalid_request("normalized predicate is required")}
    else
      if no_post_filter?(plan, request),
        do: {:ok, documents},
        else: filter_documents(documents, predicate, deadline)
    end
  end

  defp filter_documents(documents, predicate, deadline) do
    with {:ok, compiled} <- Selector.compile(predicate) do
      documents
      |> Enum.with_index()
      |> Enum.reduce_while({:ok, []}, &filter_document(&1, &2, compiled, deadline))
      |> case do
        {:ok, values} -> {:ok, Enum.reverse(values)}
        error -> error
      end
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

  defp cursor_values(documents, request) do
    case MapAccess.get(request, :after_ordering) do
      after_ordering when is_map(after_ordering) ->
        sort = Ordering.compile_sort(MapAccess.get(request, :sort, []))

        Enum.drop_while(documents, fn document ->
          Ordering.compare_cursor(document, after_ordering, sort) != :gt
        end)

      _ ->
        case MapAccess.get(request, :after_id) do
          nil -> documents
          after_id -> Enum.drop_while(documents, &(&1.id <= after_id))
        end
    end
  end

  defp rejected_index_reasons(indexes, plan, request) do
    selected = MapSet.new(Enum.map(Plan.index_bindings(plan), & &1.index_id))

    Enum.map(indexes, fn index ->
      id = index_id(index)
      {id, if(MapSet.member?(selected, id), do: :selected, else: :incompatible)}
    end)
    |> Map.new()
    |> Map.put(:request, MapAccess.get(request, :index))
  end

  defp selected_metadata(%Plan{selected_indexes: [binding | _]}), do: binding
  defp selected_metadata(%Plan{}), do: %{index_id: nil, definition_digest: nil}

  defp first_binding_id(%Plan{selected_indexes: [binding | _]}), do: binding.index_id
  defp first_binding_id(%Plan{}), do: nil

  defp index_id(index) when is_map(index), do: MapAccess.get(index, :index_id)
end
