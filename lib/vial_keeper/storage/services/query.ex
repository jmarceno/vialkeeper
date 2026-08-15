defmodule VialKeeper.Storage.Services.Query do
  @moduledoc """
  Shared query orchestration over storage ports.

  Loads index definitions, plans with `VialKeeper.Query.Planner`, retrieves
  opaque candidates through the index-candidates port, then applies shared
  `VialKeeper.Query.Executor` filtering, ordering, cursors, projection, deadlines,
  and explain shaping.
  """

  alias VialKeeper.JSON.StrictCache
  alias VialKeeper.MapAccess
  alias VialKeeper.Observability.Instrumentation.Query, as: QueryInstrumentation
  alias VialKeeper.Query.{Executor, Plan, Planner, Projection}
  alias VialKeeper.Storage.BackendContext
  alias VialKeeper.Storage.Ports.Access

  @query_plan_cache_limit 16

  @doc "Executes a normalized query request against an opaque backend context."
  @spec execute(BackendContext.t(), map(), map() | nil) ::
          {:ok, map()} | {:error, VialKeeper.Error.t()}
  def execute(%BackendContext{} = context, request, supplied_identity \\ nil)
      when is_map(request) do
    started_native = System.monotonic_time()
    identity = supplied_identity || context_identity(context)
    uuid = Map.get(identity, :database_uuid)

    QueryInstrumentation.execute(uuid, 0, started_native, fn ->
      result = execute_uninstrumented(context, request, identity, started_native)
      {result, examined_count(result)}
    end)
  end

  defp execute_uninstrumented(context, request, identity, started_native) do
    deadline = Executor.deadline(identity, started_native)

    with {:ok, indexes} <- list_indexes(context),
         :ok <- Executor.check_deadline(deadline),
         {:ok, plan} <- plan_request(identity, indexes, request),
         :ok <- Executor.check_deadline(deadline),
         :ok <- Executor.validate_bookmark_plan(request, plan),
         limit <- Executor.page_limit(request, identity),
         {:ok, documents, examined} <-
           gather_candidates(context, plan, indexes, request, identity, deadline, limit),
         request <- attach_full_text_index(request, plan, indexes),
         {:ok, matched} <- Executor.filter_query(documents, request, plan, deadline),
         :ok <- Executor.check_deadline(deadline),
         :ok <- Executor.enforce_scan_limit(plan, examined, identity),
         {:ok, ordered} <- Executor.sort_documents(matched, request, deadline),
         {:ok, ordered} <- Executor.apply_after_cursor(ordered, request, deadline),
         page_values <- Enum.take(ordered, limit),
         :ok <- Executor.check_deadline(deadline),
         {:ok, projected} <- Executor.project_documents(page_values, request, deadline) do
      {:ok, Executor.assemble_result(projected, ordered, plan, request, identity, examined)}
    end
  end

  @doc "Executes a bounded subscription membership snapshot."
  @spec subscription_snapshot(BackendContext.t(), map(), map() | nil) ::
          {:ok, map()} | {:error, VialKeeper.Error.t()}
  def subscription_snapshot(%BackendContext{} = context, request, supplied_identity \\ nil)
      when is_map(request) do
    started_native = System.monotonic_time()
    identity = supplied_identity || context_identity(context)
    uuid = Map.get(identity, :database_uuid)

    QueryInstrumentation.execute(uuid, 0, started_native, fn ->
      result = subscription_snapshot_uninstrumented(context, request, identity, started_native)
      {result, examined_count(result)}
    end)
  end

  defp subscription_snapshot_uninstrumented(context, request, identity, started_native) do
    deadline = Executor.deadline(identity, started_native)

    max_members =
      MapAccess.get(request, :max_members) ||
        get_in(identity, [:config, "subscriptions", "max_members"]) ||
        500

    with {:ok, indexes} <- list_indexes(context),
         :ok <- Executor.check_deadline(deadline),
         {:ok, plan} <- plan_request(identity, indexes, request),
         :ok <- Executor.check_deadline(deadline),
         candidate_limit <-
           if(Executor.no_post_filter?(plan, request),
             do: max_members + 1,
             else: Executor.scan_threshold(identity)
           ),
         {:ok, documents, examined} <-
           gather_candidates(
             context,
             plan,
             indexes,
             request,
             identity,
             deadline,
             candidate_limit
           ),
         request <- attach_full_text_index(request, plan, indexes),
         {:ok, matched} <- Executor.filter_query(documents, request, plan, deadline),
         :ok <- Executor.check_deadline(deadline),
         :ok <- Executor.enforce_scan_limit(plan, examined, identity),
         {:ok, ordered} <- Executor.sort_documents(matched, request, deadline),
         :ok <- Executor.enforce_subscription_membership_bound(ordered, max_members),
         :ok <- Executor.check_deadline(deadline),
         {:ok, projected} <- Executor.project_documents(ordered, request, deadline) do
      {:ok, Executor.assemble_subscription_result(projected, identity)}
    end
  end

  @doc "Builds a public explain payload for a normalized query request."
  @spec explain(BackendContext.t(), map(), map() | nil) ::
          {:ok, map()} | {:error, VialKeeper.Error.t()}
  def explain(%BackendContext{} = context, request, supplied_identity \\ nil)
      when is_map(request) do
    identity = supplied_identity || context_identity(context)
    deadline = Executor.deadline(identity, System.monotonic_time())

    with {:ok, indexes} <- list_indexes(context),
         :ok <- Executor.check_deadline(deadline),
         {:ok, plan} <- Planner.plan(indexes, request),
         :ok <- Executor.check_deadline(deadline),
         {:ok, winning_count} <- winning_document_count(context),
         :ok <- Executor.check_deadline(deadline),
         {:ok, candidate_count} <-
           explain_candidate_count(
             context,
             plan,
             indexes,
             request,
             identity,
             winning_count,
             deadline
           ) do
      {:ok, Executor.explain_payload(plan, indexes, request, identity, candidate_count)}
    end
  end

  defp gather_candidates(
         context,
         %Plan{kind: :bounded_scan} = plan,
         _indexes,
         _request,
         identity,
         deadline,
         _limit
       ) do
    with {:ok, count} <- winning_document_count(context),
         :ok <- Executor.check_deadline(deadline),
         :ok <- Executor.enforce_scan_limit(plan, count, identity),
         :ok <- Executor.check_deadline(deadline),
         {:ok, documents} <-
           Access.port(context, :index_candidates).lookup_candidates(context, %{
             kind: :bounded_scan
           }),
         :ok <- Executor.check_deadline(deadline) do
      {:ok, normalize_candidates(documents), count}
    end
  end

  defp gather_candidates(
         context,
         %Plan{kind: :full_text} = plan,
         indexes,
         request,
         _identity,
         deadline,
         _limit
       ) do
    selected = selected_index(indexes, plan)
    search = MapAccess.get(request, :search)

    if selected && search do
      with {:ok, rows} <-
             Access.port(context, :index_candidates).full_text_candidates(context, %{
               index_id: MapAccess.get(selected, :index_id) || MapAccess.get(selected, "index_id"),
               text: MapAccess.get(search, :text),
               mode: MapAccess.get(search, :mode, "all"),
               deadline: deadline
             }),
           :ok <- Executor.check_deadline(deadline) do
        {:ok, normalize_candidates(rows), length(rows)}
      end
    else
      {:error, VialKeeper.Error.invalid_index_hint("full-text plan cannot be executed", %{})}
    end
  end

  defp gather_candidates(
         context,
         %Plan{kind: :single} = plan,
         indexes,
         request,
         identity,
         deadline,
         limit
       ) do
    selected = selected_index(indexes, plan)
    scan = List.first(plan.scans)
    threshold = Executor.scan_threshold(identity)

    with {:ok, rows} <-
           Access.port(context, :index_candidates).range_scan_candidates(context, %{
             index_id: MapAccess.get(selected, :index_id) || MapAccess.get(selected, "index_id"),
             index: selected,
             scan: scan,
             plan: plan,
             request: request,
             limit: limit,
             threshold: threshold,
             identity: identity,
             deadline: deadline
           }),
         :ok <- Executor.check_deadline(deadline) do
      examined = MapAccess.get(rows, :examined) || length(candidate_list(rows))
      {:ok, normalize_candidates(candidate_list(rows)), examined}
    end
  end

  defp gather_candidates(
         context,
         %Plan{kind: :union} = plan,
         indexes,
         _request,
         identity,
         deadline,
         _limit
       ) do
    threshold = Executor.scan_threshold(identity)

    Enum.reduce_while(plan.scans, {:ok, [], MapSet.new()}, fn scan, {:ok, candidates, seen} ->
      selected = selected_index(indexes, scan["index_id"])

      with :ok <- Executor.check_deadline(deadline),
           {:ok, rows} <-
             Access.port(context, :index_candidates).range_scan_candidates(context, %{
               index_id: scan["index_id"],
               index: selected,
               scan: scan,
               threshold: threshold,
               identity: identity,
               deadline: deadline
             }),
           :ok <- Executor.check_deadline(deadline) do
        docs = normalize_candidates(candidate_list(rows))
        append_union_candidate_batch(docs, candidates, seen, threshold)
      else
        {:error, _} = error -> {:halt, error}
      end
    end)
    |> case do
      {:ok, batches, _seen} ->
        documents =
          batches
          |> Enum.reverse()
          |> Enum.concat()

        with :ok <- Executor.check_deadline(deadline) do
          {:ok, documents, length(documents)}
        end

      {:error, _} = error ->
        error
    end
  end

  defp explain_candidate_count(
         _context,
         %Plan{kind: :bounded_scan},
         _indexes,
         _request,
         _identity,
         count,
         _deadline
       ),
       do: {:ok, count}

  defp explain_candidate_count(
         context,
         %Plan{kind: :single} = plan,
         indexes,
         request,
         _identity,
         _count,
         deadline
       ) do
    selected = selected_index(indexes, plan)
    scan = List.first(plan.scans)

    with {:ok, %{count: count}} <-
           Access.port(context, :index_candidates).range_scan_candidates(context, %{
             index_id: MapAccess.get(selected, :index_id) || MapAccess.get(selected, "index_id"),
             index: selected,
             scan: scan,
             plan: plan,
             request: request,
             count_only: true,
             deadline: deadline
           }),
         :ok <- Executor.check_deadline(deadline) do
      {:ok, count}
    end
  end

  defp explain_candidate_count(
         context,
         %Plan{kind: :union} = plan,
         indexes,
         _request,
         _identity,
         _count,
         deadline
       ) do
    with :ok <- Executor.check_deadline(deadline),
         {:ok, %{count: count}} <-
           Access.port(context, :index_candidates).range_scan_candidates(context, %{
             union_count: true,
             arms: union_count_arms(plan, indexes),
             plan_kind: :union,
             deadline: deadline
           }),
         :ok <- Executor.check_deadline(deadline) do
      {:ok, count}
    end
  end

  defp explain_candidate_count(context, plan, indexes, request, identity, _count, deadline) do
    case gather_candidates(context, plan, indexes, request, identity, deadline, nil) do
      {:ok, _documents, examined} -> {:ok, examined}
      {:error, _} = error -> error
    end
  end

  defp append_unique_candidates(candidates, seen, threshold) do
    append_unique_candidates(candidates, seen, threshold, [])
  end

  defp append_unique_candidates([], seen, _threshold, unique),
    do: {:ok, Enum.reverse(unique), seen}

  defp append_unique_candidates([candidate | rest], seen, threshold, unique) do
    candidate_id = candidate.id
    next_seen = MapSet.put(seen, candidate_id)

    cond do
      MapSet.member?(seen, candidate_id) ->
        append_unique_candidates(rest, seen, threshold, unique)

      MapSet.size(next_seen) > threshold ->
        {:error, MapSet.size(next_seen)}

      true ->
        append_unique_candidates(rest, next_seen, threshold, [candidate | unique])
    end
  end

  defp append_union_candidate_batch(docs, candidates, seen, threshold) do
    case append_unique_candidates(docs, seen, threshold) do
      {:ok, unique_docs, seen} ->
        {:cont, {:ok, [unique_docs | candidates], seen}}

      {:error, candidate_count} ->
        {:halt,
         {:error,
          VialKeeper.Error.index_required("query requires a compatible index", %{
            candidate_count: candidate_count,
            threshold: threshold
          })}}
    end
  end

  defp list_indexes(context),
    do: Access.port(context, :index_candidates).list_indexes(context)

  defp winning_document_count(context),
    do: Access.port(context, :index_candidates).winning_document_count(context)

  defp plan_request(identity, indexes, request) do
    StrictCache.memoize(
      :query_plan,
      {Map.get(identity, :database_uuid), indexes, request},
      @query_plan_cache_limit,
      fn ->
        case Planner.plan(indexes, request) do
          {:ok, _} = result -> result
          {:error, error} -> Executor.plan_error(request, error)
        end
      end
    )
  end

  defp attach_full_text_index(request, %Plan{kind: :full_text} = plan, indexes) do
    case selected_index(indexes, plan) do
      nil -> request
      index -> Map.put(request, :full_text_index, index)
    end
  end

  defp attach_full_text_index(request, _plan, _indexes), do: request

  defp union_count_arms(%Plan{scans: scans}, indexes) do
    Enum.map(scans, fn scan ->
      %{index: selected_index(indexes, scan["index_id"]), scan: scan}
    end)
  end

  defp selected_index(indexes, %Plan{selected_indexes: [binding | _]}),
    do: selected_index(indexes, binding.index_id)

  defp selected_index(indexes, index_id) when is_binary(index_id) do
    Enum.find(indexes, fn index ->
      MapAccess.get(index, :index_id) == index_id or MapAccess.get(index, "index_id") == index_id
    end)
  end

  defp selected_index(_indexes, _), do: nil

  defp candidate_list(%{candidates: candidates}) when is_list(candidates), do: candidates
  defp candidate_list(candidates) when is_list(candidates), do: candidates

  defp normalize_candidates(candidates) when is_list(candidates) do
    Enum.map(candidates, &normalize_candidate/1)
  end

  defp normalize_candidate(candidate) when is_map(candidate) do
    id = MapAccess.get(candidate, :id) || MapAccess.get(candidate, :document_id)

    Projection.document(
      id,
      MapAccess.get(candidate, :revision) || MapAccess.get(candidate, :winning_revision),
      MapAccess.get(candidate, :body)
    )
    |> maybe_put_rank(MapAccess.get(candidate, :rank))
  end

  defp maybe_put_rank(document, rank) when is_number(rank), do: Map.put(document, :rank, rank)
  defp maybe_put_rank(document, _), do: document

  defp context_identity(%BackendContext{identity: identity}) when is_map(identity), do: identity
  defp context_identity(_), do: %{current_sequence: 0, config: VialKeeper.Config.defaults()}

  defp examined_count({:ok, %{examined: examined}}) when is_integer(examined), do: examined
  defp examined_count(_), do: 0
end
