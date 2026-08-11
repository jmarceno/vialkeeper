defmodule ElixirDB.Storage.Services.Query do
  @moduledoc """
  Shared query orchestration over storage ports.

  Loads index definitions, plans with `ElixirDB.Query.Planner`, retrieves
  opaque candidates through the index-candidates port, then applies shared
  `ElixirDB.Query.Executor` filtering, ordering, cursors, projection, deadlines,
  and explain shaping.
  """

  alias ElixirDB.MapAccess
  alias ElixirDB.Query.{Executor, Plan, Planner}
  alias ElixirDB.Storage.BackendContext
  alias ElixirDB.Storage.Ports.Access

  @doc "Executes a normalized query request against an opaque backend context."
  @spec execute(BackendContext.t(), map(), map() | nil) ::
          {:ok, map()} | {:error, ElixirDB.Error.t()}
  def execute(%BackendContext{} = context, request, supplied_identity \\ nil)
      when is_map(request) do
    started_native = System.monotonic_time()
    identity = supplied_identity || context_identity(context)
    deadline = Executor.deadline(identity, started_native)

    with {:ok, indexes} <- list_indexes(context),
         :ok <- Executor.check_deadline(deadline),
         {:ok, plan} <- plan_request(indexes, request),
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
          {:ok, map()} | {:error, ElixirDB.Error.t()}
  def subscription_snapshot(%BackendContext{} = context, request, supplied_identity \\ nil)
      when is_map(request) do
    started_native = System.monotonic_time()
    identity = supplied_identity || context_identity(context)
    deadline = Executor.deadline(identity, started_native)

    max_members =
      MapAccess.get(request, :max_members) ||
        get_in(identity, [:config, "subscriptions", "max_members"]) ||
        500

    with {:ok, indexes} <- list_indexes(context),
         :ok <- Executor.check_deadline(deadline),
         {:ok, plan} <- plan_request(indexes, request),
         :ok <- Executor.check_deadline(deadline),
         candidate_limit <-
           if(Executor.no_post_filter?(plan, request), do: max_members + 1, else: nil),
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
          {:ok, map()} | {:error, ElixirDB.Error.t()}
  def explain(%BackendContext{} = context, request, supplied_identity \\ nil)
      when is_map(request) do
    identity = supplied_identity || context_identity(context)
    deadline = Executor.deadline(identity, System.monotonic_time())

    with {:ok, indexes} <- list_indexes(context),
         {:ok, plan} <- Planner.plan(indexes, request),
         {:ok, winning_count} <- winning_document_count(context),
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
      {:error, ElixirDB.Error.invalid_index_hint("full-text plan cannot be executed", %{})}
    end
  end

  defp gather_candidates(
         context,
         %Plan{kind: :single} = plan,
         indexes,
         request,
         _identity,
         deadline,
         limit
       ) do
    selected = selected_index(indexes, plan)
    scan = List.first(plan.scans)

    with {:ok, rows} <-
           Access.port(context, :index_candidates).range_scan_candidates(context, %{
             index_id: MapAccess.get(selected, :index_id) || MapAccess.get(selected, "index_id"),
             index: selected,
             scan: scan,
             plan: plan,
             request: request,
             limit: limit,
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
         _identity,
         deadline,
         _limit
       ) do
    Enum.reduce_while(plan.scans, {:ok, [], 0}, fn scan, {:ok, candidates, examined} ->
      selected = selected_index(indexes, scan["index_id"])

      with :ok <- Executor.check_deadline(deadline),
           {:ok, rows} <-
             Access.port(context, :index_candidates).range_scan_candidates(context, %{
               index_id: scan["index_id"],
               index: selected,
               scan: scan,
               deadline: deadline
             }),
           :ok <- Executor.check_deadline(deadline) do
        docs = normalize_candidates(candidate_list(rows))
        {:cont, {:ok, [docs | candidates], examined + length(docs)}}
      else
        {:error, _} = error -> {:halt, error}
      end
    end)
    |> case do
      {:ok, batches, _examined} ->
        documents =
          batches
          |> Enum.reverse()
          |> Enum.concat()
          |> Enum.uniq_by(& &1.id)

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

  defp explain_candidate_count(context, plan, indexes, request, identity, _count, deadline) do
    case gather_candidates(context, plan, indexes, request, identity, deadline, nil) do
      {:ok, _documents, examined} -> {:ok, examined}
      {:error, _} = error -> error
    end
  end

  defp list_indexes(context),
    do: Access.port(context, :index_candidates).list_indexes(context)

  defp winning_document_count(context),
    do: Access.port(context, :index_candidates).winning_document_count(context)

  defp plan_request(indexes, request) do
    case Planner.plan(indexes, request) do
      {:ok, _} = result -> result
      {:error, error} -> Executor.plan_error(request, error)
    end
  end

  defp attach_full_text_index(request, %Plan{kind: :full_text} = plan, indexes) do
    case selected_index(indexes, plan) do
      nil -> request
      index -> Map.put(request, :full_text_index, index)
    end
  end

  defp attach_full_text_index(request, _plan, _indexes), do: request

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

    %{
      id: id,
      revision: MapAccess.get(candidate, :revision) || MapAccess.get(candidate, :winning_revision),
      body: MapAccess.get(candidate, :body)
    }
    |> maybe_put_rank(MapAccess.get(candidate, :rank))
  end

  defp maybe_put_rank(document, rank) when is_number(rank), do: Map.put(document, :rank, rank)
  defp maybe_put_rank(document, _), do: document

  defp context_identity(%BackendContext{identity: identity}) when is_map(identity), do: identity
  defp context_identity(_), do: %{current_sequence: 0, config: ElixirDB.Config.defaults()}
end
