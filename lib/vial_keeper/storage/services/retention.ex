defmodule VialKeeper.Storage.Services.Retention do
  @moduledoc """
  Shared retention orchestration over storage ports.

  Loads retention facts, asks `VialKeeper.Retention.Service` for pure decisions,
  and applies compaction or boundary-transfer effects through retention and
  document ports. Callers supply an opaque `BackendContext`.
  """

  alias VialKeeper.Domain.{BoundaryPage, RetentionState}
  alias VialKeeper.MapAccess
  alias VialKeeper.Retention.Frontier
  alias VialKeeper.Retention.Service, as: RetentionService
  alias VialKeeper.Storage.BackendContext
  alias VialKeeper.Storage.Ports.Access
  alias VialKeeper.Storage.Services.Facts

  @page_size 100

  @doc """
  Runs compact-retention inside an already-open transaction.
  """
  @spec compact_tx(BackendContext.t(), map()) :: {:ok, map()} | {:error, VialKeeper.Error.t()}
  def compact_tx(%BackendContext{} = context, request) when is_map(request) do
    _ = request
    now = DateTime.utc_now()

    with :ok <- retention_fault_check(context),
         {:ok, meta} <- load_meta(context),
         {:ok, peers} <- Facts.list_peer_positions(context),
         {:ok, boundaries} <-
           Facts.list_boundaries(context, source_database_uuid: meta.database_uuid),
         mode <- runtime_retention_mode(meta.config),
         frontier <-
           Frontier.compute(
             RetentionService.compute_frontier_input(
               meta,
               peers,
               mode,
               now
             )
           ),
         {:ok, plan_docs} <- Facts.list_compaction_documents(context, frontier.candidate_floor) do
      decision = decide_compaction(meta, peers, boundaries, plan_docs, mode, now)

      apply_compaction_decision(context, meta, decision)
    end
  end

  @spec decide_compaction(
          RetentionService.meta(),
          list(),
          list(),
          list(),
          :disabled | :stable_frontier,
          DateTime.t()
        ) ::
          {:noop, map()}
          | {:apply, map(), term(), RetentionService.effect_meta()}
          | {:error, VialKeeper.Error.t()}
  defp decide_compaction(meta, peers, boundaries, plan_docs, mode, now) do
    :erlang.apply(
      RetentionService,
      :decide_compaction_with_mode,
      [meta, peers, boundaries, plan_docs, meta.config, mode, now]
    )
  end

  @spec apply_compaction_decision(BackendContext.t(), RetentionService.meta(), term()) ::
          {:ok, map()} | {:error, VialKeeper.Error.t()}
  defp apply_compaction_decision(context, meta, decision) do
    case decision do
      {:noop, stats} ->
        {:ok, stats}

      {:apply, apply_frontier, plan, effect_meta} ->
        apply_compaction(context, meta, apply_frontier, plan, effect_meta)

      {:error, _} = error ->
        error
    end
  end

  @doc "Builds the public retention state snapshot."
  @spec retention_state(BackendContext.t()) ::
          {:ok, RetentionState.t()} | {:error, VialKeeper.Error.t()}
  def retention_state(%BackendContext{} = context) do
    with {:ok, meta} <- load_meta(context),
         {:ok, maintenance_counter} <- Facts.maintenance_counter(context) do
      RetentionState.new(%{
        history_epoch: meta.history_epoch,
        floor_sequence: meta.retention_floor_sequence,
        compaction_epoch: meta.compaction_epoch,
        boundary_digest: Map.get(meta, :retention_boundary_digest),
        mode: RetentionService.retention_mode(meta.config),
        maintenance_counter: maintenance_counter
      })
    end
  end

  @doc "Lists durable peer ledger positions."
  @spec list_peer_positions(BackendContext.t()) ::
          {:ok, [term()]} | {:error, VialKeeper.Error.t()}
  def list_peer_positions(%BackendContext{} = context),
    do: Facts.list_peer_positions(context)

  @doc "Validates and CAS-writes a peer ledger position."
  @spec put_peer_position_cas(BackendContext.t(), map()) ::
          {:ok, map()} | {:error, VialKeeper.Error.t()}
  def put_peer_position_cas(%BackendContext{} = context, request) when is_map(request) do
    peer_uuid =
      MapAccess.get(request, :peer_database_uuid) ||
        request |> MapAccess.get(:value, %{}) |> peer_uuid_from_value()

    with {:ok, meta} <- load_meta(context),
         {:ok, peers} <- Facts.list_peer_positions(context),
         {:ok, peer} <- RetentionService.decode_peer(MapAccess.get(request, :value)),
         :ok <-
           RetentionService.validate_peer_put(
             meta,
             peers,
             peer,
             MapAccess.get(request, :bootstrap_completed, false)
           ) do
      Facts.put_peer_position_record(context, %{
        peer_database_uuid: peer_uuid,
        expected_version: MapAccess.get(request, :expected_version, 0),
        value: RetentionService.peer_wire(peer)
      })
    end
  end

  @doc "Reads one boundary page for the local source database."
  @spec read_boundary_pages(BackendContext.t(), map()) ::
          {:ok, BoundaryPage.t()} | {:error, VialKeeper.Error.t()}
  def read_boundary_pages(%BackendContext{} = context, request) when is_map(request) do
    cursor = MapAccess.get(request, :cursor) || MapAccess.get(request, :page_cursor)
    limit = MapAccess.get(request, :limit, @page_size)
    requested_epoch = MapAccess.get(request, :compaction_epoch)
    requested_history = MapAccess.get(request, :source_history_epoch)

    with {:ok, meta} <- load_meta(context),
         :ok <- RetentionService.validate_boundary_read(meta, requested_history, requested_epoch),
         {:ok, boundaries} <-
           Facts.list_boundaries(context, source_database_uuid: meta.database_uuid),
         digest <- BoundaryPage.digest_for(Enum.map(boundaries, & &1.boundary)),
         {:ok, page_boundaries, next_page} <-
           RetentionService.paginate_boundaries(boundaries, cursor, limit) do
      BoundaryPage.page(
        meta.history_epoch,
        meta.compaction_epoch,
        digest,
        next_page,
        Enum.map(page_boundaries, & &1.boundary),
        meta.database_uuid
      )
    end
  end

  @doc "Installs one boundary page (replace, stage, or complete)."
  @spec install_boundary_pages(BackendContext.t(), map() | BoundaryPage.t()) ::
          {:ok, map()} | {:error, VialKeeper.Error.t()}
  def install_boundary_pages(%BackendContext{} = context, %BoundaryPage{} = page) do
    install_boundary_page(context, page)
  end

  def install_boundary_pages(%BackendContext{} = context, request) when is_map(request) do
    case RetentionService.decode_boundary_page(request) do
      {:ok, page} -> install_boundary_page(context, page)
      {:error, _} = error -> error
    end
  end

  defp install_boundary_page(context, page) do
    with :ok <- RetentionService.validate_boundary_page_fields(page),
         {:ok, installed} <- Facts.boundary_install_state(context, page.source_database_uuid),
         :ok <- RetentionService.validate_boundary_install(installed, page) do
      install_boundary_page_contents(context, page)
    end
  end

  defp install_boundary_page_contents(context, %{install_id: install_id} = page)
       when is_binary(install_id) do
    with :ok <- maybe_begin_boundary_install(context, page),
         :ok <- Facts.stage_boundary_page(context, install_id, page) do
      if is_nil(page.next_page) do
        Facts.complete_boundary_install(context, install_id)
      else
        {:ok,
         RetentionService.install_result(
           length(page.boundaries),
           page.boundary_digest,
           page.compaction_epoch
         )}
      end
    end
  end

  defp install_boundary_page_contents(context, %{install_id: nil, next_page: nil} = page) do
    Facts.replace_boundary_set(
      context,
      RetentionService.boundary_install_state_for_page(page),
      page.boundaries
    )
  end

  defp install_boundary_page_contents(_context, %{install_id: nil}),
    do:
      {:error,
       VialKeeper.Error.invalid_request(
         "boundary install_id is required for a paginated boundary transfer"
       )}

  defp maybe_begin_boundary_install(context, %{replace: true, install_id: install_id} = page) do
    Facts.begin_boundary_install(
      context,
      install_id,
      RetentionService.boundary_install_state_for_page(page)
    )
  end

  defp maybe_begin_boundary_install(_context, %{replace: false, install_id: _install_id}), do: :ok

  defp apply_compaction(context, meta, frontier, plan, effect_meta) do
    with :ok <- Facts.apply_compaction_effect(context, effect_meta) do
      {:ok,
       RetentionService.apply_stats(
         meta,
         frontier,
         plan,
         effect_meta.increment_maintenance?,
         effect_meta.new_compaction_epoch
       )}
    end
  end

  @spec load_meta(BackendContext.t()) ::
          {:ok, RetentionService.meta()} | {:error, VialKeeper.Error.t()}
  defp load_meta(%BackendContext{} = context) do
    case Access.port(context, :lifecycle).identity(context) do
      {:ok, identity} when is_map(identity) ->
        config = Map.get(identity, :config) || VialKeeper.Config.defaults()

        {:ok,
         %{
           database_uuid: Map.fetch!(identity, :database_uuid),
           history_epoch: Map.fetch!(identity, :history_epoch),
           current_sequence: Map.get(identity, :current_sequence, 0),
           retention_floor_sequence: Map.get(identity, :retention_floor_sequence, 0),
           compaction_epoch: Map.get(identity, :compaction_epoch, 0),
           retention_boundary_digest: Map.get(identity, :retention_boundary_digest),
           config: config
         }}

      {:error, _} = error ->
        error
    end
  end

  defp retention_fault_check(%BackendContext{identity: identity}) when is_map(identity) do
    case Map.get(identity, :retention_fault) do
      fun when is_function(fun, 1) ->
        case fun.(:compact_retention_mid_tx) do
          :ok -> :ok
          {:error, %VialKeeper.Error{} = error} -> {:error, error}
        end

      _ ->
        :ok
    end
  end

  defp retention_fault_check(_), do: :ok

  @spec runtime_retention_mode(map()) :: :disabled | :stable_frontier
  defp runtime_retention_mode(config) when is_map(config) do
    case get_in(config, ["retention", "mode"]) do
      "stable_frontier" -> :stable_frontier
      _ -> :disabled
    end
  end

  defp peer_uuid_from_value(%{"peer_database_uuid" => uuid}), do: uuid
  defp peer_uuid_from_value(%{peer_database_uuid: uuid}), do: uuid
  defp peer_uuid_from_value(_), do: nil
end
