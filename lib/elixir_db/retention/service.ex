defmodule ElixirDB.Retention.Service do
  @moduledoc """
  Shared retention policy decisions for compaction, peer positions, and
  boundary-page transfer. Callers supply loaded domain facts; persistence
  remains behind storage ports.
  """

  alias ElixirDB.Domain.{BoundaryPage, PeerPosition, RetentionState}
  alias ElixirDB.Retention.{CompactionPlan, Frontier}

  @page_size 100

  @type meta :: %{
          required(:database_uuid) => binary(),
          required(:history_epoch) => binary(),
          required(:current_sequence) => non_neg_integer(),
          required(:retention_floor_sequence) => non_neg_integer(),
          required(:compaction_epoch) => non_neg_integer(),
          optional(:retention_boundary_digest) => binary() | nil,
          optional(:config) => map()
        }

  @type effect_meta :: %{
          new_floor: non_neg_integer(),
          new_compaction_epoch: non_neg_integer(),
          work?: boolean(),
          peers_to_expire: [PeerPosition.t()],
          boundaries_to_upsert: [term()],
          boundaries_to_remove: [term()],
          source_database_uuid: binary(),
          source_history_epoch: binary(),
          removals: %{optional(binary()) => [binary()]},
          documents_to_empty: [binary()],
          delete_changes_through: non_neg_integer(),
          boundary_digest: binary() | nil,
          increment_maintenance?: boolean(),
          result_stats: map()
        }

  @doc "Builds Frontier input, marking lease-expired active peers as `:expired`."
  @spec compute_frontier_input(meta(), [PeerPosition.t()], atom(), DateTime.t()) :: map()
  def compute_frontier_input(meta, peers, mode, now)
      when is_map(meta) and is_list(peers) and mode in [:disabled, :stable_frontier] do
    now_ms = DateTime.to_unix(now, :millisecond)

    %{
      source_database_uuid: meta.database_uuid,
      source_history_epoch: meta.history_epoch,
      current_sequence: meta.current_sequence,
      current_floor: meta.retention_floor_sequence,
      mode: mode,
      peers: update_expired_peer_statuses(peers, now_ms),
      now: now
    }
  end

  @doc """
  Decides whether compaction is a no-op or should apply a plan.

  Returns `{:noop, stats}`, `{:apply, frontier, plan, effect_meta}`, or
  `{:error, error}` when compaction is disabled but work remains.
  """
  @spec decide_compaction(meta(), [PeerPosition.t()], [map()], [map()], map(), DateTime.t()) ::
          {:noop, map()}
          | {:apply, map(), CompactionPlan.t(), effect_meta()}
          | {:error, ElixirDB.Error.t()}
  def decide_compaction(meta, peers, boundaries, plan_input_docs, config, now)
      when is_map(meta) and is_list(peers) and is_list(boundaries) and is_list(plan_input_docs) and
             is_map(config) do
    now_ms = DateTime.to_unix(now, :millisecond)
    mode = retention_mode(config)
    frontier = Frontier.compute(compute_frontier_input(meta, peers, mode, now))

    plan =
      CompactionPlan.plan(%{
        documents: plan_input_docs,
        local_database_uuid: meta.database_uuid,
        candidate_floor: frontier.candidate_floor,
        history_depth: history_depth(config),
        boundaries: boundaries,
        peers: peers,
        peer_installed_epochs: peer_installed_epochs(peers),
        compaction_epoch: meta.compaction_epoch,
        now_ms: now_ms
      })

    case maybe_noop(frontier, plan, mode, meta) do
      {:noop, stats} ->
        {:noop, stats}

      :ok ->
        effect = build_effect_meta(meta, frontier, plan, peers, now_ms)
        {:apply, frontier, plan, effect}

      {:error, _} = error ->
        error
    end
  end

  @doc "True when a compaction plan has no durable work."
  @spec work_empty?(CompactionPlan.t()) :: boolean()
  def work_empty?(%CompactionPlan{} = plan) do
    plan.removals == %{} and plan.boundaries_to_upsert == [] and
      plan.boundaries_to_remove == [] and plan.delete_changes_through == 0 and
      plan.documents_to_empty == []
  end

  @doc "Stats map returned when compaction is a durable no-op."
  @spec noop_stats(map(), meta()) :: map()
  def noop_stats(frontier, meta) when is_map(frontier) and is_map(meta) do
    %{
      noop?: true,
      old_floor: meta.retention_floor_sequence,
      new_floor: meta.retention_floor_sequence,
      old_compaction_epoch: meta.compaction_epoch,
      new_compaction_epoch: meta.compaction_epoch,
      removed_changes: 0,
      removed_revisions: 0,
      removed_boundaries: 0,
      active_peer_count: frontier.active_peer_count,
      expired_peer_count: frontier.expired_peer_count,
      blocking_peer_count: frontier.blocking_peer_count,
      bootstrap_required_count: frontier.bootstrap_required_count
    }
  end

  @doc "Validates an incoming peer put against local meta and stored peers."
  @spec validate_peer_put(meta(), [PeerPosition.t()], PeerPosition.t(), boolean()) ::
          :ok | {:error, ElixirDB.Error.t()}
  def validate_peer_put(meta, peers, incoming_peer, bootstrap_completed)
      when is_map(meta) and is_list(peers) and is_boolean(bootstrap_completed) do
    with :ok <- validate_peer_source_database(meta, incoming_peer) do
      check_peer_regression(peers, incoming_peer, bootstrap_completed, meta)
    end
  end

  @doc "Validates a boundary-page read against the local history/compaction epoch."
  @spec validate_boundary_read(meta(), binary() | nil, non_neg_integer() | nil) ::
          :ok | {:error, ElixirDB.Error.t()}
  def validate_boundary_read(_meta, nil, nil), do: :ok

  def validate_boundary_read(meta, requested_history, requested_epoch) when is_map(meta) do
    with :ok <- validate_boundary_history(meta, requested_history) do
      validate_boundary_epoch_request(meta, requested_epoch)
    end
  end

  @doc "Validates required boundary-page fields for install/transfer."
  @spec validate_boundary_page_fields(BoundaryPage.t() | map()) ::
          :ok | {:error, ElixirDB.Error.t()}
  def validate_boundary_page_fields(page) when is_map(page) do
    cond do
      not is_binary(page.source_history_epoch) or page.source_history_epoch == "" ->
        {:error, ElixirDB.Error.invalid_request("boundary page source_history_epoch is required")}

      not is_integer(page.compaction_epoch) or page.compaction_epoch < 0 ->
        {:error,
         ElixirDB.Error.invalid_request("boundary page compaction_epoch must be non-negative")}

      not is_binary(page.boundary_digest) or page.boundary_digest == "" ->
        {:error, ElixirDB.Error.invalid_request("boundary page boundary_digest is required")}

      not is_list(page.boundaries) ->
        {:error, ElixirDB.Error.invalid_request("boundary page boundaries must be an array")}

      true ->
        :ok
    end
  end

  @doc """
  Validates a boundary install against previously installed source state.

  Pure: callers supply `installed_state` loaded from retention records.
  """
  @spec validate_boundary_install(map() | nil, BoundaryPage.t() | map()) ::
          :ok | {:error, ElixirDB.Error.t()}
  def validate_boundary_install(installed_state, page) when is_map(page) do
    with :ok <- validate_source_uuid(page.source_database_uuid) do
      validate_boundary_epoch(installed_state, page)
    end
  end

  @doc """
  Returns `:ok` when a staged boundary page matches install session metadata.
  """
  @spec same_boundary_install?(map(), BoundaryPage.t() | map()) ::
          :ok | {:error, ElixirDB.Error.t()}
  def same_boundary_install?(metadata, page) when is_map(metadata) and is_map(page) do
    if page.source_database_uuid == metadata.source_database_uuid and
         page.source_history_epoch == metadata.source_history_epoch and
         page.compaction_epoch == metadata.compaction_epoch and
         page.boundary_digest == metadata.boundary_digest do
      :ok
    else
      {:error, ElixirDB.Error.boundary_conflict("boundary pages belong to different installs")}
    end
  end

  @doc "Paginates stored boundaries with source-qualified NUL record keys."
  @spec paginate_boundaries([map()], binary() | nil, pos_integer() | term()) ::
          {:ok, [map()], binary() | nil} | {:error, ElixirDB.Error.t()}
  def paginate_boundaries(boundaries, cursor, limit) when is_list(boundaries) do
    with :ok <- validate_pagination(cursor, limit) do
      max = ElixirDB.Config.host_limits()[:max_bulk_operations] || 500
      page_limit = if is_integer(limit) and limit > 0, do: min(limit, max), else: @page_size
      sorted = sort_boundaries(boundaries)
      start_index = boundary_start_index(sorted, cursor)
      page = Enum.slice(sorted, start_index, page_limit)
      next_page = next_boundary_cursor(sorted, start_index, page)
      {:ok, page, next_page}
    end
  end

  @doc "Retention mode atom from database config."
  @spec retention_mode(map()) :: :disabled | :stable_frontier
  def retention_mode(config) when is_map(config) do
    case get_in(config, ["retention", "mode"]) do
      "stable_frontier" -> :stable_frontier
      _ -> :disabled
    end
  end

  @doc "Wire map for a peer position ledger record."
  @spec peer_wire(PeerPosition.t()) :: map()
  def peer_wire(%PeerPosition{} = peer) do
    %{
      "peer_database_uuid" => peer.peer_database_uuid,
      "peer_history_epoch" => peer.peer_history_epoch,
      "source_database_uuid" => peer.source_database_uuid,
      "source_history_epoch" => peer.source_history_epoch,
      "safe_source_sequence" => peer.safe_source_sequence,
      "installed_source_compaction_epoch" => peer.installed_source_compaction_epoch,
      "last_seen_at" => peer.last_seen_at,
      "lease_expires_at" => peer.lease_expires_at,
      "status" => Atom.to_string(peer.status)
    }
  end

  @doc "Decodes a peer position from atom-key or wire maps."
  @spec decode_peer(term()) :: {:ok, PeerPosition.t()} | {:error, ElixirDB.Error.t()}
  def decode_peer(value) when is_map(value) do
    if Enum.any?(Map.keys(value), &is_atom/1),
      do: PeerPosition.new(value),
      else: PeerPosition.from_wire(value)
  end

  def decode_peer(_),
    do: {:error, ElixirDB.Error.invalid_request("peer position must be an object")}

  @doc "Decodes a boundary page from atom-key or wire maps."
  @spec decode_boundary_page(term()) :: {:ok, BoundaryPage.t()} | {:error, ElixirDB.Error.t()}
  def decode_boundary_page(value) when is_map(value) do
    if Enum.any?(Map.keys(value), &is_atom/1),
      do: BoundaryPage.new(value),
      else: BoundaryPage.from_wire(value)
  end

  def decode_boundary_page(_),
    do: {:error, ElixirDB.Error.invalid_request("boundary page must be an object")}

  @doc "Install-state fields carried with a boundary page."
  @spec boundary_install_state_for_page(BoundaryPage.t() | map()) :: map()
  def boundary_install_state_for_page(page) when is_map(page) do
    Map.take(page, [
      :source_database_uuid,
      :source_history_epoch,
      :compaction_epoch,
      :boundary_digest
    ])
  end

  @doc "Public compaction result stats after applying a plan."
  @spec apply_stats(meta(), map(), CompactionPlan.t(), boolean(), non_neg_integer()) :: map()
  def apply_stats(meta, frontier, plan, work?, new_compaction_epoch)
      when is_map(meta) and is_map(frontier) do
    %{
      noop?: not work?,
      old_floor: meta.retention_floor_sequence,
      new_floor: frontier.candidate_floor,
      old_compaction_epoch: meta.compaction_epoch,
      new_compaction_epoch: new_compaction_epoch,
      removed_changes: plan.stats.changes_removed,
      removed_revisions: plan.stats.revisions_removed,
      removed_boundaries: plan.stats.boundaries_removed,
      active_peer_count: frontier.active_peer_count,
      expired_peer_count: frontier.expired_peer_count,
      blocking_peer_count: frontier.blocking_peer_count,
      bootstrap_required_count: frontier.bootstrap_required_count
    }
  end

  defp build_effect_meta(meta, frontier, plan, peers, now_ms) do
    new_floor = frontier.candidate_floor
    floor_advanced = new_floor > meta.retention_floor_sequence

    new_compaction_epoch =
      RetentionState.compaction_epoch_after(floor_advanced, meta.compaction_epoch)

    work? = not work_empty?(plan)

    %{
      peers_to_expire: peers_to_expire(peers, now_ms),
      boundaries_to_upsert: plan.boundaries_to_upsert,
      boundaries_to_remove: plan.boundaries_to_remove,
      source_database_uuid: meta.database_uuid,
      source_history_epoch: meta.history_epoch,
      new_compaction_epoch: new_compaction_epoch,
      removals: plan.removals,
      documents_to_empty: plan.documents_to_empty,
      delete_changes_through: plan.delete_changes_through,
      new_floor: new_floor,
      boundary_digest: nil,
      increment_maintenance?: work?,
      result_stats: compaction_result_stats(meta, frontier, plan, work?)
    }
  end

  defp compaction_result_stats(meta, frontier, plan, work?) do
    %{
      "noop" => not work?,
      "old_floor" => meta.retention_floor_sequence,
      "new_floor" => frontier.candidate_floor,
      "compaction_epoch" =>
        RetentionState.compaction_epoch_after(frontier.floor_advanced, meta.compaction_epoch),
      "removed_revisions" => plan.stats.revisions_removed,
      "removed_boundaries" => plan.stats.boundaries_removed,
      "removed_changes" => plan.stats.changes_removed
    }
  end

  defp peers_to_expire(peers, now_ms) do
    peers
    |> Enum.filter(&(PeerPosition.expired?(&1, now_ms) and &1.status != :expired))
    |> Enum.map(&%{&1 | status: :expired})
  end

  defp update_expired_peer_statuses(peers, now_ms) do
    Enum.map(peers, fn peer ->
      if PeerPosition.expired?(peer, now_ms) and peer.status == :active,
        do: %{peer | status: :expired},
        else: peer
    end)
  end

  defp maybe_noop(frontier, plan, :disabled, meta) do
    if frontier.noop? and work_empty?(plan),
      do: {:noop, noop_stats(frontier, meta)},
      else: {:error, ElixirDB.Error.invalid_request("retention compaction is disabled")}
  end

  defp maybe_noop(frontier, plan, :stable_frontier, meta) do
    if frontier.noop? and work_empty?(plan),
      do: {:noop, noop_stats(frontier, meta)},
      else: :ok
  end

  defp validate_boundary_history(_meta, nil), do: :ok

  defp validate_boundary_history(meta, history) when is_binary(history) do
    if history == meta.history_epoch,
      do: :ok,
      else:
        {:error,
         ElixirDB.Error.boundary_conflict("boundary request history epoch does not match", %{
           expected: meta.history_epoch,
           received: history
         })}
  end

  defp validate_boundary_history(_meta, _),
    do: {:error, ElixirDB.Error.invalid_request("boundary request history epoch is invalid")}

  defp validate_boundary_epoch_request(_meta, nil), do: :ok

  defp validate_boundary_epoch_request(meta, epoch) when is_integer(epoch) and epoch >= 0 do
    if epoch <= meta.compaction_epoch,
      do: :ok,
      else:
        {:error,
         ElixirDB.Error.boundary_conflict("boundary request compaction epoch is not available", %{
           current: meta.compaction_epoch,
           requested: epoch
         })}
  end

  defp validate_boundary_epoch_request(_meta, _),
    do: {:error, ElixirDB.Error.invalid_request("boundary request compaction epoch is invalid")}

  defp validate_boundary_epoch(nil, _page), do: :ok

  defp validate_boundary_epoch(
         %{compaction_epoch: installed_epoch, source_history_epoch: installed_history},
         page
       ) do
    replacing? = page.replace or is_nil(page.install_id)

    cond do
      page.compaction_epoch < installed_epoch ->
        {:error,
         ElixirDB.Error.boundary_conflict("boundary page compaction epoch regressed for source", %{
           received: page.compaction_epoch,
           installed: installed_epoch
         })}

      page.source_history_epoch != installed_history and not replacing? ->
        {:error,
         ElixirDB.Error.boundary_conflict("boundary page history epoch changed mid-install")}

      true ->
        :ok
    end
  end

  defp validate_source_uuid(uuid) when is_binary(uuid) and uuid != "", do: :ok

  defp validate_source_uuid(_),
    do: {:error, ElixirDB.Error.invalid_request("boundary page source_database_uuid is required")}

  defp validate_pagination(cursor, limit) do
    max = ElixirDB.Config.host_limits()[:max_bulk_operations] || 500
    clamped = if is_integer(limit) and limit > 0, do: min(limit, max), else: nil

    cond do
      not is_nil(cursor) and not is_binary(cursor) ->
        {:error, ElixirDB.Error.invalid_request("boundary page cursor must be a binary or null")}

      is_nil(clamped) ->
        {:error, ElixirDB.Error.invalid_request("boundary page limit must be a positive integer")}

      true ->
        :ok
    end
  end

  defp sort_boundaries(boundaries) do
    Enum.sort_by(boundaries, fn %{boundary: boundary} ->
      {boundary.document_id, boundary.history_id}
    end)
  end

  defp boundary_start_index(_sorted, nil), do: 0

  defp boundary_start_index(sorted, key) when is_binary(key) do
    case Enum.find_index(sorted, fn %{boundary: boundary, source_database_uuid: source_uuid} ->
           BoundaryPage.record_key(source_uuid, boundary.document_id, boundary.history_id) == key
         end) do
      nil -> 0
      index -> index + 1
    end
  end

  defp next_boundary_cursor(sorted, start_index, page) do
    case Enum.at(sorted, start_index + length(page)) do
      %{boundary: boundary, source_database_uuid: source_uuid} ->
        BoundaryPage.record_key(source_uuid, boundary.document_id, boundary.history_id)

      _ ->
        nil
    end
  end

  defp validate_peer_source_database(meta, peer) do
    if peer.source_database_uuid == meta.database_uuid,
      do: :ok,
      else:
        {:error,
         ElixirDB.Error.invalid_request("peer source_database_uuid does not match local database")}
  end

  defp check_peer_regression(peers, incoming, bootstrap_completed, meta) do
    case Enum.find(peers, &(&1.peer_database_uuid == incoming.peer_database_uuid)) do
      nil -> :ok
      previous -> check_previous_peer(previous, incoming, bootstrap_completed, meta)
    end
  end

  defp check_previous_peer(previous, incoming, bootstrap_completed, meta) do
    cond do
      PeerPosition.history_changed?(previous, incoming) ->
        check_peer_history_change(incoming, bootstrap_completed, meta)

      PeerPosition.regresses?(previous, incoming) ->
        {:error,
         ElixirDB.Error.rebase_required("peer position regressed", %{
           peer_database_uuid: incoming.peer_database_uuid
         })}

      true ->
        :ok
    end
  end

  defp check_peer_history_change(incoming, false, _meta) do
    if incoming.status == :bootstrap_required do
      :ok
    else
      {:error,
       ElixirDB.Error.rebase_required("peer history changed; bootstrap is required", %{
         peer_database_uuid: incoming.peer_database_uuid
       })}
    end
  end

  defp check_peer_history_change(incoming, true, meta),
    do: validate_bootstrap_replacement(incoming, meta)

  defp validate_bootstrap_replacement(peer, meta) do
    if peer.status == :active and
         peer.source_history_epoch == meta.history_epoch and
         peer.safe_source_sequence >= meta.retention_floor_sequence and
         peer.installed_source_compaction_epoch >= meta.compaction_epoch do
      :ok
    else
      {:error,
       ElixirDB.Error.rebase_required("peer bootstrap replacement is incomplete", %{
         peer_database_uuid: peer.peer_database_uuid
       })}
    end
  end

  defp peer_installed_epochs(peers) do
    Map.new(peers, fn peer -> {peer.peer_database_uuid, peer.installed_source_compaction_epoch} end)
  end

  defp history_depth(config), do: get_in(config, ["retention", "history_depth"]) || 0
end
