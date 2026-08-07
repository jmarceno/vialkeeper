defmodule ElixirDB.Storage.SQLite.Retention do
  @moduledoc """
  SQLite compact-retention transaction for stable-frontier mode.

  Invoked only from the adapter inside an IMMEDIATE transaction.
  """

  alias ElixirDB.Domain.{BoundaryPage, PeerPosition, RetentionState}
  alias ElixirDB.JSON.Canonical
  alias ElixirDB.Retention.{CompactionPlan, Frontier}
  alias ElixirDB.Storage.SQLite.{Connection, LocalRecords, Meta, RetentionRecords, Revisions}

  @page_size 100

  @spec compact(map(), map()) :: {:ok, map()} | {:error, ElixirDB.Error.t()}
  def compact(adapter, _request \\ %{}) do
    conn = adapter.conn
    now = DateTime.utc_now()
    now_ms = DateTime.to_unix(now, :millisecond)

    with {:ok, meta} <- Meta.load(conn),
         {:ok, peers} <- RetentionRecords.list_peers(conn),
         {:ok, boundaries} <- RetentionRecords.list_boundaries(conn),
         {:ok, maintenance_counter} <- RetentionRecords.maintenance_counter(conn),
         mode <- retention_mode(meta.config),
         {:ok, frontier} <-
           compute_frontier(meta, peers, mode, now, now_ms),
         {:ok, plan_input} <- load_plan_input(conn, meta, frontier.candidate_floor),
         plan <-
           CompactionPlan.plan(
             Map.merge(plan_input, %{
               candidate_floor: frontier.candidate_floor,
               history_depth: history_depth(meta.config),
               boundaries: boundaries,
               peers: peers,
               peer_installed_epochs: peer_installed_epochs(peers),
               compaction_epoch: meta.compaction_epoch,
               now_ms: now_ms
             })
           ) do
      case maybe_noop(frontier, plan, mode, meta) do
        {:noop, stats} -> {:ok, stats}
        :ok -> apply_plan(adapter, meta, frontier, plan, peers, maintenance_counter)
        {:error, error} -> {:error, error}
      end
    end
  end

  @spec retention_state(Connection.handle(), map()) ::
          {:ok, RetentionState.t()} | {:error, ElixirDB.Error.t()}
  def retention_state(conn, config) do
    with {:ok, meta} <- Meta.load(conn),
         {:ok, maintenance_counter} <- RetentionRecords.maintenance_counter(conn) do
      RetentionState.new(%{
        history_epoch: meta.history_epoch,
        floor_sequence: meta.retention_floor_sequence,
        compaction_epoch: meta.compaction_epoch,
        boundary_digest: meta.retention_boundary_digest,
        mode: retention_mode_atom(config),
        maintenance_counter: maintenance_counter
      })
    end
  end

  @spec list_peer_positions(Connection.handle()) ::
          {:ok, [PeerPosition.t()]} | {:error, ElixirDB.Error.t()}
  def list_peer_positions(conn), do: RetentionRecords.list_peers(conn)

  @spec put_peer_position_cas(map(), map()) :: {:ok, map()} | {:error, ElixirDB.Error.t()}
  def put_peer_position_cas(adapter, request) when is_map(request) do
    peer_uuid =
      ElixirDB.MapAccess.get(request, :peer_database_uuid) ||
        request |> ElixirDB.MapAccess.get(:value, %{}) |> peer_uuid_from_value()

    with {:ok, peer} <- decode_peer(ElixirDB.MapAccess.get(request, :value)),
         :ok <- validate_peer_regression(adapter.conn, peer),
         wire <- peer_wire(peer) do
      LocalRecords.put_cas_tx(adapter, %{
        namespace: RetentionRecords.peer_ledger_namespace(),
        key: peer_uuid,
        expected_version: ElixirDB.MapAccess.get(request, :expected_version, 0),
        value: wire
      })
    end
  end

  @spec read_boundary_pages(Connection.handle(), map()) ::
          {:ok, BoundaryPage.t()} | {:error, ElixirDB.Error.t()}
  def read_boundary_pages(conn, request) when is_map(request) do
    cursor =
      ElixirDB.MapAccess.get(request, :cursor) || ElixirDB.MapAccess.get(request, :page_cursor)

    limit = ElixirDB.MapAccess.get(request, :limit, @page_size)
    requested_epoch = ElixirDB.MapAccess.get(request, :compaction_epoch)
    requested_history = ElixirDB.MapAccess.get(request, :source_history_epoch)

    with {:ok, meta} <- Meta.load(conn),
         :ok <- validate_boundary_request(meta, requested_history, requested_epoch),
         {:ok, boundaries} <- RetentionRecords.list_boundaries(conn),
         {:ok, page_boundaries, next_page} <- paginate_boundaries(boundaries, cursor, limit),
         digest <- BoundaryPage.digest_for(Enum.map(page_boundaries, & &1.boundary)) do
      BoundaryPage.page(
        meta.history_epoch,
        meta.compaction_epoch,
        digest,
        next_page,
        Enum.map(page_boundaries, & &1.boundary)
      )
    end
  end

  @spec install_boundary_pages(Connection.handle(), map() | BoundaryPage.t()) ::
          {:ok, map()} | {:error, ElixirDB.Error.t()}
  def install_boundary_pages(conn, %BoundaryPage{} = page) do
    install_boundary_page(conn, page)
  end

  def install_boundary_pages(conn, request) when is_map(request) do
    case decode_boundary_page(request) do
      {:ok, page} -> install_boundary_page(conn, page)
      {:error, _} = error -> error
    end
  end

  defp install_boundary_page(conn, page) do
    with {:ok, meta} <- Meta.load(conn),
         :ok <- validate_boundary_install(meta, page),
         :ok <- upsert_boundary_page(conn, page) do
      digest = BoundaryPage.digest_for(page.boundaries)

      {:ok,
       %{
         installed: length(page.boundaries),
         boundary_digest: digest,
         compaction_epoch: page.compaction_epoch
       }}
    end
  end

  defp decode_boundary_page(value) when is_map(value) do
    if Enum.any?(Map.keys(value), &is_atom/1),
      do: BoundaryPage.new(value),
      else: BoundaryPage.from_wire(value)
  end

  defp compute_frontier(meta, peers, :disabled, _now, _now_ms) do
    {:ok,
     Frontier.compute(%{
       source_history_epoch: meta.history_epoch,
       current_sequence: meta.current_sequence,
       current_floor: meta.retention_floor_sequence,
       mode: :disabled,
       peers: peers,
       now: DateTime.utc_now()
     })}
  end

  defp compute_frontier(meta, peers, :stable_frontier, now, now_ms) do
    {:ok,
     Frontier.compute(%{
       source_history_epoch: meta.history_epoch,
       current_sequence: meta.current_sequence,
       current_floor: meta.retention_floor_sequence,
       mode: :stable_frontier,
       peers: update_expired_peer_statuses(meta, peers, now_ms),
       now: now
     })}
  end

  defp update_expired_peer_statuses(_meta, peers, now_ms) do
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

  defp noop_stats(frontier, meta) do
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

  defp work_empty?(plan) do
    plan.removals == %{} and plan.boundaries_to_upsert == [] and
      plan.boundaries_to_remove == [] and plan.change_sequences_to_delete == []
  end

  defp apply_plan(adapter, meta, frontier, plan, peers, maintenance_counter) do
    now_ms = DateTime.to_unix(DateTime.utc_now(), :millisecond)
    conn = adapter.conn
    new_floor = frontier.candidate_floor
    floor_advanced = new_floor > meta.retention_floor_sequence

    new_compaction_epoch =
      RetentionState.compaction_epoch_after(floor_advanced, meta.compaction_epoch)

    work? = not work_empty?(plan)

    with :ok <- persist_peer_statuses(conn, peers, now_ms),
         :ok <- upsert_boundaries(conn, plan.boundaries_to_upsert, new_compaction_epoch),
         :ok <- remove_boundaries(conn, plan.boundaries_to_remove),
         :ok <- delete_revisions(conn, plan.removals),
         :ok <- delete_changes(conn, plan.change_sequences_to_delete),
         :ok <- update_meta(conn, new_floor, new_compaction_epoch, conn_digest(conn)),
         :ok <- maybe_increment_maintenance(conn, maintenance_counter, work?),
         :ok <-
           RetentionRecords.put_last_result(conn, compaction_stats(meta, frontier, plan, work?)) do
      {:ok,
       %{
         noop?: not work?,
         old_floor: meta.retention_floor_sequence,
         new_floor: new_floor,
         old_compaction_epoch: meta.compaction_epoch,
         new_compaction_epoch: new_compaction_epoch,
         removed_changes: plan.stats.changes_removed,
         removed_revisions: plan.stats.revisions_removed,
         removed_boundaries: plan.stats.boundaries_removed,
         active_peer_count: frontier.active_peer_count,
         expired_peer_count: frontier.expired_peer_count,
         blocking_peer_count: frontier.blocking_peer_count,
         bootstrap_required_count: frontier.bootstrap_required_count
       }}
    end
  end

  defp compaction_stats(meta, frontier, plan, work?) do
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

  defp load_plan_input(conn, _meta, _candidate_floor) do
    case Connection.query(
           conn,
           "SELECT doc_key, document_id, winning_revision, update_sequence FROM documents"
         ) do
      {:ok, rows} ->
        documents =
          Enum.map(rows, fn [doc_key, document_id, winning_revision, update_sequence] ->
            revisions = load_document_revisions(conn, doc_key)

            %{
              document_id: document_id,
              latest_change_sequence: update_sequence,
              winning_revision: winning_revision,
              revisions: revisions
            }
          end)

        {:ok, %{documents: documents}}

      {:error, reason} ->
        {:error, normalize_error(reason)}
    end
  end

  defp load_document_revisions(conn, doc_key) do
    case Connection.query(
           conn,
           "SELECT revision_id, generation, parent_revision, history_id, digest, deleted, body_json, insertion_sequence FROM revisions WHERE doc_key = ?",
           [doc_key]
         ) do
      {:ok, rows} ->
        Enum.map(rows, fn row -> Revisions.from_row(row) end)

      _ ->
        []
    end
  end

  defp delete_revisions(_conn, removals) when map_size(removals) == 0, do: :ok

  defp delete_revisions(conn, removals) do
    Enum.reduce_while(removals, :ok, fn {document_id, revision_ids}, :ok ->
      delete_document_revisions(conn, document_id, revision_ids)
    end)
    |> normalize_delete_result()
  end

  defp delete_document_revisions(conn, document_id, revision_ids) do
    case doc_key_for(conn, document_id) do
      {:ok, doc_key} -> delete_revision_ids(conn, doc_key, revision_ids)
      {:error, error} -> {:halt, error}
    end
  end

  defp delete_revision_ids(conn, doc_key, revision_ids) do
    Enum.reduce_while(revision_ids, :ok, fn revision_id, :ok ->
      delete_single_revision(conn, doc_key, revision_id)
    end)
    |> case do
      :ok -> {:cont, :ok}
      {:error, error} -> {:halt, error}
    end
  end

  defp delete_single_revision(conn, doc_key, revision_id) do
    case Connection.execute(
           conn,
           "DELETE FROM revisions WHERE doc_key = ? AND revision_id = ?",
           [doc_key, revision_id]
         ) do
      :ok -> {:cont, :ok}
      {:error, reason} -> {:halt, {:error, normalize_error(reason)}}
    end
  end

  defp normalize_delete_result(:ok), do: :ok
  defp normalize_delete_result({:error, error}), do: {:error, error}

  defp delete_changes(_conn, []), do: :ok

  defp delete_changes(conn, sequences) do
    max = Enum.max(sequences)

    case Connection.execute(conn, "DELETE FROM changes WHERE sequence <= ?", [max]) do
      :ok -> :ok
      {:error, reason} -> {:error, normalize_error(reason)}
    end
  end

  defp upsert_boundaries(_conn, [], _epoch), do: :ok

  defp upsert_boundaries(conn, boundaries, epoch) do
    Enum.reduce_while(boundaries, :ok, fn boundary, :ok ->
      upsert_boundary(conn, boundary, epoch)
    end)
  end

  defp upsert_boundary(conn, boundary, epoch) do
    key = RetentionRecords.boundary_key(boundary.document_id, boundary.history_id)
    value = RetentionRecords.encode_boundary(boundary, epoch)

    case Canonical.encode(value) do
      {:ok, json} -> execute_boundary_upsert(conn, key, json)
      {:error, reason} -> {:halt, {:error, normalize_error(reason)}}
    end
  end

  defp execute_boundary_upsert(conn, key, json) do
    case Connection.execute(
           conn,
           "INSERT INTO local_records(namespace, record_key, record_version, value_json) VALUES (?, ?, 1, ?) ON CONFLICT(namespace, record_key) DO UPDATE SET record_version = record_version + 1, value_json = excluded.value_json",
           [RetentionRecords.retention_boundaries_namespace(), key, json]
         ) do
      :ok -> {:cont, :ok}
      {:error, reason} -> {:halt, {:error, normalize_error(reason)}}
    end
  end

  defp remove_boundaries(_conn, []), do: :ok

  defp remove_boundaries(conn, boundaries) do
    Enum.reduce_while(boundaries, :ok, fn boundary, :ok ->
      key = RetentionRecords.boundary_key(boundary.document_id, boundary.history_id)

      case Connection.execute(
             conn,
             "DELETE FROM local_records WHERE namespace = ? AND record_key = ?",
             [RetentionRecords.retention_boundaries_namespace(), key]
           ) do
        :ok -> {:cont, :ok}
        {:error, reason} -> {:halt, {:error, normalize_error(reason)}}
      end
    end)
  end

  defp update_meta(conn, floor, compaction_epoch, digest) do
    Connection.execute(
      conn,
      "UPDATE db_meta SET retention_floor_sequence = ?, compaction_epoch = ?, retention_boundary_digest = ? WHERE id = 1",
      [floor, compaction_epoch, digest]
    )
    |> case do
      :ok -> :ok
      {:error, reason} -> {:error, normalize_error(reason)}
    end
  end

  defp conn_digest(conn) do
    case RetentionRecords.list_boundaries(conn) do
      {:ok, boundaries} ->
        BoundaryPage.digest_for(Enum.map(boundaries, & &1.boundary))

      _ ->
        nil
    end
  end

  defp maybe_increment_maintenance(_conn, _counter, false), do: :ok

  defp maybe_increment_maintenance(conn, counter, true),
    do: RetentionRecords.put_maintenance_counter(conn, counter + 1)

  defp persist_peer_statuses(_conn, [], _now_ms), do: :ok

  defp persist_peer_statuses(conn, peers, now_ms) do
    Enum.reduce_while(peers, :ok, fn peer, :ok ->
      persist_peer_status(conn, peer, now_ms)
    end)
  end

  defp persist_peer_status(conn, peer, now_ms) do
    if PeerPosition.expired?(peer, now_ms) and peer.status != :expired do
      persist_expired_peer(conn, %{peer | status: :expired})
    else
      {:cont, :ok}
    end
  end

  defp persist_expired_peer(conn, peer) do
    case Canonical.encode(peer_wire(peer)) do
      {:ok, json} -> execute_peer_status_upsert(conn, peer.peer_database_uuid, json)
      {:error, reason} -> {:halt, {:error, normalize_error(reason)}}
    end
  end

  defp execute_peer_status_upsert(conn, key, json) do
    case Connection.execute(
           conn,
           "INSERT INTO local_records(namespace, record_key, record_version, value_json) VALUES (?, ?, 1, ?) ON CONFLICT(namespace, record_key) DO UPDATE SET value_json = excluded.value_json",
           [RetentionRecords.peer_ledger_namespace(), key, json]
         ) do
      :ok -> {:cont, :ok}
      {:error, reason} -> {:halt, {:error, normalize_error(reason)}}
    end
  end

  defp validate_boundary_request(_meta, nil, _), do: :ok

  defp validate_boundary_request(meta, history, _) when is_binary(history) do
    if history == meta.history_epoch,
      do: :ok,
      else:
        {:error,
         ElixirDB.Error.boundary_conflict("boundary request history epoch does not match", %{
           expected: meta.history_epoch,
           received: history
         })}
  end

  defp validate_boundary_request(_meta, _history, nil), do: :ok

  defp validate_boundary_request(meta, _history, epoch) when is_integer(epoch) and epoch >= 0 do
    if epoch <= meta.compaction_epoch,
      do: :ok,
      else:
        {:error,
         ElixirDB.Error.boundary_conflict("boundary request compaction epoch is not available", %{
           current: meta.compaction_epoch,
           requested: epoch
         })}
  end

  defp validate_boundary_install(meta, page) do
    retention_mode = get_in(meta.config, ["retention", "mode"]) || "disabled"

    cond do
      page.source_history_epoch != meta.history_epoch and retention_mode != "disabled" ->
        {:error,
         ElixirDB.Error.boundary_conflict("boundary page history epoch does not match", %{
           expected: meta.history_epoch,
           received: page.source_history_epoch
         })}

      page.compaction_epoch < meta.compaction_epoch ->
        {:error,
         ElixirDB.Error.boundary_conflict("boundary page compaction epoch regressed", %{
           current: meta.compaction_epoch,
           received: page.compaction_epoch
         })}

      page.boundary_digest != BoundaryPage.digest_for(page.boundaries) ->
        {:error, ElixirDB.Error.boundary_conflict("boundary page digest mismatch")}

      true ->
        :ok
    end
  end

  defp upsert_boundary_page(conn, page) do
    Enum.reduce_while(page.boundaries, :ok, fn boundary, :ok ->
      upsert_boundaries(conn, [boundary], page.compaction_epoch)
    end)
  end

  defp paginate_boundaries(boundaries, cursor, limit) do
    sorted = sort_boundaries(boundaries)
    start_index = boundary_start_index(sorted, cursor)
    page = Enum.slice(sorted, start_index, limit)
    next_page = next_boundary_cursor(sorted, start_index, page)
    {:ok, page, next_page}
  end

  defp sort_boundaries(boundaries) do
    Enum.sort_by(boundaries, fn %{boundary: boundary} ->
      {boundary.document_id, boundary.history_id}
    end)
  end

  defp boundary_start_index(_sorted, nil), do: 0

  defp boundary_start_index(sorted, key) when is_binary(key) do
    case Enum.find_index(sorted, fn %{boundary: boundary} ->
           RetentionRecords.boundary_key(boundary.document_id, boundary.history_id) == key
         end) do
      nil -> 0
      index -> index + 1
    end
  end

  defp next_boundary_cursor(sorted, start_index, page) do
    case Enum.at(sorted, start_index + length(page)) do
      %{boundary: boundary} ->
        RetentionRecords.boundary_key(boundary.document_id, boundary.history_id)

      _ ->
        nil
    end
  end

  defp validate_peer_regression(conn, incoming) do
    case RetentionRecords.list_peers(conn) do
      {:ok, peers} -> check_peer_regression(peers, incoming)
      {:error, error} -> {:error, error}
    end
  end

  defp check_peer_regression(peers, incoming) do
    case Enum.find(peers, &(&1.peer_database_uuid == incoming.peer_database_uuid)) do
      nil ->
        :ok

      previous ->
        if PeerPosition.regresses?(previous, incoming),
          do:
            {:error,
             ElixirDB.Error.rebase_required("peer position regressed", %{
               peer_database_uuid: incoming.peer_database_uuid
             })},
          else: :ok
    end
  end

  defp decode_peer(value) when is_map(value) do
    if Enum.any?(Map.keys(value), &is_atom/1),
      do: PeerPosition.new(value),
      else: PeerPosition.from_wire(value)
  end

  defp peer_wire(%PeerPosition{} = peer) do
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

  defp peer_uuid_from_value(%{"peer_database_uuid" => uuid}), do: uuid
  defp peer_uuid_from_value(%{peer_database_uuid: uuid}), do: uuid
  defp peer_uuid_from_value(_), do: nil

  defp peer_installed_epochs(peers) do
    Map.new(peers, fn peer -> {peer.peer_database_uuid, peer.installed_source_compaction_epoch} end)
  end

  defp retention_mode(config) do
    case get_in(config, ["retention", "mode"]) do
      "stable_frontier" -> :stable_frontier
      _ -> :disabled
    end
  end

  defp retention_mode_atom(config) do
    case get_in(config, ["retention", "mode"]) do
      "stable_frontier" -> :stable_frontier
      _ -> :disabled
    end
  end

  defp history_depth(config), do: get_in(config, ["retention", "history_depth"]) || 0

  defp doc_key_for(conn, document_id) do
    case Connection.query(conn, "SELECT doc_key FROM documents WHERE document_id = ?", [document_id]) do
      {:ok, [[doc_key]]} -> {:ok, doc_key}
      {:ok, []} -> {:error, ElixirDB.Error.document_not_found("document not found")}
      {:error, reason} -> {:error, normalize_error(reason)}
    end
  end

  defp normalize_error(%ElixirDB.Error{} = error), do: error

  defp normalize_error(reason),
    do: ElixirDB.Error.internal_error("SQLite operation failed", %{cause: inspect(reason)})
end
