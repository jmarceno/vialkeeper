defmodule ElixirDB.Runtime.DatabaseOwner do
  @moduledoc false
  use GenServer
  alias ElixirDB.Commands
  alias ElixirDB.DatabaseBundle
  alias ElixirDB.MapAccess
  alias ElixirDB.Observability.Instrumentation.Compact
  alias ElixirDB.Runtime.{AttachmentCoordinator, ChangeNotifier, RetentionScheduler}
  alias ElixirDB.Storage.Results
  alias ElixirDB.Storage.SQLite.Adapter

  def start_link({uuid, %DatabaseBundle{} = bundle}),
    do: GenServer.start_link(__MODULE__, {uuid, bundle}, name: via(uuid))

  def via(uuid), do: {:via, Registry, {ElixirDB.Runtime.DatabaseRegistry, {:owner, uuid}}}

  def command(uuid, command, timeout \\ 30_000) do
    case Registry.lookup(ElixirDB.Runtime.DatabaseRegistry, {:owner, uuid}) do
      [{pid, _}] -> GenServer.call(pid, command, timeout)
      [] -> {:error, ElixirDB.Error.database_closed("database owner is not running")}
    end
  end

  @impl true
  def init({uuid, %DatabaseBundle{} = bundle}) do
    path = DatabaseBundle.sqlite_path(bundle)

    case Adapter.open(path) do
      {:ok, adapter} ->
        case Map.get(adapter.identity, :database_uuid) do
          ^uuid ->
            {:ok, %{uuid: uuid, path: path, bundle: bundle, adapter: adapter}}

          actual ->
            _ = Adapter.close(adapter)

            {:stop,
             ElixirDB.Error.database_unavailable("database UUID mismatch", %{
               reason: :uuid_mismatch,
               expected: uuid,
               actual: actual
             })}
        end

      {:error, %ElixirDB.Error{} = error} ->
        {:stop, error}
    end
  end

  @impl true
  def handle_call(command, from, state) do
    handle_command(Commands.normalize(command), from, state)
  end

  defp handle_command(%Commands.Identity{}, _from, state),
    do: reply(Adapter.identity(state.adapter), state)

  defp handle_command(%Commands.UpdateConfig{request: request}, _from, state) do
    case Adapter.update_config(state.adapter, request) do
      {:ok, config} = ok ->
        RetentionScheduler.reschedule(state.uuid)
        AttachmentCoordinator.update_limits(state.uuid, Map.get(config, "attachments", %{}))
        {:reply, ok, state}

      {:error, _} = error ->
        {:reply, error, state}
    end
  end

  defp handle_command(%Commands.IntegrityCheck{request: request}, _from, state),
    do: reply(Adapter.integrity_check(state.adapter, request), state)

  defp handle_command(%Commands.GetDocument{request: request}, _from, state),
    do: reply(wrap_get(Adapter.get_document(state.adapter, request)), state)

  defp handle_command(%Commands.GetRevision{request: request}, _from, state),
    do: reply(wrap_get(Adapter.get_revision(state.adapter, request)), state)

  defp handle_command(%Commands.PutDocument{request: request}, _from, state),
    do:
      mutate(
        wrap_put(Adapter.apply_local_mutation(state.adapter, Map.put(request, :operation, :put))),
        state
      )

  defp handle_command(%Commands.DeleteDocument{request: request}, _from, state),
    do:
      mutate(
        wrap_put(
          Adapter.apply_local_mutation(state.adapter, Map.put(request, :operation, :delete))
        ),
        state
      )

  defp handle_command(%Commands.BulkWrite{request: request}, _from, state),
    do: mutate(Adapter.apply_bulk_mutation(state.adapter, request), state)

  defp handle_command(%Commands.ResolveConflict{request: request}, _from, state),
    do: mutate(Adapter.resolve_conflict(state.adapter, request), state)

  defp handle_command(%Commands.ReadChanges{request: request}, _from, state),
    do: reply(wrap_changes(Adapter.read_changes(state.adapter, request)), state)

  defp handle_command(%Commands.DiffRevisions{request: request}, _from, state),
    do: reply(Adapter.diff_revisions(state.adapter, request), state)

  defp handle_command(%Commands.GetRevisionChains{request: request}, _from, state),
    do: reply(Adapter.get_revision_chains(state.adapter, request), state)

  defp handle_command(
         %Commands.ImportRevisionChains{request: request},
         _from,
         state
       ),
       do: mutate(Adapter.import_revision_chains(state.adapter, request), state)

  defp handle_command(
         %Commands.GetLocalRecord{namespace: namespace, key: key},
         _from,
         state
       ),
       do: reply(Adapter.get_local_record(state.adapter, namespace, key), state)

  defp handle_command(%Commands.GetCheckpoint{replication_id: id}, _from, state),
    do: reply(Adapter.get_local_record(state.adapter, "checkpoints", id), state)

  defp handle_command(%Commands.PutLocalRecord{request: request}, _from, state),
    do: reply(Adapter.put_local_record_cas(state.adapter, request), state)

  defp handle_command(%Commands.PutCheckpoint{request: request}, _from, state),
    do: reply(Adapter.put_local_record_cas(state.adapter, request), state)

  defp handle_command(%Commands.ListIndexes{}, _from, state),
    do: reply(Adapter.list_indexes(state.adapter), state)

  defp handle_command(%Commands.CreateIndex{request: request}, _from, state),
    do: reply(Adapter.create_index(state.adapter, request), state)

  defp handle_command(%Commands.DeleteIndex{index_id: index_id}, _from, state),
    do: reply(Adapter.delete_index(state.adapter, index_id), state)

  defp handle_command(%Commands.RebuildIndex{index_id: index_id}, _from, state),
    do: reply(Adapter.rebuild_index(state.adapter, index_id), state)

  defp handle_command(%Commands.ExecuteQuery{request: request}, _from, state),
    do: reply(Adapter.execute_query(state.adapter, request), state)

  defp handle_command(%Commands.ExplainQuery{request: request}, _from, state),
    do: reply(Adapter.explain_query(state.adapter, request), state)

  defp handle_command(%Commands.ListJobs{}, _from, state),
    do: reply(Adapter.list_replication_jobs(state.adapter), state)

  defp handle_command(%Commands.PutJob{request: request}, _from, state),
    do: reply(Adapter.put_replication_job(state.adapter, request), state)

  defp handle_command(%Commands.DeleteJob{job_id: job_id}, _from, state),
    do: reply(Adapter.delete_replication_job(state.adapter, job_id), state)

  defp handle_command(%Commands.CompactRetention{request: request}, _from, state),
    do: compact(request, state)

  defp handle_command(%Commands.RetentionStatus{}, _from, state),
    do: reply(Adapter.retention_state(state.adapter), state)

  defp handle_command(%Commands.ListPeerPositions{}, _from, state),
    do: reply(Adapter.list_peer_positions(state.adapter), state)

  defp handle_command(%Commands.PutPeerPositionCas{request: request}, _from, state),
    do: reply(Adapter.put_peer_position_cas(state.adapter, request), state)

  defp handle_command(%Commands.ReadBoundaryPages{request: request}, _from, state),
    do: reply(Adapter.read_boundary_pages(state.adapter, request), state)

  defp handle_command(%Commands.InstallBoundaryPages{request: request}, _from, state),
    do: reply(Adapter.install_boundary_pages(state.adapter, request), state)

  defp handle_command(
         %Commands.HasLocalOriginChanges{peer_database_uuid: peer_database_uuid},
         _from,
         state
       ),
       do: reply(Adapter.has_local_origin_changes?(state.adapter, peer_database_uuid), state)

  defp handle_command(
         %Commands.ClearPendingLocalCausal{peer_database_uuid: peer_database_uuid},
         _from,
         state
       ),
       do: reply(Adapter.clear_pending_local_causal(state.adapter, peer_database_uuid), state)

  defp handle_command(%Commands.ResolveAttachmentTicket{request: request}, _from, state),
    do: reply(Adapter.resolve_attachment_ticket(state.adapter, request), state)

  defp handle_command(%Commands.ResolveBlobMetadata{request: request}, _from, state),
    do: reply(Adapter.resolve_blob_metadata(state.adapter, request), state)

  defp handle_command(%Commands.ProtectPendingBlob{request: request}, _from, state),
    do: reply(Adapter.protect_pending_blob(state.adapter, request), state)

  defp handle_command(%Commands.RemovePendingBlobProtection{request: request}, _from, state),
    do: reply(Adapter.remove_pending_blob_protection(state.adapter, request), state)

  defp handle_command(%Commands.ListLiveAttachmentDigests{request: request}, _from, state),
    do: reply(Adapter.list_live_attachment_digests(state.adapter, request), state)

  defp handle_command(%Commands.CleanupExpiredPendingBlobs{request: request}, _from, state),
    do: reply(Adapter.cleanup_expired_pending_blobs(state.adapter, request), state)

  defp handle_command(%Commands.Close{}, _from, state),
    do: {:stop, :shutdown, :ok, state}

  defp handle_command(_unknown, _from, state),
    do: {:reply, {:error, ElixirDB.Error.invalid_request("unknown database command")}, state}

  @impl true
  def terminate(_reason, %{adapter: adapter}), do: Adapter.close(adapter)

  defp compact(request, state) do
    trigger = compact_trigger(request)

    result =
      Compact.run(state.uuid, trigger, fn ->
        Adapter.compact_retention(state.adapter, request)
      end)

    case result do
      {:ok, stats} ->
        maybe_publish_maintenance(state.uuid, stats)
        {:reply, result, state}

      {:error, _} ->
        {:reply, result, state}
    end
  end

  defp compact_trigger(request) when is_map(request) do
    case MapAccess.get(request, :trigger) do
      :scheduled -> :scheduled
      "scheduled" -> :scheduled
      _ -> :explicit
    end
  end

  defp compact_trigger(_), do: :explicit

  defp maybe_publish_maintenance(uuid, stats) do
    old_floor = Map.get(stats, :old_floor, 0)
    new_floor = Map.get(stats, :new_floor, 0)
    old_epoch = Map.get(stats, :old_compaction_epoch, 0)
    new_epoch = Map.get(stats, :new_compaction_epoch, 0)

    if new_floor > old_floor or new_epoch > old_epoch do
      ChangeNotifier.publish_maintenance(uuid, %{
        database_uuid: uuid,
        new_floor: new_floor,
        new_compaction_epoch: new_epoch,
        event_kind: :compaction
      })
    end

    :ok
  end

  defp mutate({:ok, %{sequence: sequence} = value}, state) do
    ChangeNotifier.publish(state.uuid, sequence)
    {:reply, {:ok, value}, state}
  end

  defp mutate({:ok, value}, state), do: {:reply, {:ok, value}, state}
  defp mutate({:error, _} = result, state), do: {:reply, result, state}
  defp reply(result, state), do: {:reply, result, state}

  defp wrap_get({:ok, map}) when is_map(map),
    do: {:ok, Results.get_document(map)}

  defp wrap_get(other), do: other

  defp wrap_put({:ok, map}) when is_map(map),
    do: {:ok, Results.put_document(map)}

  defp wrap_put(other), do: other

  defp wrap_changes({:ok, map}) when is_map(map),
    do: {:ok, Results.read_changes(map)}

  defp wrap_changes(other), do: other
end
