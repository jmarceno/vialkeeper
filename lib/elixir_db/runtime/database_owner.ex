defmodule ElixirDB.Runtime.DatabaseOwner do
  @moduledoc "Serializes database commands through one owner process."
  use GenServer
  require Logger
  alias ElixirDB.Commands
  alias ElixirDB.DatabaseBundle
  alias ElixirDB.DerivedView.Manager, as: DerivedViewManager
  alias ElixirDB.MapAccess
  alias ElixirDB.Observability.Instrumentation.Compact
  alias ElixirDB.Runtime.{AttachmentCoordinator, ChangeNotifier, RetentionScheduler}
  alias ElixirDB.Storage.Registry, as: StorageRegistry
  alias ElixirDB.Storage.Results
  alias ElixirDB.Storage.Services

  def start_link({uuid, %DatabaseBundle{} = bundle}),
    do: start_link({uuid, bundle, nil})

  def start_link({uuid, %DatabaseBundle{} = bundle, expected_kind}),
    do: GenServer.start_link(__MODULE__, {uuid, bundle, expected_kind}, name: via(uuid))

  def via(uuid), do: {:via, Registry, {ElixirDB.Runtime.DatabaseRegistry, {:owner, uuid}}}

  def command(uuid, command, timeout \\ 30_000) do
    case Registry.lookup(ElixirDB.Runtime.DatabaseRegistry, {:owner, uuid}) do
      [{pid, _}] -> GenServer.call(pid, command, timeout)
      [] -> {:error, ElixirDB.Error.database_closed("database owner is not running")}
    end
  end

  @spec sync(binary(), timeout()) :: :ok
  def sync(uuid, timeout \\ 5_000) when is_binary(uuid) do
    case Registry.lookup(ElixirDB.Runtime.DatabaseRegistry, {:owner, uuid}) do
      [{pid, _}] -> GenServer.call(pid, :sync, timeout)
      [] -> :ok
    end
  end

  @impl true
  def init({uuid, %DatabaseBundle{} = bundle, expected_kind}) do
    backend = StorageRegistry.backend()
    path = backend.artifact_path(DatabaseBundle.root(bundle))

    case backend.open(path) do
      {:ok, adapter} ->
        open_owner(backend, adapter, path, bundle, uuid, expected_kind)

      {:error, %ElixirDB.Error{} = error} ->
        {:stop, error}
    end
  end

  defp open_owner(backend, adapter, path, bundle, uuid, expected_kind) do
    case backend.identity(adapter) do
      {:ok, identity} ->
        accept_or_reject_owner(backend, adapter, path, bundle, uuid, expected_kind, identity)

      {:error, %ElixirDB.Error{} = error} ->
        _ = backend.close(adapter)
        {:stop, error}
    end
  end

  defp accept_or_reject_owner(backend, adapter, _path, bundle, uuid, expected_kind, identity) do
    actual_uuid = MapAccess.get(identity, :database_uuid)
    actual_kind = MapAccess.get(identity, :database_kind)

    cond do
      actual_uuid == uuid and (is_nil(expected_kind) or expected_kind == actual_kind) ->
        context =
          adapter
          |> backend.to_context()
          |> Map.put(:bundle_root, DatabaseBundle.root(bundle))
          |> Map.put(:identity, identity)
          |> Map.put(:capabilities, backend_capabilities(backend))

        {:ok, %{uuid: uuid, bundle: bundle, context: context}}

      actual_uuid == uuid ->
        _ = backend.close(adapter)

        {:stop,
         ElixirDB.Error.integrity_violation(
           "database kind does not match registration hint",
           ElixirDB.Error.identity_mismatch_details(
             :database_kind_mismatch,
             expected_kind,
             actual_kind
           )
         )}

      true ->
        _ = backend.close(adapter)

        {:stop,
         ElixirDB.Error.database_unavailable(
           "database UUID mismatch",
           ElixirDB.Error.identity_mismatch_details(:uuid_mismatch, uuid, actual_uuid)
         )}
    end
  end

  defp backend_capabilities(backend) do
    if function_exported?(backend, :capabilities_report, 0) do
      backend.capabilities_report()
    else
      %{}
    end
  end

  @impl true
  def handle_call(:sync, _from, state), do: {:reply, :ok, state}

  def handle_call(command, from, state) do
    handle_command(Commands.normalize(command), from, state)
  catch
    kind, reason ->
      Logger.error("database owner command raised",
        kind: kind,
        reason: inspect(reason)
      )

      {:reply,
       {:error,
        ElixirDB.Error.internal_error("database command failed", %{
          cause: inspect(reason),
          kind: kind
        })}, state}
  end

  defp handle_command(%Commands.Identity{}, _from, state),
    do: reply(Services.identity(state.context), state)

  defp handle_command(%Commands.UpdateConfig{request: request}, _from, state) do
    case Services.update_config(state.context, request) do
      {:ok, config} = ok ->
        RetentionScheduler.reschedule(state.uuid)
        AttachmentCoordinator.update_limits(state.uuid, Map.get(config, "attachments", %{}))
        {:reply, ok, put_config(state, config)}

      {:error, _} = error ->
        {:reply, error, state}
    end
  end

  defp handle_command(%Commands.IntegrityCheck{request: request}, _from, state),
    do: reply(Services.integrity_check(state.context, request), state)

  defp handle_command(%Commands.GetDocument{request: request}, _from, state),
    do: reply(wrap_get(Services.get_document(state.context, request)), state)

  defp handle_command(%Commands.GetRevision{request: request}, _from, state),
    do: reply(wrap_get(Services.get_revision(state.context, request)), state)

  defp handle_command(%Commands.PutDocument{request: request}, _from, state),
    do:
      writable_mutation(state, fn ->
        mutate(
          wrap_put(
            Services.apply_local_mutation(state.context, Map.put(request, :operation, :put))
          ),
          state
        )
      end)

  defp handle_command(%Commands.DeleteDocument{request: request}, _from, state),
    do:
      writable_mutation(state, fn ->
        mutate(
          wrap_put(
            Services.apply_local_mutation(state.context, Map.put(request, :operation, :delete))
          ),
          state
        )
      end)

  defp handle_command(%Commands.BulkWrite{request: request}, _from, state),
    do:
      writable_mutation(state, fn ->
        mutate(Services.apply_bulk_mutation(state.context, request), state)
      end)

  defp handle_command(%Commands.ResolveConflict{request: request}, _from, state),
    do:
      writable_mutation(state, fn ->
        mutate(Services.resolve_conflict(state.context, request), state)
      end)

  defp handle_command(%Commands.ReadChanges{request: request}, _from, state),
    do: reply(wrap_changes(Services.read_changes(state.context, request)), state)

  defp handle_command(%Commands.DiffRevisions{request: request}, _from, state),
    do: reply(Services.diff_revisions(state.context, request), state)

  defp handle_command(%Commands.GetRevisionChains{request: request}, _from, state),
    do: reply(Services.get_revision_chains(state.context, request), state)

  defp handle_command(
         %Commands.ImportRevisionChains{request: request},
         _from,
         state
       ),
       do:
         writable_mutation(state, fn ->
           mutate(Services.import_revision_chains(state.context, request), state)
         end)

  defp handle_command(
         %Commands.GetLocalRecord{namespace: namespace, key: key},
         _from,
         state
       ),
       do: reply(Services.get_local_record(state.context, namespace, key), state)

  defp handle_command(%Commands.GetCheckpoint{replication_id: id}, _from, state),
    do: reply(Services.get_local_record(state.context, "checkpoints", id), state)

  defp handle_command(%Commands.PutLocalRecord{request: request}, _from, state),
    do: reply(Services.put_local_record_cas(state.context, request), state)

  defp handle_command(%Commands.PutCheckpoint{request: request}, _from, state),
    do: reply(Services.put_local_record_cas(state.context, request), state)

  defp handle_command(%Commands.ListIndexes{}, _from, state),
    do: reply(Services.list_indexes(state.context), state)

  defp handle_command(%Commands.CreateIndex{request: request}, _from, state),
    do: reply(Services.create_index(state.context, request), state)

  defp handle_command(%Commands.DeleteIndex{index_id: index_id}, _from, state),
    do: reply(Services.delete_index(state.context, index_id), state)

  defp handle_command(%Commands.RebuildIndex{index_id: index_id}, _from, state),
    do: reply(Services.rebuild_index(state.context, index_id), state)

  defp handle_command(%Commands.ExecuteQuery{request: request}, _from, state),
    do: reply(Services.execute_public_query(state.context, request), state)

  defp handle_command(%Commands.ExecuteSubscriptionSnapshot{request: request}, _from, state),
    do: reply(Services.execute_public_subscription_snapshot(state.context, request), state)

  defp handle_command(%Commands.GetRevisionsBatch{requests: requests}, _from, state),
    do: reply(Services.get_revisions_batch(state.context, requests), state)

  defp handle_command(%Commands.ExplainQuery{request: request}, _from, state),
    do: reply(Services.explain_public_query(state.context, request), state)

  defp handle_command(%Commands.ListJobs{}, _from, state),
    do: reply(Services.list_replication_jobs(state.context), state)

  defp handle_command(%Commands.PutJob{request: request}, _from, state),
    do: reply(Services.put_replication_job(state.context, request), state)

  defp handle_command(%Commands.DeleteJob{job_id: job_id}, _from, state),
    do: reply(Services.delete_replication_job(state.context, job_id), state)

  defp handle_command(%Commands.CompactRetention{request: request}, _from, state),
    do: compact(request, state)

  defp handle_command(%Commands.RetentionStatus{}, _from, state),
    do: reply(Services.retention_state(state.context), state)

  defp handle_command(%Commands.ListPeerPositions{}, _from, state),
    do: reply(Services.list_peer_positions(state.context), state)

  defp handle_command(%Commands.PutPeerPositionCas{request: request}, _from, state),
    do: reply(Services.put_peer_position_cas(state.context, request), state)

  defp handle_command(%Commands.ReadBoundaryPages{request: request}, _from, state),
    do: reply(Services.read_boundary_pages(state.context, request), state)

  defp handle_command(%Commands.InstallBoundaryPages{request: request}, _from, state),
    do: reply(Services.install_boundary_pages(state.context, request), state)

  defp handle_command(
         %Commands.HasLocalOriginChanges{peer_database_uuid: peer_database_uuid},
         _from,
         state
       ),
       do: reply(Services.has_local_origin_changes?(state.context, peer_database_uuid), state)

  defp handle_command(
         %Commands.ClearPendingLocalCausal{peer_database_uuid: peer_database_uuid},
         _from,
         state
       ),
       do: reply(Services.clear_pending_local_causal(state.context, peer_database_uuid), state)

  defp handle_command(%Commands.ResolveAttachmentTicket{request: request}, _from, state),
    do: reply(Services.resolve_attachment_ticket(state.context, request), state)

  defp handle_command(%Commands.ResolveBlobMetadata{request: request}, _from, state),
    do: reply(Services.resolve_blob_metadata(state.context, request), state)

  defp handle_command(%Commands.ProtectPendingBlob{request: request}, _from, state),
    do: reply(Services.protect_pending_blob(state.context, request), state)

  defp handle_command(%Commands.RemovePendingBlobProtection{request: request}, _from, state),
    do: reply(Services.remove_pending_blob_protection(state.context, request), state)

  defp handle_command(%Commands.ListLiveAttachmentDigests{request: request}, _from, state),
    do: reply(Services.list_live_attachment_digests(state.context, request), state)

  defp handle_command(%Commands.CleanupExpiredPendingBlobs{request: request}, _from, state),
    do: reply(Services.cleanup_expired_pending_blobs(state.context, request), state)

  defp handle_command(%Commands.ListViews{}, _from, state),
    do: reply(Services.list_views(state.context), state)

  defp handle_command(%Commands.CreateView{request: request}, _from, state),
    do: reply(Services.create_view(state.context, request), state)

  defp handle_command(%Commands.DeleteView{view_id: view_id}, _from, state),
    do: reply(Services.delete_view(state.context, view_id), state)

  defp handle_command(%Commands.ViewState{view_id: view_id}, _from, state),
    do: reply(Services.view_state(state.context, view_id), state)

  defp handle_command(%Commands.ApplyViewBatch{request: request}, _from, state),
    do: reply(Services.apply_view_batch(state.context, request), state)

  defp handle_command(%Commands.BeginViewRebuild{request: request}, _from, state),
    do: reply(Services.begin_view_rebuild(state.context, request), state)

  defp handle_command(%Commands.AppendViewRebuildPage{request: request}, _from, state),
    do: reply(Services.append_view_rebuild_page(state.context, request), state)

  defp handle_command(%Commands.FinishViewRebuild{request: request}, _from, state),
    do: reply(Services.finish_view_rebuild(state.context, request), state)

  defp handle_command(%Commands.QueryView{request: request}, _from, state),
    do: reply(Services.query_view(state.context, request), state)

  defp handle_command(%Commands.ReadWinningDocumentsPage{request: request}, _from, state),
    do: reply(Services.read_winning_documents_page(state.context, request), state)

  defp handle_command(%Commands.GetDerivedView{}, _from, state),
    do: reply(Services.get_derived_view(state.context), state)

  defp handle_command(%Commands.SetDerivedEnabled{request: request}, _from, state),
    do: set_derived_enabled(request, state)

  defp handle_command(%Commands.ListDerivedSources{}, _from, state),
    do: reply(Services.list_derived_sources(state.context), state)

  defp handle_command(%Commands.SetDerivedSourceError{request: request}, _from, state),
    do: reply(Services.set_derived_source_error(state.context, request), state)

  defp handle_command(%Commands.ApplyDerivedSourceBatch{request: request}, _from, state),
    do: mutate(Services.apply_derived_source_batch(state.context, request), state)

  defp handle_command(%Commands.BeginDerivedSourceRebuild{request: request}, _from, state),
    do: reply(Services.begin_derived_source_rebuild(state.context, request), state)

  defp handle_command(%Commands.ApplyDerivedRebuildPage{request: request}, _from, state),
    do: mutate(Services.apply_derived_rebuild_page(state.context, request), state)

  defp handle_command(
         %Commands.PruneDerivedRebuildStalePage{request: request},
         _from,
         state
       ),
       do: mutate(Services.prune_derived_rebuild_stale_page(state.context, request), state)

  defp handle_command(%Commands.FinishDerivedSourceRebuild{request: request}, _from, state),
    do: reply(Services.finish_derived_source_rebuild(state.context, request), state)

  defp handle_command(%Commands.Close{}, _from, state),
    do: {:stop, :shutdown, :ok, state}

  defp handle_command(_unknown, _from, state),
    do: {:reply, {:error, ElixirDB.Error.invalid_request("unknown database command")}, state}

  defp set_derived_enabled(request, state) do
    result = Services.set_derived_enabled(state.context, request)

    case result do
      {:ok, %{enabled: true}} ->
        sources =
          case Services.get_derived_view(state.context) do
            {:ok, %{definition: %{sources: source_uuids}}} -> source_uuids
            _ -> nil
          end

        _ = DerivedViewManager.start(state.uuid, sources)
        reply(result, state)

      {:ok, %{enabled: false}} ->
        _ = DerivedViewManager.close(state.uuid)
        reply(result, state)

      _ ->
        reply(result, state)
    end
  end

  @impl true
  def terminate(_reason, state), do: Services.close(state.context)

  defp compact(request, state) do
    trigger = compact_trigger(request)

    result =
      Compact.run(state.uuid, trigger, fn ->
        Services.compact_retention(state.context, request)
      end)

    case result do
      {:ok, stats} ->
        maybe_publish_maintenance(state.uuid, stats)
        schedule_attachment_gc(state.uuid)
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

  # After compact succeeds: never delete blobs inside the compact storage txn.
  # Spawn so GC can acquire owner admission for live-digest / pending cleanup
  # without re-entering this GenServer call. Module is configured (not aliased)
  # so runtime does not depend on the application Attachments facade.
  defp schedule_attachment_gc(uuid) do
    module = Application.fetch_env!(:elixir_db, :attachment_gc_module)

    case AttachmentCoordinator.schedule_gc(uuid, module) do
      :ok -> :ok
      {:error, _} -> :ok
    end
  end

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

  defp mutate({:ok, %{last_sequence: sequence} = value}, state)
       when is_integer(sequence) and sequence > 0 do
    ChangeNotifier.publish(state.uuid, sequence)
    {:reply, {:ok, value}, state}
  end

  defp mutate({:ok, values} = result, state) when is_list(values) do
    sequence =
      Enum.reduce(values, 0, fn
        %{sequence: value}, maximum when is_integer(value) -> max(maximum, value)
        _value, maximum -> maximum
      end)

    if sequence > 0, do: ChangeNotifier.publish(state.uuid, sequence)
    {:reply, result, state}
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

  defp writable_mutation(state, fun) do
    case database_kind(state) do
      :derived ->
        {:reply,
         {:error,
          ElixirDB.Error.derived_database_read_only(
            "derived databases accept writes only from their materializer"
          )}, state}

      _ ->
        fun.()
    end
  end

  defp database_kind(%{context: context}) do
    MapAccess.get(context.identity, :database_kind, :ordinary)
  end

  defp put_config(state, config) when is_map(config) do
    %{
      state
      | context: %{state.context | identity: Map.put(state.context.identity, :config, config)}
    }
  end
end
