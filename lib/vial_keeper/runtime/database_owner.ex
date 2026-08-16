defmodule VialKeeper.Runtime.DatabaseOwner do
  @moduledoc "Serializes database commands through one owner process."
  use GenServer
  require Logger
  alias VialKeeper.Commands
  alias VialKeeper.DatabaseBundle
  alias VialKeeper.DerivedView.Manager, as: DerivedViewManager
  alias VialKeeper.Error
  alias VialKeeper.MapAccess
  alias VialKeeper.Observability.Instrumentation.Compact
  alias VialKeeper.Observability.Instrumentation.Mutation

  alias VialKeeper.Runtime.{
    AttachmentCoordinator,
    ChangeNotifier,
    CommandContext,
    CommandIO,
    DatabaseCommandPolicy,
    DatabaseReadDispatch,
    RetentionScheduler,
    ShadowBinding
  }

  alias VialKeeper.Storage.Registry, as: StorageRegistry
  alias VialKeeper.Storage.Results
  alias VialKeeper.Storage.Services

  @spec start_link({binary(), DatabaseBundle.t()}) :: GenServer.on_start()
  def start_link({uuid, %DatabaseBundle{} = bundle}),
    do: start_link({uuid, bundle, nil})

  @spec start_link({binary(), DatabaseBundle.t(), atom() | nil}) :: GenServer.on_start()
  def start_link({uuid, %DatabaseBundle{} = bundle, expected_kind}),
    do:
      GenServer.start_link(__MODULE__, {uuid, bundle, expected_kind},
        name: via(uuid, expected_kind || :ordinary)
      )

  @spec child_spec({binary(), DatabaseBundle.t()}) :: map()
  def child_spec({uuid, bundle}), do: child_spec({uuid, bundle, nil})

  @spec child_spec({binary(), DatabaseBundle.t(), atom() | nil}) :: map()
  def child_spec({uuid, _bundle, _kind} = arg) do
    %{
      id: {:database_owner, uuid},
      start: {__MODULE__, :start_link, [arg]},
      restart: :transient,
      type: :worker,
      shutdown: VialKeeper.Config.shutdown_timeout()
    }
  end

  @spec via(binary()) :: {:via, module(), term()}
  def via(uuid), do: via(uuid, nil)

  @spec via(binary(), atom() | nil) :: {:via, module(), term()}
  def via(uuid, kind),
    do: {:via, Registry, {VialKeeper.Runtime.DatabaseRegistry, {:owner, uuid}, kind}}

  @spec command(binary(), term()) :: term() | {:error, Error.t()}
  @spec command(binary(), term(), timeout()) :: term() | {:error, Error.t()}
  def command(uuid, command, timeout \\ 30_000) do
    case Registry.lookup(VialKeeper.Runtime.DatabaseRegistry, {:owner, uuid}) do
      [{pid, _}] ->
        GenServer.call(pid, {:owner_queued_command, System.monotonic_time(), command}, timeout)

      [] ->
        {:error, Error.database_closed("database owner is not running")}
    end
  end

  @doc "Routes a command with an explicit internal authority context."
  @spec command_with_context(binary(), CommandContext.t(), term(), timeout()) :: term()
  def command_with_context(uuid, %CommandContext{} = context, command, timeout \\ 30_000) do
    case Registry.lookup(VialKeeper.Runtime.DatabaseRegistry, {:owner, uuid}) do
      [{pid, _}] ->
        GenServer.call(
          pid,
          {:owner_queued_command, System.monotonic_time(), {:command_context, context, command}},
          timeout
        )

      [] ->
        {:error, Error.database_closed("database owner is not running")}
    end
  end

  @spec sync(binary(), timeout()) :: :ok
  def sync(uuid, timeout \\ 5_000) when is_binary(uuid) do
    case Registry.lookup(VialKeeper.Runtime.DatabaseRegistry, {:owner, uuid}) do
      [{pid, _}] -> GenServer.call(pid, :sync, timeout)
      [] -> :ok
    end
  end

  @doc """
  Returns the writer's opaque backend context so a reader worker can open its
  own readonly connection. The context is a capability token for path and
  identity; callers must not reuse the writer connection.

  The timeout is the lifecycle-grade default because a worker restart can race
  a long writer commit; a short call timeout would silently shrink the pool.
  """
  @spec reader_source(binary(), timeout()) ::
          {:ok, VialKeeper.Storage.BackendContext.t()} | {:error, Error.t()}
  def reader_source(uuid, timeout \\ VialKeeper.Config.shutdown_timeout())
      when is_binary(uuid) do
    case Registry.lookup(VialKeeper.Runtime.DatabaseRegistry, {:owner, uuid}) do
      [{pid, _}] -> GenServer.call(pid, :reader_source, timeout)
      [] -> {:error, Error.database_closed("database owner is not running")}
    end
  end

  @impl true
  def init({uuid, %DatabaseBundle{} = bundle, expected_kind}) do
    backend = StorageRegistry.backend()
    path = backend.artifact_path(DatabaseBundle.root(bundle))

    case backend.open(path) do
      {:ok, adapter} ->
        open_owner(backend, adapter, path, bundle, uuid, expected_kind)

      {:error, %Error{} = error} ->
        {:stop, error}
    end
  end

  defp open_owner(backend, adapter, path, bundle, uuid, expected_kind) do
    case backend.identity(adapter) do
      {:ok, identity} ->
        accept_or_reject_owner(backend, adapter, path, bundle, uuid, expected_kind, identity)

      {:error, %Error{} = error} ->
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

        _ =
          Registry.update_value(VialKeeper.Runtime.DatabaseRegistry, {:owner, uuid}, fn _ ->
            actual_kind || :ordinary
          end)

        {:ok, %{uuid: uuid, bundle: bundle, context: context}}

      actual_uuid == uuid ->
        _ = backend.close(adapter)

        {:stop,
         Error.integrity_violation(
           "database kind does not match registration hint",
           Error.identity_mismatch_details(
             :database_kind_mismatch,
             expected_kind,
             actual_kind
           )
         )}

      true ->
        _ = backend.close(adapter)

        {:stop,
         Error.database_unavailable(
           "database UUID mismatch",
           Error.identity_mismatch_details(:uuid_mismatch, uuid, actual_uuid)
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

  def handle_call(:reader_source, _from, state), do: {:reply, {:ok, state.context}, state}

  def handle_call({:owner_queued_command, queued_at, command}, from, state)
      when is_integer(queued_at) do
    record_owner_queue(command, queued_at)
    handle_call(command, from, state)
  end

  def handle_call({:command_context, %CommandContext{} = context, command}, from, state) do
    safe_dispatch(context, command, from, state)
  end

  def handle_call(command, from, state) do
    safe_dispatch(CommandContext.public(), command, from, state)
  end

  defp safe_dispatch(context, command, from, state) do
    dispatch_command(context, command, from, state)
  catch
    kind, reason ->
      Logger.error("database owner command raised",
        kind: kind,
        reason: Exception.format(kind, reason, __STACKTRACE__)
      )

      {:reply,
       {:error,
        Error.internal_error("database command failed", %{
          cause: inspect(reason),
          kind: kind
        })}, state}
  end

  defp dispatch_command(%CommandContext{} = context, command, from, state) do
    case Commands.normalize(command) do
      %_{} = normalized ->
        with :ok <- DatabaseCommandPolicy.authorize(database_kind(state), context, normalized),
             :ok <- ShadowBinding.check(database_kind(state), state.context, context, state.uuid) do
          handle_command(normalized, from, state)
        else
          {:error, %Error{} = error} -> {:reply, {:error, error}, state}
        end

      other ->
        handle_owner_command(other, from, state)
    end
  end

  defp handle_command(%module{} = command, from, state) do
    case Map.get(CommandIO.classes(), module) do
      :read -> reply(DatabaseReadDispatch.run(state.context, command), state)
      _ -> handle_owner_command(command, from, state)
    end
  end

  defp handle_owner_command(%Commands.UpdateConfig{request: request}, _from, state) do
    case Services.update_config(state.context, request) do
      {:ok, config} = ok ->
        _ = RetentionScheduler.reschedule(state.uuid)
        _ = AttachmentCoordinator.update_limits(state.uuid, Map.get(config, "attachments", %{}))
        {:reply, ok, put_config(state, config)}

      {:error, _} = error ->
        {:reply, error, state}
    end
  end

  defp handle_owner_command(%Commands.IntegrityCheck{request: request}, _from, state),
    do: reply(Services.integrity_check(state.context, request), state)

  defp handle_owner_command(%Commands.PutDocument{request: request}, _from, state),
    do:
      Mutation.with_operation(:put, fn ->
        writable_mutation(state, fn ->
          mutate(
            wrap_put(
              Services.apply_local_mutation(state.context, Map.put(request, :operation, :put))
            ),
            state
          )
        end)
      end)

  defp handle_owner_command(%Commands.DeleteDocument{request: request}, _from, state),
    do:
      Mutation.with_operation(:delete, fn ->
        writable_mutation(state, fn ->
          mutate(
            wrap_put(
              Services.apply_local_mutation(state.context, Map.put(request, :operation, :delete))
            ),
            state
          )
        end)
      end)

  defp handle_owner_command(%Commands.BulkWrite{request: request}, _from, state),
    do:
      Mutation.with_operation(:bulk_write, fn ->
        writable_mutation(state, fn ->
          mutate(Services.apply_bulk_mutation(state.context, request), state)
        end)
      end)

  defp handle_owner_command(%Commands.ResolveConflict{request: request}, _from, state),
    do:
      Mutation.with_operation(:resolve, fn ->
        writable_mutation(state, fn ->
          mutate(Services.resolve_conflict(state.context, request), state)
        end)
      end)

  defp handle_owner_command(
         %Commands.ImportRevisionChains{request: request},
         _from,
         state
       ),
       do:
         writable_mutation(state, fn ->
           mutate(Services.import_revision_chains(state.context, request), state)
         end)

  defp handle_owner_command(%Commands.PutLocalRecord{request: request}, _from, state),
    do: reply(Services.put_local_record_cas(state.context, request), state)

  defp handle_owner_command(%Commands.PutCheckpoint{request: request}, _from, state),
    do: reply(Services.put_local_record_cas(state.context, request), state)

  defp handle_owner_command(%Commands.CreateIndex{request: request}, _from, state),
    do: reply(Services.create_index(state.context, request), state)

  defp handle_owner_command(%Commands.DeleteIndex{index_id: index_id}, _from, state),
    do: reply(Services.delete_index(state.context, index_id), state)

  defp handle_owner_command(%Commands.RebuildIndex{index_id: index_id}, _from, state),
    do: reply(Services.rebuild_index(state.context, index_id), state)

  defp handle_owner_command(%Commands.PutJob{request: request}, _from, state),
    do: reply(Services.put_replication_job(state.context, request), state)

  defp handle_owner_command(%Commands.DeleteJob{job_id: job_id}, _from, state),
    do: reply(Services.delete_replication_job(state.context, job_id), state)

  defp handle_owner_command(%Commands.CompactRetention{request: request}, _from, state),
    do: compact(request, state)

  defp handle_owner_command(%Commands.PutPeerPositionCas{request: request}, _from, state),
    do: reply(Services.put_peer_position_cas(state.context, request), state)

  defp handle_owner_command(%Commands.InstallBoundaryPages{request: request}, _from, state),
    do: reply(Services.install_boundary_pages(state.context, request), state)

  defp handle_owner_command(
         %Commands.ClearPendingLocalCausal{peer_database_uuid: peer_database_uuid},
         _from,
         state
       ),
       do: reply(Services.clear_pending_local_causal(state.context, peer_database_uuid), state)

  defp handle_owner_command(%Commands.ProtectPendingBlob{request: request}, _from, state),
    do: reply(Services.protect_pending_blob(state.context, request), state)

  defp handle_owner_command(%Commands.ProtectPendingBlobs{request: request}, _from, state),
    do: reply(Services.protect_pending_blobs(state.context, request), state)

  defp handle_owner_command(%Commands.RemovePendingBlobProtection{request: request}, _from, state),
    do: reply(Services.remove_pending_blob_protection(state.context, request), state)

  defp handle_owner_command(%Commands.ListLiveAttachmentDigests{request: request}, _from, state),
    do: reply(Services.list_live_attachment_digests(state.context, request), state)

  defp handle_owner_command(%Commands.CleanupExpiredPendingBlobs{request: request}, _from, state),
    do: reply(Services.cleanup_expired_pending_blobs(state.context, request), state)

  defp handle_owner_command(%Commands.CreateView{request: request}, _from, state),
    do: reply(Services.create_view(state.context, request), state)

  defp handle_owner_command(%Commands.DeleteView{view_id: view_id}, _from, state),
    do: reply(Services.delete_view(state.context, view_id), state)

  defp handle_owner_command(%Commands.ApplyViewBatch{request: request}, _from, state),
    do: reply(Services.apply_view_batch(state.context, request), state)

  defp handle_owner_command(%Commands.BeginViewRebuild{request: request}, _from, state),
    do: reply(Services.begin_view_rebuild(state.context, request), state)

  defp handle_owner_command(%Commands.AppendViewRebuildPage{request: request}, _from, state),
    do: reply(Services.append_view_rebuild_page(state.context, request), state)

  defp handle_owner_command(%Commands.FinishViewRebuild{request: request}, _from, state),
    do: reply(Services.finish_view_rebuild(state.context, request), state)

  defp handle_owner_command(%Commands.SetDerivedEnabled{request: request}, _from, state),
    do: set_derived_enabled(request, state)

  defp handle_owner_command(%Commands.SetDerivedSourceError{request: request}, _from, state),
    do: reply(Services.set_derived_source_error(state.context, request), state)

  defp handle_owner_command(%Commands.ApplyDerivedSourceBatch{request: request}, _from, state),
    do: mutate(Services.apply_derived_source_batch(state.context, request), state)

  defp handle_owner_command(%Commands.BeginDerivedSourceRebuild{request: request}, _from, state),
    do: reply(Services.begin_derived_source_rebuild(state.context, request), state)

  defp handle_owner_command(%Commands.ApplyDerivedRebuildPage{request: request}, _from, state),
    do: mutate(Services.apply_derived_rebuild_page(state.context, request), state)

  defp handle_owner_command(
         %Commands.PruneDerivedRebuildStalePage{request: request},
         _from,
         state
       ),
       do: mutate(Services.prune_derived_rebuild_stale_page(state.context, request), state)

  defp handle_owner_command(%Commands.FinishDerivedSourceRebuild{request: request}, _from, state),
    do: reply(Services.finish_derived_source_rebuild(state.context, request), state)

  defp handle_owner_command(%Commands.Close{}, _from, state),
    do: {:stop, :shutdown, :ok, state}

  defp handle_owner_command(_unknown, _from, state),
    do: {:reply, {:error, Error.invalid_request("unknown database command")}, state}

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
    module = Application.fetch_env!(:vial_keeper, :attachment_gc_module)

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
    Mutation.phase(:change_notifier, fn -> ChangeNotifier.publish(state.uuid, sequence) end)
    {:reply, {:ok, value}, state}
  end

  defp mutate({:ok, %{last_sequence: sequence} = value}, state)
       when is_integer(sequence) and sequence > 0 do
    Mutation.phase(:change_notifier, fn -> ChangeNotifier.publish(state.uuid, sequence) end)
    {:reply, {:ok, value}, state}
  end

  defp mutate({:ok, values} = result, state) when is_list(values) do
    sequence =
      Enum.reduce(values, 0, fn
        %{sequence: value}, maximum when is_integer(value) -> max(maximum, value)
        _value, maximum -> maximum
      end)

    if sequence > 0,
      do: Mutation.phase(:change_notifier, fn -> ChangeNotifier.publish(state.uuid, sequence) end)

    {:reply, result, state}
  end

  defp mutate({:ok, value}, state), do: {:reply, {:ok, value}, state}
  defp mutate({:error, _} = result, state), do: {:reply, result, state}
  defp reply(result, state), do: {:reply, result, state}

  defp wrap_put({:ok, map}) when is_map(map),
    do: {:ok, Results.put_document(map)}

  defp wrap_put(other), do: other

  defp writable_mutation(state, fun) do
    case database_kind(state) do
      :derived ->
        {:reply,
         {:error,
          Error.derived_database_read_only(
            "derived databases accept writes only from their materializer"
          )}, state}

      _ ->
        fun.()
    end
  end

  defp record_owner_queue(command, queued_at) do
    case mutation_operation(command) do
      operation when operation in [:put, :delete, :resolve, :bulk_write, :import] ->
        Mutation.record(operation, :owner_queue, max(System.monotonic_time() - queued_at, 0))

      _other ->
        :ok
    end
  end

  defp mutation_operation({:command_context, %CommandContext{}, command}),
    do: mutation_operation(command)

  defp mutation_operation(command) do
    case Commands.normalize(command) do
      %Commands.PutDocument{} -> :put
      %Commands.DeleteDocument{} -> :delete
      %Commands.ResolveConflict{} -> :resolve
      %Commands.BulkWrite{} -> :bulk_write
      %Commands.ImportRevisionChains{} -> :import
      _other -> nil
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
