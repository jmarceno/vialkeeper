defmodule VialKeeper.Runtime.DatabaseOwner do
  @moduledoc "Serializes database commands through one owner process."
  use GenServer
  require Logger
  require VialKeeper.Probe
  alias VialKeeper.Commands
  alias VialKeeper.DatabaseBundle
  alias VialKeeper.Deadline
  alias VialKeeper.DerivedView.Manager, as: DerivedViewManager
  alias VialKeeper.Error
  alias VialKeeper.MapAccess
  alias VialKeeper.Observability.Instrumentation.Compact
  alias VialKeeper.Observability.Instrumentation.Mutation
  alias VialKeeper.Probe

  alias VialKeeper.Runtime.{
    AttachmentCoordinator,
    ChangeNotifier,
    CommandContext,
    CommandIO,
    DatabaseCommandPolicy,
    DatabaseReadDispatch,
    MutationCommands,
    RetentionScheduler,
    SequenceLedger,
    ShadowBinding,
    WriterPool
  }

  alias VialKeeper.Storage.Registry, as: StorageRegistry
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

  @doc """
  Returns the owner's opaque backend context so a writer slot can open its own
  read-write connection with `VialKeeper.Storage.Lifecycle.open_writer/1`.
  Callers must not use the owner's connection.
  """
  @spec writer_source(binary(), timeout()) ::
          {:ok, VialKeeper.Storage.BackendContext.t()} | {:error, Error.t()}
  def writer_source(uuid, timeout \\ VialKeeper.Config.shutdown_timeout())
      when is_binary(uuid),
      do: reader_source(uuid, timeout)

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

        start_ledger(backend, adapter, uuid, bundle, context)

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

  defp start_ledger(backend, adapter, uuid, bundle, context) do
    case SequenceLedger.initialize(uuid, context) do
      :ok ->
        {:ok,
         %{
           uuid: uuid,
           bundle: bundle,
           context: context,
           cache_epoch: WriterPool.cache_epoch(uuid)
         }}

      {:error, %Error{} = error} ->
        _ = backend.close(adapter)
        {:stop, error}
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
    Probe.measure :owner_command do
      safe_dispatch(context, command, from, state)
    end
  end

  def handle_call(command, from, state) do
    Probe.measure :owner_command do
      safe_dispatch(CommandContext.public(), command, from, state)
    end
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
    state = refresh_writer_caches(state)

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
    cond do
      Map.get(CommandIO.classes(), module) == :read ->
        reply(DatabaseReadDispatch.run(state.context, command), state)

      MutationCommands.handles?(command) ->
        mutation(command, state)

      true ->
        handle_owner_command(command, from, state)
    end
  end

  defp mutation(command, state) do
    deadline = Deadline.from_timeout(VialKeeper.Config.request_timeout_ms())
    outcome = MutationCommands.execute(command, state.context, state.uuid, deadline)
    {:reply, MutationCommands.finish(outcome, command, state.uuid, deadline), state}
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

  defp handle_owner_command(%Commands.BeginDerivedSourceRebuild{request: request}, _from, state),
    do: reply(Services.begin_derived_source_rebuild(state.context, request), state)

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

  defp reply(result, state), do: {:reply, result, state}

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
      %_{} = normalized -> MutationCommands.operation(normalized)
      _other -> nil
    end
  end

  # A barrier (a serial or exclusive command run while writer slots exist)
  # bumps the writer pool's cache epoch; every writer then drops its caches.
  defp refresh_writer_caches(state) do
    epoch = WriterPool.refresh_writer_caches(state.uuid, state.context, state.cache_epoch)
    %{state | cache_epoch: epoch}
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
