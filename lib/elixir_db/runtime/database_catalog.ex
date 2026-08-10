defmodule ElixirDB.Runtime.DatabaseCatalog do
  @moduledoc "Registration catalog and lazy database runtime manager."
  use GenServer
  require Logger
  alias ElixirDB.MapAccess
  alias ElixirDB.Observability.Instrumentation.Database
  alias ElixirDB.Query.SubscriptionHub
  alias ElixirDB.Replication.JobManager

  alias ElixirDB.Runtime.{
    AttachmentCoordinator,
    ChangeNotifier,
    DatabaseAdmission,
    DatabaseRuntimeSupervisor,
    Deadline,
    RegistrationManifest
  }

  alias ElixirDB.DatabaseBundle
  alias ElixirDB.PathSafety
  alias ElixirDB.Storage.SQLite.Adapter
  def start_link(_args), do: GenServer.start_link(__MODULE__, [], name: __MODULE__)

  def create(relative_path, options \\ %{}),
    do: GenServer.call(__MODULE__, {:create, relative_path, options})

  def register(relative_path), do: GenServer.call(__MODULE__, {:register, relative_path})
  def unregister(uuid), do: GenServer.call(__MODULE__, {:unregister, uuid})
  def list, do: GenServer.call(__MODULE__, :list)

  def info(uuid) do
    case GenServer.call(__MODULE__, {:info, uuid}) do
      {:route, ^uuid} -> command(uuid, {:command, :identity, %{}}, 30_000)
      result -> result
    end
  end

  def close(uuid), do: GenServer.call(__MODULE__, {:close, uuid}, 30_000)

  @doc "Returns the absolute bundle root for a registered database UUID."
  def bundle_root(uuid), do: GenServer.call(__MODULE__, {:bundle_root, uuid})

  # The database.open span is created here, in the caller process, before the
  # GenServer.call — so the span is a child of the caller's trace (e.g. the
  # HTTP request that triggered the open) and covers call latency.
  def open(uuid) do
    Database.open(uuid, fn ->
      GenServer.call(__MODULE__, {:open, uuid}, 30_000)
    end)
  end

  @doc """
  Ensures a database is registered and its runtime is open for command routing.

  This is a short host-global catalog call. Queue wait and owner execution happen
  outside the catalog GenServer via per-database admission.
  """
  @spec ensure_command_target(binary(), Deadline.t()) :: :ok | {:error, ElixirDB.Error.t()}
  def ensure_command_target(uuid) when is_binary(uuid) do
    ensure_command_target(uuid, Deadline.from_timeout(30_000))
  end

  def ensure_command_target(uuid, :infinity) when is_binary(uuid) do
    GenServer.call(__MODULE__, {:ensure_command_target, uuid}, :infinity)
  end

  def ensure_command_target(uuid, deadline_ms) when is_binary(uuid) do
    if Deadline.exhausted?(deadline_ms) do
      {:error, command_deadline_error()}
    else
      GenServer.call(
        __MODULE__,
        {:ensure_command_target, uuid},
        Deadline.call_timeout(deadline_ms)
      )
    end
  catch
    :exit, :timeout ->
      {:error, command_deadline_error()}

    :exit, {:timeout, {GenServer, :call, _}} ->
      {:error, command_deadline_error()}
  end

  @spec command(binary(), term(), timeout()) :: term() | {:error, ElixirDB.Error.t()}
  def command(uuid, command, timeout \\ 30_000)

  def command(uuid, command, :infinity) when is_binary(uuid) do
    command_as(uuid, :foreground, command, :infinity)
  end

  def command(uuid, command, timeout) when is_binary(uuid) do
    command_as(uuid, :foreground, command, timeout)
  end

  @doc "Trusted internal command routing with an explicit admission service class."
  @spec command_as(binary(), DatabaseAdmission.service_class(), term(), timeout()) ::
          term() | {:error, ElixirDB.Error.t()}
  def command_as(uuid, class, command, timeout \\ 30_000)

  def command_as(uuid, class, command, :infinity) when is_binary(uuid) do
    with :ok <- ensure_command_target(uuid, :infinity) do
      Database.command(uuid, command, fn ->
        DatabaseAdmission.execute_with_deadline(uuid, class, command, :infinity)
      end)
    end
  end

  def command_as(uuid, class, command, timeout)
      when is_binary(uuid) and is_integer(timeout) and timeout >= 0 do
    deadline_ms = Deadline.from_timeout(timeout)

    with :ok <- ensure_command_target(uuid, deadline_ms) do
      Database.command(uuid, command, fn ->
        DatabaseAdmission.execute_with_deadline(uuid, class, command, deadline_ms)
      end)
    end
  end

  @impl true
  def init(_) do
    root = ElixirDB.Config.database_root()
    :ok = File.mkdir_p(root)
    state = %{root: root, entries: load_entries(), close_operations: %{}}
    Process.send_after(self(), :resume_registered_jobs, 0)
    {:ok, state}
  end

  # The registration manifest is routing-only, reconstructible state. If it cannot
  # be read cleanly — corrupt JSON, an invalid entry, an unsupported version, or
  # duplicate uuid/path — the catalog degrades to an empty registration set rather
  # than crashing. A poisoned manifest must never take the server down; the fault
  # is surfaced loudly for operators instead.
  defp load_entries do
    case RegistrationManifest.read() do
      {:ok, entries} ->
        Map.new(entries, &{&1.uuid, &1})

      {:error, %ElixirDB.Error{code: code}} ->
        Logger.warning("registration manifest unreadable (#{code}); starting with an empty catalog")

        %{}
    end
  end

  @impl true
  def handle_call({:create, relative_path, options}, _from, state) do
    # SAFETY: create touches the filesystem and adapter DDL; an unanticipated raise here
    # would crash the shared catalog. Catch and convert to a typed error.
    safe(
      fn ->
        with {:ok, bundle_root} <- safe_path(state.root, relative_path),
             false <- File.exists?(bundle_root),
             {:ok, bundle} <- DatabaseBundle.create(bundle_root),
             {:ok, config} <-
               ElixirDB.Config.merge_and_bound(
                 MapAccess.get(options, :config, ElixirDB.Config.defaults())
               ),
             {:ok, adapter} <-
               Adapter.create(DatabaseBundle.sqlite_path(bundle), Map.put(options, :config, config)),
             {:ok, identity} <- Adapter.identity(adapter),
             :ok <- Adapter.close(adapter),
             {:ok, state} <- put_entry(state, identity.database_uuid, relative_path, bundle) do
          {:reply, {:ok, identity}, state}
        else
          true ->
            {:reply, {:error, ElixirDB.Error.invalid_request("database bundle already exists")},
             state}

          {:error, %ElixirDB.Error{} = error} ->
            {:reply, {:error, error}, state}

          {:error, reason} ->
            {:reply,
             {:error,
              ElixirDB.Error.internal_error("database creation failed", %{cause: inspect(reason)})},
             state}
        end
      end,
      state
    )
  end

  @impl true
  def handle_call({:register, relative_path}, _from, state) do
    # SAFETY: register opens the database file via the adapter; an unanticipated raise
    # would crash the shared catalog. Catch and convert to a typed error.
    safe(
      fn ->
        with {:ok, bundle_root} <- safe_path(state.root, relative_path),
             true <- File.dir?(bundle_root),
             :ok <- ensure_path_not_open(state, bundle_root),
             {:ok, bundle} <- DatabaseBundle.validate(bundle_root),
             {:ok, adapter} <- Adapter.open(DatabaseBundle.sqlite_path(bundle)),
             {:ok, identity} <- Adapter.identity(adapter),
             :ok <- Adapter.close(adapter),
             :ok <- no_duplicate_uuid(state, identity.database_uuid, bundle_root),
             {:ok, next} <- put_entry(state, identity.database_uuid, relative_path, bundle) do
          {:reply, {:ok, identity}, next}
        else
          false ->
            {:reply,
             {:error, ElixirDB.Error.database_unavailable("registered database bundle is missing")},
             state}

          {:error, %ElixirDB.Error{} = error} ->
            {:reply, {:error, error}, state}

          {:error, reason} ->
            {:reply,
             {:error,
              ElixirDB.Error.database_unavailable("database could not be registered", %{
                cause: inspect(reason)
              })}, state}
        end
      end,
      state
    )
  end

  @impl true
  def handle_call({:unregister, uuid}, _from, state) do
    case Map.get(state.entries, uuid) do
      nil ->
        {:reply, {:error, ElixirDB.Error.database_not_registered("database is not registered")},
         state}

      _entry ->
        case Registry.lookup(ElixirDB.Runtime.DatabaseRegistry, {:owner, uuid}) do
          [] ->
            next = %{state | entries: Map.delete(state.entries, uuid)}
            unregister_entry(next, state)

          _ ->
            {:reply,
             {:error,
              ElixirDB.Error.database_not_closable("database must be closed before unregistering")},
             state}
        end
    end
  end

  @impl true
  def handle_call(:list, _from, state),
    do: {:reply, {:ok, Enum.map(Map.values(state.entries), &entry_status/1)}, state}

  @impl true
  def handle_call({:bundle_root, uuid}, _from, state) do
    case Map.get(state.entries, uuid) do
      %{bundle_root: bundle_root} when is_binary(bundle_root) ->
        {:reply, {:ok, bundle_root}, state}

      nil ->
        {:reply, {:error, ElixirDB.Error.database_not_registered("database is not registered")},
         state}

      _entry ->
        {:reply, {:error, ElixirDB.Error.database_unavailable("database bundle root is missing")},
         state}
    end
  end

  @impl true
  def handle_call({:info, uuid}, _from, state) do
    case Map.get(state.entries, uuid) do
      nil ->
        {:reply, {:error, ElixirDB.Error.database_not_registered("database is not registered")},
         state}

      entry ->
        case Registry.lookup(ElixirDB.Runtime.DatabaseRegistry, {:owner, uuid}) do
          [{_pid, _}] ->
            {:reply, {:route, uuid}, state}

          [] ->
            inspect_unregistered_entry(state, uuid, entry)
        end
    end
  end

  @impl true
  def handle_call({:open, uuid}, _from, state) do
    # The database.open span wraps this call in the caller process (see open/1).
    # Rejected opens (unavailable/in_use) become outcome: :rejected there but
    # keep span status UNSET (expected outcomes).
    if Map.has_key?(state.close_operations, uuid) do
      {:reply, {:error, ElixirDB.Error.database_closed("database is closing")}, state}
    else
      case open_runtime(state, uuid) do
        {:ok, info, new_state} -> {:reply, {:ok, info}, new_state}
        {:error, error, new_state} -> {:reply, {:error, error}, new_state}
      end
    end
  end

  @impl true
  def handle_call({:close, uuid}, from, state) do
    case Map.get(state.close_operations, uuid) do
      waiters when is_list(waiters) ->
        {:noreply,
         %{state | close_operations: Map.put(state.close_operations, uuid, [from | waiters])}}

      nil ->
        catalog = self()

        case start_close_operation(uuid, catalog) do
          {:ok, _pid} ->
            {:noreply, %{state | close_operations: Map.put(state.close_operations, uuid, [from])}}

          {:error, reason} ->
            {:reply,
             {:error,
              ElixirDB.Error.internal_error("database close could not be scheduled", %{
                cause: inspect(reason)
              })}, state}
        end
    end
  end

  @impl true
  def handle_call({:ensure_command_target, uuid}, _from, state) do
    safe(fn -> reply_ensure_command_target(uuid, state) end, state)
  end

  defp reply_ensure_command_target(uuid, state) do
    if Map.has_key?(state.close_operations, uuid) do
      {:reply, {:error, ElixirDB.Error.database_closed("database is closing")}, state}
    else
      case open_runtime(state, uuid) do
        {:ok, _info, new_state} -> {:reply, :ok, new_state}
        {:error, error, new_state} -> {:reply, {:error, error}, new_state}
      end
    end
  end

  defp unregister_entry(next, state) do
    case RegistrationManifest.write(Map.values(next.entries)) do
      :ok -> {:reply, :ok, next}
      {:error, error} -> {:reply, {:error, error}, state}
    end
  end

  defp inspect_unregistered_entry(state, uuid, entry) do
    case inspect_entry(entry) do
      {:ok, _} = ok ->
        {:reply, ok, mark_status(state, uuid, :registered)}

      {:error, %ElixirDB.Error{} = error} = err ->
        {:reply, err, maybe_mark_unavailable(state, uuid, error)}
    end
  end

  defp command_deadline_error do
    ElixirDB.Error.internal_error("database command timed out", %{
      reason: :deadline_exhausted
    })
  end

  # SAFETY: final catch-all net for handle_call clauses that touch the adapter. An
  # unanticipated raise/throw is converted to a typed internal_error reply so the shared
  # catalog GenServer never crashes from a poisoned input. `state` is returned unchanged
  # because the operation failed before (or outside of) any state mutation.
  defp safe(fun, state) do
    fun.()
  catch
    kind, reason ->
      Logger.error("catalog operation raised",
        kind: kind,
        reason: inspect(reason)
      )

      {:reply,
       {:error,
        ElixirDB.Error.internal_error("database operation failed", %{
          cause: inspect(reason),
          kind: kind
        })}, state}
  end

  @impl true
  def handle_info({:close_finished, uuid, result}, state) do
    case Map.pop(state.close_operations, uuid) do
      {nil, _operations} ->
        {:noreply, state}

      {waiters, operations} ->
        Enum.each(waiters, &GenServer.reply(&1, result))
        {:noreply, %{state | close_operations: operations}}
    end
  end

  @impl true
  def handle_info(:resume_registered_jobs, state) do
    uuids = Map.keys(state.entries)

    Task.Supervisor.start_child(ElixirDB.TaskSupervisor, fn ->
      Enum.each(uuids, &resume_registered_jobs/1)
    end)

    {:noreply, state}
  end

  defp open_runtime(state, uuid) do
    case Map.get(state.entries, uuid) do
      nil ->
        {:error, ElixirDB.Error.database_not_registered("database is not registered"), state}

      %{bundle_root: _root} = entry ->
        open_registered_entry(state, uuid, entry)
    end
  end

  defp open_registered_entry(state, uuid, entry) do
    case Registry.lookup(ElixirDB.Runtime.DatabaseRegistry, {:owner, uuid}) do
      [{_pid, _}] ->
        {:ok, %{database_uuid: uuid, runtime_state: :open}, mark_status(state, uuid, :registered)}

      [] ->
        start_registered_entry(state, uuid, entry)
    end
  end

  defp start_registered_entry(state, uuid, %{bundle_root: bundle_root} = entry) do
    cond do
      not File.dir?(bundle_root) ->
        error = ElixirDB.Error.database_unavailable("registered database bundle is missing")
        {:error, error, mark_status(state, uuid, :unavailable)}

      open_count() >= (ElixirDB.Config.host_limits()[:max_open_databases] || 64) ->
        {:error, ElixirDB.Error.resource_limit("maximum open database count reached", %{}), state}

      true ->
        start_database_runtime(state, uuid, entry, bundle_root)
    end
  end

  defp start_database_runtime(state, uuid, entry, bundle_root) do
    case DatabaseBundle.prepare_for_open(bundle_root) do
      {:ok, bundle} ->
        case DynamicSupervisor.start_child(
               ElixirDB.Runtime.DatabaseSupervisor,
               {DatabaseRuntimeSupervisor, %{uuid: uuid, bundle: bundle}}
             ) do
          {:ok, _pid} ->
            {:ok, %{database_uuid: uuid, runtime_state: :open},
             mark_status(state, uuid, :registered)}

          {:error, reason} ->
            error = open_failure_error(reason, entry)
            {:error, error, maybe_mark_unavailable(state, uuid, error)}
        end

      {:error, error} ->
        {:error, error, maybe_mark_unavailable(state, uuid, error)}
    end
  end

  # LIFE-007: missing file or UUID mismatch MUST mark registration unavailable.
  defp maybe_mark_unavailable(state, uuid, %ElixirDB.Error{code: :database_unavailable} = error) do
    if uuid_mismatch?(error) or missing_file?(error),
      do: mark_status(state, uuid, :unavailable),
      else: state
  end

  defp maybe_mark_unavailable(state, _uuid, _error), do: state

  defp mark_status(state, uuid, status) do
    case Map.get(state.entries, uuid) do
      nil ->
        state

      entry ->
        %{state | entries: Map.put(state.entries, uuid, Map.put(entry, :status, status))}
    end
  end

  defp open_failure_error(reason, _entry) do
    case extract_error(reason) do
      %ElixirDB.Error{} = error ->
        error

      _ ->
        ElixirDB.Error.database_unavailable("database could not be opened", %{
          cause: inspect(reason)
        })
    end
  end

  defp extract_error(%ElixirDB.Error{} = error), do: error
  defp extract_error({:shutdown, reason}), do: extract_error(reason)
  defp extract_error({:failed_to_start_child, _id, reason}), do: extract_error(reason)
  defp extract_error(_), do: nil

  defp uuid_mismatch?(%ElixirDB.Error{details: %{reason: :uuid_mismatch}}), do: true
  defp uuid_mismatch?(_), do: false

  defp missing_file?(%ElixirDB.Error{message: message}) when is_binary(message),
    do: String.contains?(message, "missing")

  defp missing_file?(_), do: false

  defp runtime_pid(uuid) do
    case Registry.lookup(ElixirDB.Runtime.DatabaseRegistry, {:runtime, uuid}) do
      [{pid, _}] -> pid
      [] -> nil
    end
  end

  defp put_entry(state, uuid, relative, %DatabaseBundle{} = bundle) do
    entry = %{
      uuid: uuid,
      path: relative,
      bundle_root: DatabaseBundle.root(bundle),
      sqlite_path: DatabaseBundle.sqlite_path(bundle),
      status: :registered
    }

    next = %{state | entries: Map.put(state.entries, uuid, entry)}

    case RegistrationManifest.write(Map.values(next.entries)) do
      :ok -> {:ok, next}
      {:error, error} -> {:error, error}
    end
  end

  defp no_duplicate_uuid(state, uuid, bundle_root) do
    case Map.get(state.entries, uuid) do
      nil -> :ok
      %{bundle_root: ^bundle_root} -> :ok
      _ -> {:error, ElixirDB.Error.duplicate_database_uuid("database UUID is already registered")}
    end
  end

  defp ensure_path_not_open(state, bundle_root) do
    case Enum.find(state.entries, fn {_uuid, entry} -> entry.bundle_root == bundle_root end) do
      {_uuid, %{uuid: uuid}} ->
        case Registry.lookup(ElixirDB.Runtime.DatabaseRegistry, {:owner, uuid}) do
          [] ->
            :ok

          _ ->
            {:error, ElixirDB.Error.database_in_use("database must be closed before registration")}
        end

      nil ->
        :ok
    end
  end

  defp safe_path(root, relative) when is_binary(relative) do
    expanded = Path.expand(relative, root)
    root = Path.expand(root)
    relative_to_root = Path.relative_to(expanded, root)
    components = Path.split(relative)

    cond do
      Path.type(relative) == :absolute or relative == "" or
        Enum.any?(components, &(&1 in [".", ".."])) or
        String.contains?(relative, "\\") or relative_to_root == expanded or
          String.starts_with?(relative_to_root, "../") ->
        {:error, ElixirDB.Error.invalid_request("database path escapes the database root")}

      not PathSafety.no_symlink_components?(expanded) ->
        {:error, ElixirDB.Error.invalid_request("database path contains a symlink")}

      true ->
        {:ok, expanded}
    end
  end

  defp safe_path(_, _),
    do: {:error, ElixirDB.Error.invalid_request("database path must be relative")}

  defp entry_status(entry) do
    state =
      case Registry.lookup(ElixirDB.Runtime.DatabaseRegistry, {:owner, entry.uuid}) do
        [{_pid, _}] ->
          :open

        [] ->
          cond do
            Map.get(entry, :status) == :unavailable -> :unavailable
            File.dir?(entry.bundle_root) -> :registered
            true -> :unavailable
          end
      end

    %{database_uuid: entry.uuid, path: entry.path, state: state}
  end

  defp inspect_entry(entry) do
    if File.dir?(entry.bundle_root) do
      case Adapter.open(entry.sqlite_path) do
        {:ok, adapter} ->
          inspect_open_adapter(adapter, entry)

        {:error, error} ->
          {:error, error}
      end
    else
      {:error, ElixirDB.Error.database_unavailable("registered database bundle is missing")}
    end
  end

  defp inspect_open_adapter(adapter, entry) do
    result = Adapter.identity(adapter)
    _ = Adapter.close(adapter)

    case result do
      {:ok, %{database_uuid: uuid}} when uuid == entry.uuid ->
        result

      {:ok, %{database_uuid: actual}} ->
        {:error,
         ElixirDB.Error.database_unavailable("database UUID mismatch", %{
           reason: :uuid_mismatch,
           expected: entry.uuid,
           actual: actual
         })}

      other ->
        other
    end
  end

  defp resume_registered_jobs(uuid) do
    case open(uuid) do
      {:ok, _} ->
        :ok = JobManager.resume(uuid)

        if not JobManager.active?(uuid), do: _ = close(uuid)

      {:error, _} ->
        :ok
    end
  end

  defp close_runtime(uuid) do
    # Prefer checking external close blockers before begin_close. Active
    # replication remains a hard blocker; callers must disable jobs first.
    with false <- JobManager.active?(uuid),
         :ok <- drain_admission(uuid) do
      close_runtime_after_admission_drain(uuid)
    else
      true ->
        {:error, ElixirDB.Error.database_not_closable("database has active replication jobs")}

      {:error, _} = error ->
        error
    end
  end

  defp close_runtime_after_admission_drain(uuid) do
    try do
      with :ok <- close_external_services(uuid),
           runtime when not is_nil(runtime) <- runtime_pid(uuid) do
        if Process.alive?(runtime),
          do: Supervisor.stop(runtime, :shutdown, ElixirDB.Config.shutdown_timeout()),
          else: :ok
      else
        nil ->
          :ok

        {:error, _} = error ->
          error
      end
    catch
      :exit, reason ->
        {:error,
         ElixirDB.Error.internal_error("database runtime shutdown failed during close", %{
           cause: inspect(reason)
         })}
    end
    |> abort_close_on_error(uuid)
  end

  defp abort_close_on_error({:error, _} = error, uuid) do
    _ = DatabaseAdmission.cancel_close(uuid)
    error
  end

  defp abort_close_on_error(other, _uuid), do: other

  defp close_external_services(uuid) do
    with :ok <- close_attachment_coordinator(uuid),
         :ok <- close_subscription_hub(uuid) do
      close_change_notifier(uuid)
    end
  end

  defp close_attachment_coordinator(uuid) do
    case AttachmentCoordinator.begin_close(uuid) do
      :ok -> :ok
      {:error, %ElixirDB.Error{code: :database_closed}} -> :ok
      {:error, _} = error -> error
    end
  end

  defp close_subscription_hub(uuid) do
    case SubscriptionHub.close(uuid) do
      :ok -> :ok
      {:error, %ElixirDB.Error{code: :database_closed}} -> :ok
      {:error, _} = error -> error
    end
  end

  defp close_change_notifier(uuid) do
    case ChangeNotifier.close(uuid) do
      :ok -> :ok
      {:error, %ElixirDB.Error{code: :database_closed}} -> :ok
      {:error, _} = error -> error
    end
  end

  defp drain_admission(uuid) do
    with :ok <- DatabaseAdmission.begin_close(uuid) do
      await_admission_idle(uuid)
    end
  end

  defp await_admission_idle(uuid) do
    case DatabaseAdmission.await_idle(uuid, ElixirDB.Config.shutdown_timeout()) do
      :ok ->
        :ok

      other ->
        _ = DatabaseAdmission.cancel_close(uuid)
        other
    end
  catch
    :exit, reason ->
      _ = DatabaseAdmission.cancel_close(uuid)

      {:error,
       ElixirDB.Error.internal_error("database admission drain failed during close", %{
         cause: inspect(reason)
       })}
  end

  defp start_close_operation(uuid, catalog) do
    Task.Supervisor.start_child(ElixirDB.TaskSupervisor, fn ->
      send(catalog, {:close_finished, uuid, run_close_operation(uuid)})
    end)
  end

  defp run_close_operation(uuid) do
    case Registry.lookup(ElixirDB.Runtime.DatabaseRegistry, {:owner, uuid}) do
      [{_pid, _}] -> close_runtime(uuid)
      [] -> :ok
    end
  catch
    kind, reason ->
      _ = maybe_cancel_admission_close(uuid)

      {:error,
       ElixirDB.Error.internal_error("database close failed", %{
         kind: kind,
         cause: inspect(reason)
       })}
  end

  defp maybe_cancel_admission_close(uuid) do
    case DatabaseAdmission.closing?(uuid) do
      {:ok, true} -> DatabaseAdmission.cancel_close(uuid)
      _ -> :ok
    end
  end

  defp open_count do
    Registry.select(ElixirDB.Runtime.DatabaseRegistry, [
      {{{:owner, :"$1"}, :"$2", :"$3"}, [], [true]}
    ])
    |> length()
  end
end
