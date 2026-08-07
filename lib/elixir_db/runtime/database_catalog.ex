defmodule ElixirDB.Runtime.DatabaseCatalog do
  @moduledoc "Registration catalog and lazy database runtime manager."
  use GenServer
  require Logger
  alias ElixirDB.MapAccess
  alias ElixirDB.Observability.Instrumentation.Database
  alias ElixirDB.Replication.JobManager

  alias ElixirDB.Runtime.{
    DatabaseAdmission,
    DatabaseOwner,
    DatabaseRuntimeSupervisor,
    PathSafety,
    RegistrationManifest
  }

  alias ElixirDB.Runtime.ChangeNotifier
  alias ElixirDB.Runtime.DatabaseAdmission
  alias ElixirDB.Storage.SQLite.Adapter
  def start_link(_args), do: GenServer.start_link(__MODULE__, [], name: __MODULE__)

  def create(relative_path, options \\ %{}),
    do: GenServer.call(__MODULE__, {:create, relative_path, options})

  def register(relative_path), do: GenServer.call(__MODULE__, {:register, relative_path})
  def unregister(uuid), do: GenServer.call(__MODULE__, {:unregister, uuid})
  def list, do: GenServer.call(__MODULE__, :list)
  def info(uuid), do: GenServer.call(__MODULE__, {:info, uuid})
  def close(uuid), do: GenServer.call(__MODULE__, {:close, uuid}, 30_000)

  # Plan §5.1: the database.open span is created HERE, in the caller process,
  # before the GenServer.call — so the span is a child of the caller's trace
  # (e.g. the HTTP request that triggered the open) and covers call latency.
  def open(uuid) do
    Database.open(uuid, fn ->
      GenServer.call(__MODULE__, {:open, uuid}, 30_000)
    end)
  end

  def command(uuid, command, timeout \\ 30_000),
    do: GenServer.call(__MODULE__, {:command, uuid, command}, timeout)

  @impl true
  def init(_) do
    root = ElixirDB.Config.database_root()
    :ok = File.mkdir_p(root)
    state = %{root: root, entries: load_entries()}
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
        with {:ok, absolute} <- safe_path(state.root, relative_path),
             false <- File.exists?(absolute),
             {:ok, config} <-
               ElixirDB.Config.merge_and_bound(
                 MapAccess.get(options, :config, ElixirDB.Config.defaults())
               ),
             {:ok, adapter} <- Adapter.create(absolute, Map.put(options, :config, config)),
             {:ok, identity} <- Adapter.identity(adapter),
             :ok <- Adapter.close(adapter),
             {:ok, state} <- put_entry(state, identity.database_uuid, relative_path) do
          {:reply, {:ok, identity}, state}
        else
          true ->
            {:reply, {:error, ElixirDB.Error.invalid_request("database file already exists")},
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
        with {:ok, absolute} <- safe_path(state.root, relative_path),
             true <- File.regular?(absolute),
             :ok <- ensure_path_not_open(state, absolute),
             {:ok, adapter} <- Adapter.open(absolute),
             {:ok, identity} <- Adapter.identity(adapter),
             :ok <- Adapter.close(adapter),
             :ok <- no_duplicate_uuid(state, identity.database_uuid, absolute),
             {:ok, next} <- put_entry(state, identity.database_uuid, relative_path) do
          {:reply, {:ok, identity}, next}
        else
          false ->
            {:reply,
             {:error, ElixirDB.Error.database_unavailable("registered database file is missing")},
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
  def handle_call({:info, uuid}, _from, state) do
    case Map.get(state.entries, uuid) do
      nil ->
        {:reply, {:error, ElixirDB.Error.database_not_registered("database is not registered")},
         state}

      entry ->
        case Registry.lookup(ElixirDB.Runtime.DatabaseRegistry, {:owner, uuid}) do
          [{_pid, _}] ->
            {:reply, DatabaseOwner.command(uuid, {:command, :identity, %{}}), state}

          [] ->
            inspect_unregistered_entry(state, uuid, entry)
        end
    end
  end

  @impl true
  def handle_call({:open, uuid}, _from, state) do
    # The database.open span wraps this call in the CALLER process (see open/1),
    # per Plan §5.1. Rejected opens (unavailable/in_use) become
    # outcome: :rejected there but keep span status UNSET (expected outcomes).
    case open_runtime(state, uuid) do
      {:ok, info, new_state} -> {:reply, {:ok, info}, new_state}
      {:error, error, new_state} -> {:reply, {:error, error}, new_state}
    end
  end

  @impl true
  def handle_call({:close, uuid}, _from, state) do
    result =
      case Registry.lookup(ElixirDB.Runtime.DatabaseRegistry, {:owner, uuid}) do
        [{_pid, _}] ->
          close_runtime(uuid)

        [] ->
          :ok
      end

    {:reply, result, state}
  end

  @impl true
  def handle_call({:command, uuid, command}, _from, state) do
    # Plan §5.2: wrap the central command dispatch in the database.command span.
    # command.type is derived from the command struct; expected domain errors
    # keep span status UNSET, only :internal_error sets ERROR (policy §6.5).
    #
    # SAFETY: the catalog is a single shared GenServer serving every database. An
    # unanticipated raise/throw from the adapter or admission layer would otherwise crash
    # this process and propagate a GenServer.call exit to *every* concurrent caller. Catch
    # any exception here and convert it to a typed internal_error so the catalog survives.
    case open_runtime(state, uuid) do
      {:ok, _info, state} ->
        run_command(uuid, command)
        |> command_reply(state)

      {:error, error, state} ->
        {:error, error}
        |> command_reply(state)
    end
  catch
    kind, reason ->
      Logger.error("database command raised",
        database_uuid: inspect(uuid),
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

  defp command_reply(result, state), do: {:reply, result, state}

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

  defp run_command(uuid, command) do
    Database.command(uuid, command, fn ->
      DatabaseAdmission.with_token(uuid, fn -> DatabaseOwner.command(uuid, command) end)
    end)
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

      %{absolute_path: _path} = entry ->
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

  defp start_registered_entry(state, uuid, %{absolute_path: path} = entry) do
    cond do
      not File.regular?(path) ->
        error = ElixirDB.Error.database_unavailable("registered database file is missing")
        {:error, error, mark_status(state, uuid, :unavailable)}

      open_count() >= (ElixirDB.Config.host_limits()[:max_open_databases] || 64) ->
        {:error, ElixirDB.Error.resource_limit("maximum open database count reached", %{}), state}

      true ->
        start_database_runtime(state, uuid, entry, path)
    end
  end

  defp start_database_runtime(state, uuid, entry, path) do
    case DynamicSupervisor.start_child(
           ElixirDB.Runtime.DatabaseSupervisor,
           {DatabaseRuntimeSupervisor, %{uuid: uuid, path: path}}
         ) do
      {:ok, _pid} ->
        {:ok, %{database_uuid: uuid, runtime_state: :open}, mark_status(state, uuid, :registered)}

      {:error, reason} ->
        error = open_failure_error(reason, entry)
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

  defp put_entry(state, uuid, relative) do
    entry = %{
      uuid: uuid,
      path: relative,
      absolute_path: Path.join(state.root, relative),
      status: :registered
    }

    next = %{state | entries: Map.put(state.entries, uuid, entry)}

    case RegistrationManifest.write(Map.values(next.entries)) do
      :ok -> {:ok, next}
      {:error, error} -> {:error, error}
    end
  end

  defp no_duplicate_uuid(state, uuid, absolute_path) do
    case Map.get(state.entries, uuid) do
      nil -> :ok
      %{absolute_path: ^absolute_path} -> :ok
      _ -> {:error, ElixirDB.Error.duplicate_database_uuid("database UUID is already registered")}
    end
  end

  defp ensure_path_not_open(state, absolute_path) do
    case Enum.find(state.entries, fn {_uuid, entry} -> entry.absolute_path == absolute_path end) do
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
            File.exists?(entry.absolute_path) -> :registered
            true -> :unavailable
          end
      end

    %{database_uuid: entry.uuid, path: entry.path, state: state}
  end

  defp inspect_entry(entry) do
    if File.exists?(entry.absolute_path) do
      case Adapter.open(entry.absolute_path) do
        {:ok, adapter} ->
          inspect_open_adapter(adapter, entry)

        {:error, error} ->
          {:error, error}
      end
    else
      {:error, ElixirDB.Error.database_unavailable("registered database file is missing")}
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
    with false <- JobManager.active?(uuid),
         :ok <-
           (case DatabaseAdmission.active_count(uuid) do
              {:ok, 0} ->
                :ok

              {:ok, count} ->
                {:error,
                 ElixirDB.Error.database_not_closable("database has admitted operations", %{
                   count: count
                 })}

              {:error, error} ->
                error
            end),
         :ok <- ChangeNotifier.close(uuid),
         runtime when not is_nil(runtime) <- runtime_pid(uuid) do
      if Process.alive?(runtime),
        do: Supervisor.stop(runtime, :shutdown, ElixirDB.Config.shutdown_timeout()),
        else: :ok
    else
      true ->
        {:error, ElixirDB.Error.database_not_closable("database has active replication jobs")}

      nil ->
        :ok

      {:error, _} = error ->
        error
    end
  end

  defp open_count do
    Registry.select(ElixirDB.Runtime.DatabaseRegistry, [
      {{{:owner, :"$1"}, :"$2", :"$3"}, [], [true]}
    ])
    |> length()
  end
end
