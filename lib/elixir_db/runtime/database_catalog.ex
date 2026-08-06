defmodule ElixirDB.Runtime.DatabaseCatalog do
  @moduledoc "Registration catalog and lazy database runtime manager."
  use GenServer
  alias ElixirDB.Storage.SQLite.Adapter

  alias ElixirDB.Runtime.{
    DatabaseAdmission,
    DatabaseOwner,
    DatabaseRuntimeSupervisor,
    RegistrationManifest
  }

  def start_link(_args), do: GenServer.start_link(__MODULE__, [], name: __MODULE__)

  def create(relative_path, options \\ %{}),
    do: GenServer.call(__MODULE__, {:create, relative_path, options})

  def register(relative_path), do: GenServer.call(__MODULE__, {:register, relative_path})
  def unregister(uuid), do: GenServer.call(__MODULE__, {:unregister, uuid})
  def list, do: GenServer.call(__MODULE__, :list)
  def info(uuid), do: GenServer.call(__MODULE__, {:info, uuid})
  def open(uuid), do: GenServer.call(__MODULE__, {:open, uuid}, 30_000)
  def close(uuid), do: GenServer.call(__MODULE__, {:close, uuid}, 30_000)

  def command(uuid, command, timeout \\ 30_000),
    do: GenServer.call(__MODULE__, {:command, uuid, command}, timeout)

  @impl true
  def init(_) do
    root = ElixirDB.Config.database_root()
    :ok = File.mkdir_p(root)
    {:ok, entries} = RegistrationManifest.read()
    state = %{root: root, entries: Map.new(entries, &{&1.uuid, &1})}
    Process.send_after(self(), :resume_registered_jobs, 0)
    {:ok, state}
  end

  @impl true
  def handle_call({:create, relative_path, options}, _from, state) do
    with {:ok, absolute} <- safe_path(state.root, relative_path),
         false <- File.exists?(absolute),
         {:ok, config} <-
           ElixirDB.Config.merge_and_bound(
             Map.get(options, :config, Map.get(options, "config", ElixirDB.Config.defaults()))
           ),
         {:ok, adapter} <- Adapter.create(absolute, Map.put(options, :config, config)),
         {:ok, identity} <- Adapter.identity(adapter),
         :ok <- Adapter.close(adapter),
         {:ok, state} <- put_entry(state, identity.database_uuid, relative_path) do
      {:reply, {:ok, identity}, state}
    else
      true ->
        {:reply, {:error, ElixirDB.Error.invalid_request("database file already exists")}, state}

      {:error, %ElixirDB.Error{} = error} ->
        {:reply, {:error, error}, state}

      {:error, reason} ->
        {:reply,
         {:error,
          ElixirDB.Error.internal_error("database creation failed", %{cause: inspect(reason)})},
         state}
    end
  end

  @impl true
  def handle_call({:register, relative_path}, _from, state) do
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

            case RegistrationManifest.write(Map.values(next.entries)) do
              :ok -> {:reply, :ok, next}
              {:error, error} -> {:reply, {:error, error}, state}
            end

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
          [{_pid, _}] -> {:reply, DatabaseOwner.command(uuid, {:command, :identity, %{}}), state}
          [] -> {:reply, inspect_entry(entry), state}
        end
    end
  end

  @impl true
  def handle_call({:open, uuid}, _from, state) do
    case open_runtime(state, uuid) do
      {:ok, info, state} -> {:reply, {:ok, info}, state}
      {:error, error, state} -> {:reply, {:error, error}, state}
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
    case open_runtime(state, uuid) do
      {:ok, _info, state} ->
        {:reply, DatabaseAdmission.with_token(uuid, fn -> DatabaseOwner.command(uuid, command) end),
         state}

      {:error, error, state} ->
        {:reply, {:error, error}, state}
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

      %{absolute_path: path} ->
        case Registry.lookup(ElixirDB.Runtime.DatabaseRegistry, {:owner, uuid}) do
          [{_pid, _}] ->
            {:ok, %{database_uuid: uuid, runtime_state: :open}, state}

          [] ->
            if open_count() >= (ElixirDB.Config.host_limits()[:max_open_databases] || 64) do
              {:error, ElixirDB.Error.resource_limit("maximum open database count reached", %{}),
               state}
            else
              case DynamicSupervisor.start_child(
                     ElixirDB.Runtime.DatabaseSupervisor,
                     {DatabaseRuntimeSupervisor, %{uuid: uuid, path: path}}
                   ) do
                {:ok, _pid} ->
                  {:ok, %{database_uuid: uuid, runtime_state: :open}, state}

                {:error, {:shutdown, %ElixirDB.Error{} = error}} ->
                  {:error, error, state}

                {:error, reason} ->
                  {:error,
                   ElixirDB.Error.database_unavailable("database could not be opened", %{
                     cause: inspect(reason)
                   }), state}
              end
            end
        end
    end
  end

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
    case Enum.find(Map.values(state.entries), &(&1.absolute_path == absolute_path)) do
      %{uuid: uuid} ->
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

      not no_symlink_components?(expanded) ->
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
        [{_pid, _}] -> :open
        [] -> if(File.exists?(entry.absolute_path), do: :registered, else: :unavailable)
      end

    %{database_uuid: entry.uuid, path: entry.path, state: state}
  end

  defp inspect_entry(entry) do
    if File.exists?(entry.absolute_path) do
      Adapter.open(entry.absolute_path) |> inspect_and_close()
    else
      {:error, ElixirDB.Error.database_unavailable("registered database file is missing")}
    end
  end

  defp inspect_and_close({:ok, adapter}) do
    result = Adapter.identity(adapter)
    _ = Adapter.close(adapter)
    result
  end

  defp inspect_and_close({:error, error}), do: {:error, error}

  defp no_symlink_components?(path) do
    path
    |> Path.split()
    |> Enum.reduce_while("", fn component, current ->
      next = Path.join(current, component)

      case File.lstat(next) do
        {:ok, %File.Stat{type: :symlink}} -> {:halt, false}
        {:ok, _} -> {:cont, next}
        {:error, :enoent} -> {:halt, true}
        {:error, _} -> {:halt, false}
      end
    end)
    |> case do
      false -> false
      _ -> true
    end
  end

  defp resume_registered_jobs(uuid) do
    case open(uuid) do
      {:ok, _} ->
        :ok = ElixirDB.Replication.JobManager.resume(uuid)

        if not ElixirDB.Replication.JobManager.active?(uuid), do: _ = close(uuid)

      {:error, _} ->
        :ok
    end
  end

  defp close_runtime(uuid) do
    with false <- ElixirDB.Replication.JobManager.active?(uuid),
         :ok <-
           (case ElixirDB.Runtime.DatabaseAdmission.active_count(uuid) do
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
         :ok <- ElixirDB.Runtime.ChangeNotifier.close(uuid),
         runtime when not is_nil(runtime) <- runtime_pid(uuid) do
      if Process.alive?(runtime), do: Supervisor.stop(runtime, :shutdown, 30_000), else: :ok
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
