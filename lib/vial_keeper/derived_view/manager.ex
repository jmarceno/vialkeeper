defmodule VialKeeper.DerivedView.Manager do
  @moduledoc "Coordinates the lifecycle and recovery of open derived materializer sessions."
  use GenServer
  require Logger

  alias VialKeeper.DerivedView.{Supervisor, Worker}
  alias VialKeeper.Runtime.DatabaseOwner

  @call_timeout 30_000

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(_opts \\ []), do: GenServer.start_link(__MODULE__, [], name: __MODULE__)

  @spec start(binary()) :: :ok | {:error, term()}
  def start(uuid) when is_binary(uuid), do: start(uuid, nil)

  @spec start(binary(), [binary()] | nil) :: :ok | {:error, term()}
  def start(uuid, source_uuids) when is_binary(uuid) do
    GenServer.call(__MODULE__, {:start, uuid, source_uuids}, @call_timeout)
  catch
    :exit, reason ->
      {:error,
       VialKeeper.Error.database_unavailable("derived materializer lifecycle is unavailable", %{
         cause: inspect(reason)
       })}
  end

  @spec close(binary()) :: :ok | {:error, term()}
  def close(uuid) when is_binary(uuid) do
    GenServer.call(__MODULE__, {:close, uuid}, @call_timeout)
  catch
    :exit, reason ->
      {:error,
       VialKeeper.Error.database_unavailable("derived materializer lifecycle is unavailable", %{
         cause: inspect(reason)
       })}
  end

  @spec ensure_closable(binary()) :: :ok | {:error, VialKeeper.Error.t()}
  def ensure_closable(uuid) when is_binary(uuid) do
    GenServer.call(__MODULE__, {:ensure_closable, uuid}, @call_timeout)
  catch
    :exit, reason ->
      {:error,
       VialKeeper.Error.database_unavailable("derived materializer lifecycle is unavailable", %{
         cause: inspect(reason)
       })}
  end

  @spec refresh(binary()) :: :ok
  def refresh(uuid) when is_binary(uuid) do
    GenServer.cast(__MODULE__, {:refresh, uuid})
    :ok
  catch
    :exit, _ -> :ok
  end

  @spec rebuild(binary()) :: :ok
  def rebuild(uuid) when is_binary(uuid) do
    GenServer.cast(__MODULE__, {:rebuild, uuid})
    :ok
  catch
    :exit, _ -> :ok
  end

  @spec worker_pid(binary()) :: {:ok, pid()} | :error
  def worker_pid(uuid) when is_binary(uuid), do: Worker.pid(uuid)

  @impl true
  def init(_args) do
    send(self(), :reconcile)
    {:ok, %{sessions: %{}, desired: MapSet.new(), dependencies: %{}, enabled: %{}}}
  end

  @impl true
  def handle_call({:start, uuid, source_uuids}, _from, state) do
    state =
      state
      |> put_enabled(uuid, true)
      |> put_dependencies(uuid, source_uuids)

    case ensure_session(uuid, %{state | desired: MapSet.put(state.desired, uuid)}) do
      {:ok, next} -> {:reply, :ok, next}
      {:error, reason, next} -> {:reply, {:error, reason}, next}
    end
  end

  @impl true
  def handle_call({:close, uuid}, _from, state) do
    state = %{state | desired: MapSet.delete(state.desired, uuid)}

    case Supervisor.stop_session(uuid) do
      :ok ->
        state =
          state
          |> Map.update!(:sessions, &Map.delete(&1, uuid))
          |> Map.update!(:enabled, &Map.delete(&1, uuid))
          |> remove_dependencies(uuid)

        {:reply, :ok, state}

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  @impl true
  def handle_call({:ensure_closable, uuid}, _from, state) do
    state = reconcile_open_derived(state)

    case durable_derived_enabled?(uuid) do
      {:ok, true} ->
        {:reply, derived_enabled_close_error(), state}

      {:ok, false} ->
        if Map.get(state.enabled, uuid, false),
          do: {:reply, derived_enabled_close_error(), state},
          else: dependency_close_reply(state, uuid)

      {:error, _} = error ->
        {:reply, error, state}
    end
  end

  @impl true
  def handle_cast({:refresh, uuid}, state) do
    _ = Worker.refresh(uuid)
    {:noreply, state}
  end

  @impl true
  def handle_cast({:rebuild, uuid}, state) do
    _ = Worker.rebuild(uuid)
    {:noreply, state}
  end

  @impl true
  def handle_info(:reconcile, state) do
    sessions =
      DynamicSupervisor.which_children(VialKeeper.DerivedView.Supervisor)
      |> Enum.reduce(state.sessions, fn
        {{:derived_session, uuid}, pid, _type, _modules}, acc when is_pid(pid) ->
          track_session(acc, uuid, pid)

        _child, acc ->
          acc
      end)

    state = %{
      state
      | sessions: sessions,
        desired: MapSet.union(state.desired, MapSet.new(Map.keys(sessions)))
    }

    {:noreply, reconcile_open_derived(state)}
  end

  @impl true
  def handle_info({:DOWN, ref, :process, pid, reason}, state) do
    case Enum.find(state.sessions, fn {_uuid, session} ->
           session.pid == pid and session.ref == ref
         end) do
      {uuid, _session} ->
        handle_session_down(uuid, reason, state)

      nil ->
        {:noreply, state}
    end
  end

  defp handle_session_down(uuid, reason, state) do
    state = %{state | sessions: Map.delete(state.sessions, uuid)}

    if MapSet.member?(state.desired, uuid),
      do: restart_session(uuid, reason, state),
      else: {:noreply, state}
  end

  defp restart_session(uuid, reason, state) do
    case ensure_session(uuid, state) do
      {:ok, next} ->
        {:noreply, next}

      {:error, error, next} ->
        Logger.warning(
          "derived materializer session stopped database=#{uuid} reason=#{inspect(reason)} error=#{inspect(error)}"
        )

        {:noreply, next}
    end
  end

  defp ensure_session(uuid, state) do
    case Supervisor.start_session(uuid) do
      {:ok, pid} ->
        {:ok, %{state | sessions: track_session(state.sessions, uuid, pid)}}

      {:error, {:already_started, pid}} ->
        {:ok, %{state | sessions: track_session(state.sessions, uuid, pid)}}

      {:error, reason} ->
        {:error, reason, state}
    end
  end

  defp reconcile_open_derived(state) do
    open_derived = MapSet.new(open_derived_uuids())

    state =
      Enum.reduce(state.sessions, state, fn {uuid, _session}, state ->
        if MapSet.member?(open_derived, uuid),
          do: state,
          else: stop_reconciled_session(state, uuid)
      end)

    Enum.reduce(open_derived, state, &reconcile_open_derived_entry(&2, &1))
  end

  defp open_derived_uuids do
    Registry.select(VialKeeper.Runtime.DatabaseRegistry, [
      {{{:owner, :"$1"}, :"$2", :"$3"}, [], [:"$1"]}
    ])
    |> Enum.filter(&derived_owner?/1)
  end

  defp derived_owner?(uuid) do
    case DatabaseOwner.command(uuid, {:command, :identity, %{}}) do
      {:ok, %{database_kind: :derived}} -> true
      {:ok, %{"database_kind" => "derived"}} -> true
      _ -> false
    end
  end

  defp reconcile_open_derived_entry(state, uuid) do
    case DatabaseOwner.command(uuid, {:command, :get_derived_view, %{}}) do
      {:ok, %{enabled: true, definition: %{sources: sources}}} when is_list(sources) ->
        state =
          state
          |> put_enabled(uuid, true)
          |> put_dependencies(uuid, sources)
          |> Map.update!(:desired, &MapSet.put(&1, uuid))

        case ensure_session(uuid, state) do
          {:ok, next} ->
            next

          {:error, error, next} ->
            Logger.warning(
              "derived materializer reconciliation could not start database=#{uuid} error=#{inspect(error)}"
            )

            next
        end

      {:ok, %{enabled: false}} ->
        stop_reconciled_session(state, uuid)

      {:ok, _metadata} ->
        Logger.warning(
          "derived materializer reconciliation found invalid metadata database=#{uuid}"
        )

        state

      {:error, error} ->
        Logger.warning(
          "derived materializer reconciliation could not read database=#{uuid} error=#{inspect(error)}"
        )

        state
    end
  end

  defp stop_reconciled_session(state, uuid) do
    case Supervisor.stop_session(uuid) do
      :ok ->
        state
        |> Map.update!(:sessions, &Map.delete(&1, uuid))
        |> Map.update!(:desired, &MapSet.delete(&1, uuid))
        |> Map.update!(:enabled, &Map.delete(&1, uuid))
        |> remove_dependencies(uuid)

      {:error, error} ->
        Logger.warning(
          "derived materializer reconciliation could not stop disabled database=#{uuid} error=#{inspect(error)}"
        )

        state
    end
  end

  defp durable_derived_enabled?(uuid) do
    case DatabaseOwner.command(uuid, {:command, :identity, %{}}) do
      {:ok, %{database_kind: :derived}} ->
        durable_derived_view_enabled?(uuid)

      {:ok, %{"database_kind" => "derived"}} ->
        durable_derived_view_enabled?(uuid)

      {:ok, _identity} ->
        {:ok, false}

      {:error, %VialKeeper.Error{code: :database_closed}} ->
        {:ok, false}

      {:error, _} = error ->
        error
    end
  end

  defp durable_derived_view_enabled?(uuid) do
    case DatabaseOwner.command(uuid, {:command, :get_derived_view, %{}}) do
      {:ok, %{enabled: enabled}} when is_boolean(enabled) ->
        {:ok, enabled}

      {:ok, _metadata} ->
        {:error, VialKeeper.Error.integrity_violation("derived enabled state is invalid")}

      {:error, _} = error ->
        error
    end
  end

  defp derived_enabled_close_error do
    {:error,
     VialKeeper.Error.database_not_closable(
       "enabled derived materializer must be disabled before close"
     )}
  end

  defp dependency_close_reply(state, uuid) do
    dependents = Map.get(state.dependencies, uuid, MapSet.new())

    if MapSet.size(dependents) > 0 do
      {:reply,
       {:error,
        VialKeeper.Error.database_not_closable(
          "database has active derived materializer dependencies",
          %{dependent_count: MapSet.size(dependents)}
        )}, state}
    else
      {:reply, :ok, state}
    end
  end

  defp put_enabled(state, uuid, enabled),
    do: %{state | enabled: Map.put(state.enabled, uuid, enabled)}

  defp put_dependencies(state, _uuid, nil), do: state

  defp put_dependencies(state, uuid, source_uuids) when is_list(source_uuids) do
    state = remove_dependencies(state, uuid)

    dependencies =
      Enum.reduce(source_uuids, state.dependencies, fn source_uuid, acc ->
        Map.update(acc, source_uuid, MapSet.new([uuid]), &MapSet.put(&1, uuid))
      end)

    %{state | dependencies: dependencies}
  end

  defp remove_dependencies(state, uuid) do
    dependencies =
      state.dependencies
      |> Enum.reduce(%{}, fn {source_uuid, derived_uuids}, acc ->
        remaining = MapSet.delete(derived_uuids, uuid)
        if MapSet.size(remaining) == 0, do: acc, else: Map.put(acc, source_uuid, remaining)
      end)

    %{state | dependencies: dependencies}
  end

  defp track_session(sessions, uuid, pid) do
    sessions
    |> untrack_existing(uuid, pid)
    |> Map.put(uuid, %{pid: pid, ref: Process.monitor(pid)})
  end

  defp untrack_existing(sessions, uuid, pid) do
    case Map.get(sessions, uuid) do
      %{pid: ^pid} ->
        sessions

      %{ref: ref} ->
        Process.demonitor(ref, [:flush])
        Map.delete(sessions, uuid)

      nil ->
        sessions
    end
  end
end
