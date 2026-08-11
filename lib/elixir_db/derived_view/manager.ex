defmodule ElixirDB.DerivedView.Manager do
  @moduledoc "Coordinates the lifecycle and recovery of open derived materializer sessions."
  use GenServer
  require Logger

  alias ElixirDB.DerivedView.{Supervisor, Worker}

  @call_timeout 30_000

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(_opts \\ []), do: GenServer.start_link(__MODULE__, [], name: __MODULE__)

  @spec start(binary()) :: :ok | {:error, term()}
  def start(uuid) when is_binary(uuid),
    do: GenServer.call(__MODULE__, {:start, uuid}, @call_timeout)

  @spec close(binary()) :: :ok | {:error, term()}
  def close(uuid) when is_binary(uuid),
    do: GenServer.call(__MODULE__, {:close, uuid}, @call_timeout)

  @spec refresh(binary()) :: :ok
  def refresh(uuid) when is_binary(uuid) do
    GenServer.cast(__MODULE__, {:refresh, uuid})
    :ok
  catch
    :exit, _ -> :ok
  end

  @spec worker_pid(binary()) :: {:ok, pid()} | :error
  def worker_pid(uuid) when is_binary(uuid), do: Worker.pid(uuid)

  @impl true
  def init(_args) do
    send(self(), :reconcile)
    {:ok, %{sessions: %{}, desired: MapSet.new()}}
  end

  @impl true
  def handle_call({:start, uuid}, _from, state) do
    case ensure_session(uuid, %{state | desired: MapSet.put(state.desired, uuid)}) do
      {:ok, next} -> {:reply, :ok, next}
      {:error, reason, next} -> {:reply, {:error, reason}, next}
    end
  end

  @impl true
  def handle_call({:close, uuid}, _from, state) do
    state = %{state | desired: MapSet.delete(state.desired, uuid)}

    case Supervisor.stop_session(uuid) do
      :ok -> {:reply, :ok, %{state | sessions: Map.delete(state.sessions, uuid)}}
      {:error, reason} -> {:reply, {:error, reason}, state}
    end
  end

  @impl true
  def handle_cast({:refresh, uuid}, state) do
    _ = Worker.refresh(uuid)
    {:noreply, state}
  end

  @impl true
  def handle_info(:reconcile, state) do
    sessions =
      DynamicSupervisor.which_children(ElixirDB.DerivedView.Supervisor)
      |> Enum.reduce(state.sessions, fn
        {{:derived_session, uuid}, pid, _type, _modules}, acc when is_pid(pid) ->
          track_session(acc, uuid, pid)

        _child, acc ->
          acc
      end)

    desired = MapSet.union(state.desired, MapSet.new(Map.keys(sessions)))
    {:noreply, %{state | sessions: sessions, desired: desired}}
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
