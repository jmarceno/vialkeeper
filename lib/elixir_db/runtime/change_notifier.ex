defmodule ElixirDB.Runtime.ChangeNotifier do
  @moduledoc false
  use GenServer

  def start_link(uuid), do: GenServer.start_link(__MODULE__, uuid, name: via(uuid))
  def via(uuid), do: {:via, Registry, {ElixirDB.Runtime.DatabaseRegistry, {:notifier, uuid}}}

  def subscribe(uuid, since) do
    case Registry.lookup(ElixirDB.Runtime.DatabaseRegistry, {:notifier, uuid}) do
      [{pid, _}] -> GenServer.call(pid, {:subscribe, self(), since})
      [] -> {:error, ElixirDB.Error.database_closed("database notifier is not running")}
    end
  end

  def publish(uuid, sequence) do
    case Registry.lookup(ElixirDB.Runtime.DatabaseRegistry, {:notifier, uuid}) do
      [{pid, _}] -> GenServer.cast(pid, {:publish, sequence})
      [] -> :ok
    end
  end

  def unsubscribe(uuid, ref) do
    case Registry.lookup(ElixirDB.Runtime.DatabaseRegistry, {:notifier, uuid}) do
      [{pid, _}] -> GenServer.cast(pid, {:unsubscribe, ref})
      [] -> :ok
    end
  end

  def subscriber_count(uuid) do
    case Registry.lookup(ElixirDB.Runtime.DatabaseRegistry, {:notifier, uuid}) do
      [{pid, _}] -> GenServer.call(pid, :subscriber_count)
      [] -> {:error, ElixirDB.Error.database_closed("database notifier is not running")}
    end
  end

  def close(uuid) do
    case Registry.lookup(ElixirDB.Runtime.DatabaseRegistry, {:notifier, uuid}) do
      [{pid, _}] ->
        try do
          GenServer.call(pid, :close)
        catch
          :exit, _ -> :ok
        end

      [] ->
        :ok
    end
  end

  @impl true
  def init(uuid), do: {:ok, %{uuid: uuid, sequence: 0, subscribers: %{}}}

  @impl true
  def handle_call({:subscribe, pid, since}, _from, state) do
    ref = Process.monitor(pid)
    state = put_in(state, [:subscribers, ref], %{pid: pid, since: since})
    {:reply, {:ok, ref, state.sequence}, state}
  end

  @impl true
  def handle_call(:subscriber_count, _from, state), do: {:reply, map_size(state.subscribers), state}

  @impl true
  def handle_call(:close, _from, state) do
    Enum.each(state.subscribers, fn {_ref, %{pid: pid}} ->
      send(pid, {:database_closed, state.uuid})
    end)

    {:reply, :ok, %{state | subscribers: %{}}}
  end

  @impl true
  def handle_cast({:publish, sequence}, state) do
    Enum.each(state.subscribers, fn {_ref, %{pid: pid, since: since}} ->
      if sequence > since, do: send(pid, {:database_changed, state.uuid, sequence})
    end)

    {:noreply, %{state | sequence: max(sequence, state.sequence)}}
  end

  @impl true
  def handle_cast({:unsubscribe, ref}, state) do
    Process.demonitor(ref, [:flush])
    {:noreply, %{state | subscribers: Map.delete(state.subscribers, ref)}}
  end

  @impl true
  def handle_info({:DOWN, ref, :process, _pid, _reason}, state),
    do: {:noreply, %{state | subscribers: Map.delete(state.subscribers, ref)}}
end
