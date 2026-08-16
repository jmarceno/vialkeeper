defmodule VialKeeper.Runtime.ChangeNotifier do
  @moduledoc "Per-database change and maintenance notification process."
  use GenServer
  alias VialKeeper.Runtime.ChildSpec

  @spec child_spec(binary()) :: map()
  def child_spec(uuid) do
    ChildSpec.worker({__MODULE__, uuid}, {__MODULE__, :start_link, [uuid]}, :temporary)
  end

  @spec start_link(binary()) :: GenServer.on_start()
  def start_link(uuid), do: GenServer.start_link(__MODULE__, uuid, name: via(uuid))

  @spec via(binary()) :: {:via, module(), term()}
  def via(uuid), do: {:via, Registry, {VialKeeper.Runtime.DatabaseRegistry, {:notifier, uuid}}}

  @spec subscribe(binary(), non_neg_integer()) ::
          {:ok, reference(), non_neg_integer()} | {:error, VialKeeper.Error.t()}
  def subscribe(uuid, since) do
    case Registry.lookup(VialKeeper.Runtime.DatabaseRegistry, {:notifier, uuid}) do
      [{pid, _}] ->
        GenServer.call(pid, {:subscribe, self(), since}, VialKeeper.Config.request_timeout_ms())

      [] ->
        {:error, VialKeeper.Error.database_closed("database notifier is not running")}
    end
  end

  @spec publish(binary(), non_neg_integer()) :: :ok
  def publish(uuid, sequence) do
    case Registry.lookup(VialKeeper.Runtime.DatabaseRegistry, {:notifier, uuid}) do
      [{pid, _}] -> GenServer.cast(pid, {:publish, sequence})
      [] -> :ok
    end
  end

  @spec publish_maintenance(binary(), map()) :: :ok
  def publish_maintenance(uuid, %{new_floor: new_floor} = event) when is_integer(new_floor) do
    case Registry.lookup(VialKeeper.Runtime.DatabaseRegistry, {:notifier, uuid}) do
      [{pid, _}] -> GenServer.cast(pid, {:publish_maintenance, event})
      [] -> :ok
    end
  end

  @spec unsubscribe(binary(), reference()) :: :ok
  def unsubscribe(uuid, ref) do
    case Registry.lookup(VialKeeper.Runtime.DatabaseRegistry, {:notifier, uuid}) do
      [{pid, _}] -> GenServer.cast(pid, {:unsubscribe, ref})
      [] -> :ok
    end
  end

  @spec subscriber_count(binary()) :: non_neg_integer() | {:error, VialKeeper.Error.t()}
  def subscriber_count(uuid) do
    case Registry.lookup(VialKeeper.Runtime.DatabaseRegistry, {:notifier, uuid}) do
      [{pid, _}] -> GenServer.call(pid, :subscriber_count, VialKeeper.Config.request_timeout_ms())
      [] -> {:error, VialKeeper.Error.database_closed("database notifier is not running")}
    end
  end

  @spec close(binary()) :: :ok
  def close(uuid) do
    case Registry.lookup(VialKeeper.Runtime.DatabaseRegistry, {:notifier, uuid}) do
      [{pid, _}] ->
        try do
          GenServer.call(pid, :close, VialKeeper.Config.shutdown_timeout())
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

    {:stop, :shutdown, :ok, %{state | subscribers: %{}}}
  end

  @impl true
  def handle_cast({:publish, sequence}, state) do
    Enum.each(state.subscribers, fn {_ref, %{pid: pid, since: since}} ->
      if sequence > since, do: send(pid, {:database_changed, state.uuid, sequence})
    end)

    {:noreply, %{state | sequence: max(sequence, state.sequence)}}
  end

  @impl true
  def handle_cast({:publish_maintenance, event}, state) do
    Enum.each(state.subscribers, fn {_ref, %{pid: pid, since: since}} ->
      if since < event.new_floor, do: send(pid, {:database_maintenance, state.uuid, event})
    end)

    {:noreply, state}
  end

  @impl true
  def handle_cast({:unsubscribe, ref}, state) when is_reference(ref) do
    Process.demonitor(ref, [:flush])
    {:noreply, %{state | subscribers: Map.delete(state.subscribers, ref)}}
  end

  def handle_cast({:unsubscribe, _ref}, state), do: {:noreply, state}

  @impl true
  def handle_info({:DOWN, ref, :process, _pid, _reason}, state),
    do: {:noreply, %{state | subscribers: Map.delete(state.subscribers, ref)}}
end
