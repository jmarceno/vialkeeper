defmodule ElixirDB.Query.SubscriptionHub do
  @moduledoc false
  use GenServer

  @type mode :: :pending_snapshot | :snapshot_draining | :active | :resetting

  def child_spec(uuid),
    do: %{
      id: {__MODULE__, uuid},
      start: {__MODULE__, :start_link, [uuid]},
      restart: :temporary,
      type: :worker
    }

  def start_link(uuid), do: GenServer.start_link(__MODULE__, uuid, name: via(uuid))

  def via(uuid),
    do: {:via, Registry, {ElixirDB.Runtime.DatabaseRegistry, {:query_subscription_hub, uuid}}}

  def begin_subscription(uuid, subscription_pid, max_buffered_events) do
    call(uuid, {:begin_subscription, subscription_pid, max_buffered_events})
  end

  def snapshot_ready(uuid, subscription_pid, sequence) do
    cast(uuid, {:snapshot_ready, subscription_pid, sequence})
  end

  def activate(uuid, subscription_pid, sequence) do
    call(uuid, {:activate, subscription_pid, sequence})
  end

  def return_credit(uuid, subscription_pid) do
    cast(uuid, {:return_credit, subscription_pid})
  end

  def unregister(uuid, subscription_pid) do
    cast(uuid, {:unregister, subscription_pid})
  end

  def count(uuid), do: call(uuid, :count)

  def close(uuid), do: call(uuid, :close)

  defp call(uuid, message) do
    case Registry.lookup(ElixirDB.Runtime.DatabaseRegistry, {:query_subscription_hub, uuid}) do
      [{pid, _}] -> GenServer.call(pid, message)
      [] -> {:error, ElixirDB.Error.database_closed("subscription hub is not running")}
    end
  end

  defp cast(uuid, message) do
    case Registry.lookup(ElixirDB.Runtime.DatabaseRegistry, {:query_subscription_hub, uuid}) do
      [{pid, _}] -> GenServer.cast(pid, message)
      [] -> :ok
    end
  end

  @impl true
  def init(uuid) do
    {:ok,
     %{
       uuid: uuid,
       cursor_sequence: 0,
       subscriptions: %{},
       pending_events: %{}
     }}
  end

  @impl true
  def handle_call({:begin_subscription, subscription_pid, max_buffered_events}, _from, state) do
    monitor_ref = Process.monitor(subscription_pid)

    subscription = %{
      pid: subscription_pid,
      monitor_ref: monitor_ref,
      mode: :pending_snapshot,
      credits: max_buffered_events,
      pending_incremental_events: []
    }

    {:reply, {:ok, state.cursor_sequence},
     %{
       state
       | subscriptions: Map.put(state.subscriptions, subscription_pid, subscription)
     }}
  end

  @impl true
  def handle_call({:activate, subscription_pid, sequence}, _from, state) do
    case Map.get(state.subscriptions, subscription_pid) do
      %{mode: :snapshot_draining} = subscription ->
        subscription = %{subscription | mode: :active}
        state = put_subscription(state, subscription_pid, subscription)
        deliver_pending_after(state, subscription_pid, sequence)

      _ ->
        {:reply, :ok, state}
    end
  end

  @impl true
  def handle_call(:count, _from, state), do: {:reply, map_size(state.subscriptions), state}

  @impl true
  def handle_call(:close, _from, state) do
    Enum.each(state.subscriptions, fn {_pid, %{pid: pid}} ->
      send(pid, :subscription_closed)
    end)

    {:stop, :normal, :ok, %{state | subscriptions: %{}, pending_events: %{}}}
  end

  @impl true
  def handle_cast({:snapshot_ready, subscription_pid, sequence}, state) do
    case Map.get(state.subscriptions, subscription_pid) do
      %{mode: :pending_snapshot} = subscription ->
        pending =
          state.pending_events
          |> Map.get(subscription_pid, [])
          |> Enum.filter(fn %{sequence: event_sequence} -> event_sequence > sequence end)

        subscription = %{
          subscription
          | mode: :snapshot_draining,
            pending_incremental_events: pending
        }

        {:noreply,
         %{
           state
           | subscriptions: Map.put(state.subscriptions, subscription_pid, subscription),
             pending_events: Map.delete(state.pending_events, subscription_pid)
         }}

      _ ->
        {:noreply, state}
    end
  end

  @impl true
  def handle_cast({:return_credit, subscription_pid}, state) do
    case Map.get(state.subscriptions, subscription_pid) do
      nil ->
        {:noreply, state}

      subscription ->
        {:noreply,
         put_subscription(state, subscription_pid, %{
           subscription
           | credits: subscription.credits + 1
         })}
    end
  end

  @impl true
  def handle_cast({:unregister, subscription_pid}, state) do
    {:noreply, drop_subscription(state, subscription_pid)}
  end

  @impl true
  def handle_info({:DOWN, ref, :process, pid, _reason}, state) do
    case Map.get(state.subscriptions, pid) do
      %{monitor_ref: ^ref} -> {:noreply, drop_subscription(state, pid)}
      _ -> {:noreply, state}
    end
  end

  defp deliver_pending_after(state, subscription_pid, sequence) do
    subscription = Map.fetch!(state.subscriptions, subscription_pid)

    subscription.pending_incremental_events
    |> Enum.filter(fn event -> event.sequence > sequence end)
    |> Enum.sort_by(& &1.sequence)
    |> Enum.reduce(state, fn event, state ->
      deliver_event(state, subscription_pid, event)
    end)
    |> then(fn state ->
      {:reply, :ok, put_subscription(state, subscription_pid, %{subscription | mode: :active})}
    end)
  end

  defp deliver_event(state, subscription_pid, event) do
    case Map.get(state.subscriptions, subscription_pid) do
      %{credits: credits} when credits > 0 ->
        send(subscription_pid, {:subscription_incremental, event})

        put_subscription(state, subscription_pid, %{
          Map.get(state.subscriptions, subscription_pid)
          | credits: credits - 1
        })

      _ ->
        state
    end
  end

  defp put_subscription(state, pid, subscription) do
    %{state | subscriptions: Map.put(state.subscriptions, pid, subscription)}
  end

  defp drop_subscription(state, pid) do
    case Map.get(state.subscriptions, pid) do
      %{monitor_ref: ref} -> Process.demonitor(ref, [:flush])
      _ -> :ok
    end

    %{
      state
      | subscriptions: Map.delete(state.subscriptions, pid),
        pending_events: Map.delete(state.pending_events, pid)
    }
  end
end
