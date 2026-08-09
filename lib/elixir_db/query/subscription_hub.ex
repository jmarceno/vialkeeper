defmodule ElixirDB.Query.SubscriptionHub do
  @moduledoc false
  use GenServer

  alias ElixirDB.Changes
  alias ElixirDB.Runtime.{ChangeNotifier, DatabaseCatalog}

  @batch_limit 100

  def child_spec(uuid),
    do: %{
      id: {__MODULE__, uuid},
      start: {__MODULE__, :start_link, [uuid]},
      restart: :permanent,
      type: :worker
    }

  def start_link(uuid), do: GenServer.start_link(__MODULE__, uuid, name: via(uuid))

  def via(uuid),
    do: {:via, Registry, {ElixirDB.Runtime.DatabaseRegistry, {:query_subscription_hub, uuid}}}

  def begin_subscription(uuid, pid, max_buffered_events, max_active \\ nil),
    do: call(uuid, {:begin_subscription, pid, max_buffered_events, max_active})

  def snapshot_ready(uuid, pid, sequence), do: call(uuid, {:snapshot_ready, pid, sequence})
  def activate(uuid, pid, sequence), do: call(uuid, {:activate, pid, sequence})
  def return_credit(uuid, pid), do: cast(uuid, {:return_credit, pid})
  def unregister(uuid, pid), do: cast(uuid, {:unregister, pid})
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
  def init(uuid),
    do:
      {:ok,
       %{
         uuid: uuid,
         cursor_sequence: 0,
         notifier_ref: nil,
         subscriptions: %{},
         reading: false
       }}

  @impl true
  def handle_call({:begin_subscription, pid, max_buffered_events, max_active}, _from, state) do
    if is_integer(max_active) and map_size(state.subscriptions) >= max_active do
      {:reply,
       {:error, ElixirDB.Error.subscription_overloaded("maximum active subscriptions reached")},
       state}
    else
      admit_subscription(state, pid, max_buffered_events)
    end
  end

  def handle_call({:activate, pid, sequence}, _from, state) do
    case Map.get(state.subscriptions, pid) do
      %{mode: :snapshot_draining} = subscription ->
        state = put_subscription(state, pid, %{subscription | mode: :active})
        {:reply, :ok, deliver_pending(state, pid, sequence)}

      nil ->
        {:reply, {:error, ElixirDB.Error.invalid_request("subscription is not registered")}, state}

      %{mode: mode} ->
        {:reply,
         {:error, ElixirDB.Error.invalid_request("subscription cannot activate from #{mode}")},
         state}
    end
  end

  def handle_call(:count, _from, state), do: {:reply, map_size(state.subscriptions), state}

  def handle_call(:close, _from, state) do
    Enum.each(state.subscriptions, fn {pid, _} -> send(pid, :subscription_closed) end)
    ChangeNotifier.unsubscribe(state.uuid, state.notifier_ref)
    {:stop, :normal, :ok, %{state | subscriptions: %{}, notifier_ref: nil}}
  end

  def handle_call({:snapshot_ready, pid, sequence}, _from, state) do
    case Map.get(state.subscriptions, pid) do
      %{mode: :pending_snapshot, pending_incremental_events: events} = subscription ->
        retained = Enum.filter(events, &(&1.sequence > sequence))

        updated = %{
          subscription
          | mode: :snapshot_draining,
            pending_incremental_events: retained,
            boundary_sequence: sequence
        }

        {:reply, :ok, put_subscription(state, pid, updated)}

      nil ->
        {:reply, {:error, ElixirDB.Error.invalid_request("subscription is not registered")}, state}

      %{mode: mode} ->
        {:reply,
         {:error,
          ElixirDB.Error.invalid_request("subscription cannot snapshot_ready from #{mode}")}, state}
    end
  end

  @impl true
  def handle_cast({:return_credit, pid}, state) do
    case Map.get(state.subscriptions, pid) do
      nil ->
        {:noreply, state}

      subscription ->
        {:noreply,
         put_subscription(state, pid, %{subscription | credits: subscription.credits + 1})}
    end
  end

  def handle_cast({:unregister, pid}, state), do: {:noreply, drop_subscription(state, pid)}

  @impl true
  def handle_info(:read_changes, %{reading: true} = state), do: {:noreply, state}

  def handle_info(:read_changes, state) do
    case Changes.read(state.uuid, %{since: state.cursor_sequence, limit: @batch_limit}) do
      {:ok, %{results: []}} ->
        {:noreply, %{state | reading: false}}

      {:ok, %{results: results, last_sequence: last_sequence, has_more: has_more}} ->
        state = fanout(state, results)
        state = %{state | cursor_sequence: last_sequence, reading: false}
        state = if has_more, do: schedule_read(state), else: state
        {:noreply, state}

      {:error, error} ->
        Enum.each(state.subscriptions, fn {pid, _} -> send(pid, {:subscription_error, error}) end)
        {:noreply, %{state | reading: false}}
    end
  end

  def handle_info({:database_changed, uuid, _sequence}, %{uuid: uuid} = state),
    do: {:noreply, schedule_read(state)}

  def handle_info({:database_maintenance, uuid, _event}, %{uuid: uuid} = state),
    do: {:noreply, schedule_read(state)}

  def handle_info({:database_closed, uuid}, %{uuid: uuid} = state) do
    Enum.each(state.subscriptions, fn {pid, _} -> send(pid, :subscription_closed) end)
    {:stop, :normal, %{state | subscriptions: %{}, notifier_ref: nil, reading: false}}
  end

  def handle_info({:DOWN, ref, :process, pid, _reason}, state) do
    case Map.get(state.subscriptions, pid) do
      %{monitor_ref: ^ref} -> {:noreply, drop_subscription(state, pid)}
      _ -> {:noreply, state}
    end
  end

  defp ensure_notifier(%{notifier_ref: nil} = state) do
    case ChangeNotifier.subscribe(state.uuid, state.cursor_sequence) do
      {:ok, ref, current} ->
        {:ok, %{state | notifier_ref: ref, cursor_sequence: max(state.cursor_sequence, current)}}

      {:error, %ElixirDB.Error{} = error} ->
        {:error, error}

      {:error, reason} ->
        {:error,
         ElixirDB.Error.database_unavailable("subscription notifier subscribe failed", %{
           cause: inspect(reason)
         })}
    end
  end

  defp ensure_notifier(state), do: {:ok, state}

  defp admit_subscription(state, pid, max_buffered_events) do
    ref = Process.monitor(pid)

    subscription = %{
      pid: pid,
      monitor_ref: ref,
      mode: :pending_snapshot,
      credits: max_buffered_events,
      max_buffered_events: max_buffered_events,
      boundary_sequence: nil,
      pending_incremental_events: []
    }

    case ensure_notifier(state) do
      {:ok, state} ->
        state =
          state
          |> put_subscription(pid, subscription)
          |> schedule_read()

        {:reply, {:ok, state.cursor_sequence}, state}

      {:error, error} ->
        Process.demonitor(ref, [:flush])
        {:reply, {:error, error}, state}
    end
  end

  defp schedule_read(%{reading: true} = state), do: state
  defp schedule_read(state), do: send(self(), :read_changes) && %{state | reading: true}

  defp fanout(state, results) do
    requests =
      Enum.map(results, fn result ->
        %{document_id: result.document_id, revision_id: result.winning_revision}
      end)

    case DatabaseCatalog.command(
           state.uuid,
           {:command, :get_revisions_batch, %{requests: requests}}
         ) do
      {:ok, envelopes} ->
        Enum.zip(results, envelopes)
        |> Enum.reduce(state, fn {result, envelope}, acc ->
          event = %{sequence: result.sequence, envelope: envelope}
          deliver_or_buffer(acc, event)
        end)

      {:error, error} ->
        Enum.each(state.subscriptions, fn {pid, _} -> send(pid, {:subscription_error, error}) end)
        state
    end
  end

  defp deliver_or_buffer(state, event) do
    Enum.reduce(state.subscriptions, state, fn {pid, subscription}, acc ->
      fanout_subscription(acc, pid, subscription, event)
    end)
  end

  defp fanout_subscription(state, pid, %{mode: mode}, event)
       when mode in [:pending_snapshot, :snapshot_draining],
       do: append_pending(state, pid, event)

  defp fanout_subscription(
         state,
         _pid,
         %{mode: :active, boundary_sequence: boundary},
         event
       )
       when is_integer(boundary) and event.sequence <= boundary,
       do: state

  defp fanout_subscription(state, pid, %{mode: :active, credits: credits} = subscription, event)
       when credits > 0 do
    send(pid, {:subscription_incremental, event})
    put_subscription(state, pid, %{subscription | credits: credits - 1})
  end

  defp fanout_subscription(state, pid, %{mode: :active}, _event) do
    send(
      pid,
      {:subscription_overloaded,
       ElixirDB.Error.subscription_overloaded("subscription delivery buffer is full")}
    )

    drop_subscription(state, pid)
  end

  defp fanout_subscription(state, _pid, _subscription, _event), do: state

  defp append_pending(state, pid, event) do
    case Map.fetch!(state.subscriptions, pid) do
      %{pending_incremental_events: events, max_buffered_events: maximum} = subscription ->
        if length(events) >= maximum do
          send(
            pid,
            {:subscription_overloaded,
             ElixirDB.Error.subscription_overloaded("subscription delivery buffer is full")}
          )

          drop_subscription(state, pid)
        else
          put_subscription(state, pid, %{
            subscription
            | pending_incremental_events: events ++ [event]
          })
        end
    end
  end

  defp deliver_pending(state, pid, sequence) do
    case Map.get(state.subscriptions, pid) do
      %{pending_incremental_events: events} ->
        events
        |> Enum.filter(&(&1.sequence > sequence))
        |> Enum.reduce_while(state, fn event, acc ->
          deliver_pending_event(acc, pid, event)
        end)
        |> clear_pending(pid)

      _ ->
        state
    end
  end

  defp deliver_pending_event(state, pid, event) do
    case Map.get(state.subscriptions, pid) do
      %{credits: credits} = subscription when credits > 0 ->
        send(pid, {:subscription_incremental, event})

        {:cont,
         put_subscription(state, pid, %{
           subscription
           | credits: credits - 1
         })}

      %{credits: 0} ->
        send(
          pid,
          {:subscription_overloaded,
           ElixirDB.Error.subscription_overloaded("subscription delivery buffer is full")}
        )

        {:halt, drop_subscription(state, pid)}

      _ ->
        {:halt, state}
    end
  end

  defp clear_pending(state, pid) do
    case Map.get(state.subscriptions, pid) do
      nil ->
        state

      subscription ->
        put_subscription(state, pid, %{subscription | pending_incremental_events: []})
    end
  end

  defp put_subscription(state, pid, subscription),
    do: %{state | subscriptions: Map.put(state.subscriptions, pid, subscription)}

  defp drop_subscription(state, pid) do
    case Map.get(state.subscriptions, pid) do
      %{monitor_ref: ref} -> Process.demonitor(ref, [:flush])
      _ -> :ok
    end

    state = %{state | subscriptions: Map.delete(state.subscriptions, pid)}
    maybe_release_notifier(state)
  end

  defp maybe_release_notifier(%{subscriptions: subscriptions, notifier_ref: ref} = state)
       when map_size(subscriptions) == 0 and not is_nil(ref) do
    ChangeNotifier.unsubscribe(state.uuid, ref)
    %{state | notifier_ref: nil}
  end

  defp maybe_release_notifier(state), do: state
end
