defmodule ElixirDB.Query.Subscription do
  @moduledoc false
  use GenServer

  alias ElixirDB.Query.Subscription.{Events, Membership}
  alias ElixirDB.Query.SubscriptionHub
  alias ElixirDB.Runtime.{ChildSpec, DatabaseCatalog}

  @type status :: :awaiting_snapshot | :draining_snapshot | :active | :closed | :failed

  def child_spec(options) do
    ChildSpec.worker(
      {:query_subscription, Keyword.fetch!(options, :uuid), Keyword.fetch!(options, :client_pid)},
      {__MODULE__, :start_link, [options]},
      :temporary
    )
  end

  def start_link(options), do: GenServer.start_link(__MODULE__, options)

  def next(pid, timeout \\ 30_000), do: GenServer.call(pid, :next, timeout)
  def close(pid), do: GenServer.stop(pid, :normal)

  @impl true
  def init(options) do
    uuid = Keyword.fetch!(options, :uuid)
    client_pid = Keyword.fetch!(options, :client_pid)
    request = Keyword.fetch!(options, :request)
    client_ref = Process.monitor(client_pid)

    case SubscriptionHub.begin_subscription(
           uuid,
           self(),
           request.max_buffered_events,
           request.max_active
         ) do
      {:ok, _hub_sequence} ->
        send(self(), :execute_snapshot)

        {:ok,
         %{
           uuid: uuid,
           client_pid: client_pid,
           client_ref: client_ref,
           request: request,
           snapshot_sequence: nil,
           snapshot_documents: [],
           snapshot_index: 0,
           membership: MapSet.new(),
           queue: :queue.new(),
           waiter: nil,
           heartbeat_ref: nil,
           heartbeat_timer: nil,
           status: :awaiting_snapshot,
           terminal: nil
         }}

      {:error, %ElixirDB.Error{} = error} ->
        {:stop, error}

      {:error, reason} ->
        {:stop,
         ElixirDB.Error.database_unavailable("subscription hub failed", %{cause: inspect(reason)})}
    end
  end

  @impl true
  def handle_call(:next, _from, %{waiter: waiter} = state) when not is_nil(waiter) do
    {:reply, {:error, ElixirDB.Error.invalid_request("only one next caller is allowed")}, state}
  end

  def handle_call(:next, _from, %{terminal: terminal} = state) when not is_nil(terminal) do
    {:reply, reply_for_event(terminal), %{state | terminal: nil, status: :closed}}
  end

  def handle_call(:next, _from, %{status: :closed} = state),
    do: {:reply, {:closed, Events.closed()}, state}

  def handle_call(:next, from, %{status: :awaiting_snapshot} = state),
    do: next_or_wait(state, from)

  def handle_call(
        :next,
        _from,
        %{status: :draining_snapshot, snapshot_index: index, snapshot_documents: documents} = state
      )
      when index < length(documents) do
    document = Enum.at(documents, index)

    {:reply, {:ok, Events.snapshot(state.snapshot_sequence, document)},
     %{state | snapshot_index: index + 1}}
  end

  def handle_call(
        :next,
        _from,
        %{status: :draining_snapshot, snapshot_index: index, snapshot_documents: documents} = state
      )
      when index == length(documents) do
    activate_after_snapshot(state)
  end

  def handle_call(:next, from, %{status: :active, queue: queue} = state) do
    case :queue.out(queue) do
      {{:value, event}, queue} ->
        maybe_return_credit(state.uuid, self(), event)
        {:reply, reply_for_event(event), %{state | queue: queue}}

      {:empty, _queue} ->
        next_or_wait(state, from)
    end
  end

  def handle_call(:next, from, %{status: :failed} = state), do: next_or_wait(state, from)

  @impl true
  def handle_cast({:incremental, event}, %{status: status} = state)
      when status in [:awaiting_snapshot, :draining_snapshot, :active] do
    case Membership.transition(
           event.envelope,
           state.request,
           state.membership,
           event.sequence,
           state.request.max_members
         ) do
      {:ok, nil, membership} ->
        maybe_return_credit(state.uuid, self(), %{type: :filtered})
        {:noreply, %{state | membership: membership}}

      {:ok, event_value, membership} ->
        deliver_or_queue(event_value, %{state | membership: membership})

      {:error, error} ->
        {:noreply, fail_and_wake(state, error)}
    end
  end

  def handle_cast({:incremental, _event}, state), do: {:noreply, state}

  @impl true
  def handle_info(:execute_snapshot, %{status: :awaiting_snapshot} = state) do
    result =
      DatabaseCatalog.command(state.uuid, {
        :command,
        :execute_subscription_snapshot,
        snapshot_request(state.request)
      })

    case result do
      {:ok, %{documents: documents, member_ids: member_ids, sequence: sequence}} ->
        case SubscriptionHub.snapshot_ready(state.uuid, self(), sequence) do
          :ok ->
            state = %{
              state
              | snapshot_documents: documents,
                snapshot_sequence: sequence,
                membership: MapSet.new(member_ids),
                status: :draining_snapshot
            }

            {:noreply, wake_waiter_if_present(state)}

          {:error, error} ->
            {:noreply, fail_and_wake(state, error)}
        end

      {:error, error} ->
        {:noreply, fail_and_wake(state, error)}
    end
  end

  def handle_info(:execute_snapshot, state), do: {:noreply, state}

  def handle_info({:subscription_incremental, event}, state),
    do: handle_cast({:incremental, event}, state)

  def handle_info({:subscription_overloaded, error}, state),
    do: {:noreply, fail_and_wake(state, error)}

  def handle_info({:subscription_error, error}, state),
    do: {:noreply, fail_and_wake(state, error)}

  def handle_info({:subscription_heartbeat, ref}, %{waiter: from, heartbeat_ref: ref} = state)
      when not is_nil(from) do
    GenServer.reply(from, reply_for_event(Events.heartbeat()))
    {:noreply, %{state | waiter: nil, heartbeat_ref: nil}}
  end

  def handle_info({:subscription_heartbeat, _ref}, state),
    do: {:noreply, state}

  def handle_info(:subscription_closed, state) do
    cancel_timer(state)
    SubscriptionHub.unregister(state.uuid, self())
    event = Events.closed()

    case state.waiter do
      nil ->
        {:noreply,
         %{state | status: :closed, terminal: event, heartbeat_ref: nil, heartbeat_timer: nil}}

      from ->
        GenServer.reply(from, reply_for_event(event))

        {:noreply,
         %{
           state
           | status: :closed,
             terminal: nil,
             waiter: nil,
             heartbeat_ref: nil,
             heartbeat_timer: nil
         }}
    end
  end

  def handle_info({:DOWN, ref, :process, _pid, _reason}, %{client_ref: ref} = state) do
    SubscriptionHub.unregister(state.uuid, self())
    {:stop, :normal, state}
  end

  @impl true
  def terminate(_reason, state) do
    cancel_timer(state)
    SubscriptionHub.unregister(state.uuid, self())
    :ok
  end

  defp next_or_wait(state, from) do
    cancel_timer(state)
    token = make_ref()
    timer = Process.send_after(self(), {:subscription_heartbeat, token}, state.request.heartbeat_ms)
    {:noreply, %{state | waiter: from, heartbeat_ref: token, heartbeat_timer: timer}}
  end

  defp deliver_or_queue(event, %{status: :active, waiter: waiter} = state)
       when not is_nil(waiter) do
    cancel_timer(state)
    GenServer.reply(waiter, reply_for_event(event))
    SubscriptionHub.return_credit(state.uuid, self())
    {:noreply, %{state | waiter: nil, heartbeat_ref: nil, heartbeat_timer: nil}}
  end

  defp deliver_or_queue(event, %{status: :active} = state),
    do: {:noreply, %{state | queue: :queue.in(event, state.queue)}}

  defp deliver_or_queue(event, state),
    do: {:noreply, %{state | queue: :queue.in(event, state.queue)}}

  defp fail_and_wake(state, error) do
    cancel_timer(state)
    SubscriptionHub.unregister(state.uuid, self())
    event = Events.error(error)

    case state.waiter do
      nil ->
        %{state | status: :failed, terminal: event, heartbeat_ref: nil, heartbeat_timer: nil}

      from ->
        GenServer.reply(from, reply_for_event(event))

        %{
          state
          | status: :closed,
            terminal: nil,
            waiter: nil,
            heartbeat_ref: nil,
            heartbeat_timer: nil
        }
    end
  end

  defp fail_subscription(state, error) do
    SubscriptionHub.unregister(state.uuid, self())
    %{state | status: :failed, terminal: Events.error(error)}
  end

  defp wake_waiter_if_present(%{waiter: nil} = state), do: state

  defp wake_waiter_if_present(%{waiter: from} = state) do
    cancel_timer(state)
    state = %{state | waiter: nil, heartbeat_ref: nil, heartbeat_timer: nil}

    case drain_next_reply(state) do
      {:reply, reply, new_state} ->
        GenServer.reply(from, reply)
        new_state

      :wait ->
        {:noreply, waiting} = next_or_wait(state, from)
        waiting
    end
  end

  defp drain_next_reply(
         %{status: :draining_snapshot, snapshot_index: index, snapshot_documents: documents} = state
       )
       when index < length(documents) do
    document = Enum.at(documents, index)

    {:reply, {:ok, Events.snapshot(state.snapshot_sequence, document)},
     %{state | snapshot_index: index + 1}}
  end

  defp drain_next_reply(
         %{status: :draining_snapshot, snapshot_index: index, snapshot_documents: documents} = state
       )
       when index == length(documents) do
    activate_after_snapshot(state)
  end

  defp drain_next_reply(_state), do: :wait

  defp activate_after_snapshot(%{snapshot_index: index, snapshot_sequence: sequence} = state) do
    case SubscriptionHub.activate(state.uuid, self(), sequence) do
      :ok ->
        {:reply, {:ok, Events.caught_up(sequence)},
         %{state | status: :active, snapshot_index: index + 1}}

      {:error, %ElixirDB.Error{} = error} ->
        state = fail_subscription(state, error)
        {:reply, reply_for_event(state.terminal), %{state | terminal: nil, status: :closed}}
    end
  end

  defp reply_for_event(%{type: :error} = event), do: {:error, event}
  defp reply_for_event(%{type: :closed} = event), do: {:closed, event}
  defp reply_for_event(event), do: {:ok, event}

  defp maybe_return_credit(_uuid, _pid, %{type: type})
       when type in [:heartbeat, :closed, :error, :snapshot, :caught_up, :reset],
       do: :ok

  defp maybe_return_credit(uuid, pid, _event), do: SubscriptionHub.return_credit(uuid, pid)

  defp snapshot_request(request) do
    Map.take(request, [:selector, :predicate, :fields, :max_members])
  end

  defp cancel_timer(%{heartbeat_timer: nil}), do: :ok

  defp cancel_timer(%{heartbeat_timer: timer, heartbeat_ref: token}) do
    case Process.cancel_timer(timer) do
      false ->
        receive do
          {:subscription_heartbeat, ^token} -> :ok
        after
          0 -> :ok
        end

      _ ->
        :ok
    end
  end
end
