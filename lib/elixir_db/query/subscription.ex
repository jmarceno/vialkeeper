defmodule ElixirDB.Query.Subscription do
  @moduledoc false
  use GenServer

  alias ElixirDB.Query.Subscription.{Events, Membership}
  alias ElixirDB.Query.SubscriptionHub
  alias ElixirDB.Runtime.{ChildSpec, DatabaseCatalog}

  @type status :: :snapshot | :active | :closed | :failed

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
           request.max_buffered_events
         ) do
      {:ok, sequence} ->
        send(self(), :execute_snapshot)

        {:ok,
         %{
           uuid: uuid,
           client_pid: client_pid,
           client_ref: client_ref,
           request: request,
           snapshot_sequence: sequence,
           snapshot_documents: [],
           snapshot_index: 0,
           membership: MapSet.new(),
           queue: :queue.new(),
           waiter: nil,
           heartbeat_ref: nil,
           status: :snapshot,
           terminal: nil
         }}

      {:error, error} ->
        {:stop, error}
    end
  end

  @impl true
  def handle_call(:next, _from, %{waiter: waiter} = state) when not is_nil(waiter) do
    {:reply, {:error, ElixirDB.Error.invalid_request("only one next caller is allowed")}, state}
  end

  def handle_call(:next, _from, %{status: :closed} = state),
    do: {:reply, {:closed, Events.closed()}, state}

  def handle_call(:next, _from, %{snapshot_index: index, snapshot_documents: documents} = state)
      when index < length(documents) do
    document = Enum.at(documents, index)

    {:reply, {:ok, Events.snapshot(state.snapshot_sequence, document)},
     %{state | snapshot_index: index + 1}}
  end

  def handle_call(
        :next,
        _from,
        %{snapshot_index: index, snapshot_documents: documents, status: :snapshot} = state
      )
      when index == length(documents) do
    SubscriptionHub.activate(state.uuid, self(), state.snapshot_sequence)

    {:reply, {:ok, Events.caught_up(state.snapshot_sequence)},
     %{state | status: :active, snapshot_index: index + 1}}
  end

  def handle_call(:next, from, %{queue: queue} = state) do
    case :queue.out(queue) do
      {{:value, event}, queue} ->
        maybe_return_credit(state.uuid, self(), event)
        {:reply, reply_for_event(event), %{state | queue: queue}}

      {:empty, _queue} ->
        next_or_wait(state, from)
    end
  end

  @impl true
  def handle_cast({:incremental, event}, state) do
    case Membership.transition(
           event.envelope,
           state.request,
           state.membership,
           event.sequence,
           state.request.max_members
         ) do
      {:ok, nil, membership} ->
        {:noreply, %{state | membership: membership}}

      {:ok, event_value, membership} ->
        deliver_or_queue(event_value, %{state | membership: membership})

      {:error, error} ->
        {:noreply, fail_subscription(state, error)}
    end
  end

  @impl true
  def handle_info(:execute_snapshot, state) do
    result =
      DatabaseCatalog.command(state.uuid, {
        :command,
        :execute_subscription_snapshot,
        state.request
      })

    case result do
      {:ok, %{documents: documents, member_ids: member_ids, sequence: sequence}} ->
        SubscriptionHub.snapshot_ready(state.uuid, self(), sequence)

        state = %{
          state
          | snapshot_documents: documents,
            snapshot_sequence: sequence,
            membership: MapSet.new(member_ids)
        }

        {:noreply, state}

      {:error, error} ->
        {:noreply, fail_subscription(state, error)}
    end
  end

  def handle_info({:subscription_incremental, event}, state),
    do: handle_cast({:incremental, event}, state)

  def handle_info(:heartbeat, %{waiter: nil} = state), do: {:noreply, state}

  def handle_info(:heartbeat, %{waiter: from} = state) do
    GenServer.reply(from, reply_for_event(Events.heartbeat()))
    {:noreply, %{state | waiter: nil, heartbeat_ref: nil}}
  end

  def handle_info(:subscription_closed, state),
    do: {:noreply, %{state | status: :closed, terminal: Events.closed()}}

  def handle_info({:DOWN, ref, :process, _pid, _reason}, %{client_ref: ref} = state) do
    SubscriptionHub.unregister(state.uuid, self())
    {:stop, :normal, state}
  end

  @impl true
  def terminate(_reason, state) do
    SubscriptionHub.unregister(state.uuid, self())
    :ok
  end

  defp next_or_wait(%{terminal: terminal} = state, _from) when not is_nil(terminal) do
    {:reply, reply_for_event(terminal), %{state | terminal: nil, status: :closed}}
  end

  defp next_or_wait(state, from) do
    timer = Process.send_after(self(), :heartbeat, state.request.heartbeat_ms)
    {:noreply, %{state | waiter: from, heartbeat_ref: timer}}
  end

  defp deliver_or_queue(event, %{waiter: waiter} = state) when not is_nil(waiter) do
    cancel_timer(state.heartbeat_ref)
    GenServer.reply(waiter, reply_for_event(event))
    SubscriptionHub.return_credit(state.uuid, self())
    {:noreply, %{state | waiter: nil, heartbeat_ref: nil}}
  end

  defp deliver_or_queue(event, state),
    do: {:noreply, %{state | queue: :queue.in(event, state.queue)}}

  defp fail_subscription(state, error),
    do: %{state | status: :failed, terminal: Events.error(error)}

  defp reply_for_event(%{type: :error} = event), do: {:error, event}
  defp reply_for_event(%{type: :closed} = event), do: {:closed, event}
  defp reply_for_event(event), do: {:ok, event}

  defp maybe_return_credit(_uuid, _pid, %{type: type}) when type in [:heartbeat, :closed], do: :ok
  defp maybe_return_credit(uuid, pid, _event), do: SubscriptionHub.return_credit(uuid, pid)

  defp cancel_timer(nil), do: :ok
  defp cancel_timer(ref), do: Process.cancel_timer(ref)
end
