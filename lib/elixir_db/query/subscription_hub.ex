defmodule ElixirDB.Query.SubscriptionHub do
  @moduledoc "Per-database coordinator for live-query subscription state and change delivery."
  use GenServer

  alias ElixirDB.Changes.Page
  alias ElixirDB.MapAccess
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
         reading: false,
         read_pending: false,
         read_task: nil,
         resetting: false,
         reset_floor: nil
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
    state = cancel_read_task(state)

    Enum.each(state.subscriptions, fn {pid, _} ->
      send(pid, :subscription_closed)
    end)

    if is_reference(state.notifier_ref) do
      ChangeNotifier.unsubscribe(state.uuid, state.notifier_ref)
    end

    {:stop, :normal, :ok,
     %{state | subscriptions: %{}, notifier_ref: nil, reading: false, read_task: nil}}
  end

  def handle_call({:snapshot_ready, pid, sequence}, _from, state) do
    case Map.get(state.subscriptions, pid) do
      %{mode: :pending_snapshot, pending_incremental_events: events} = subscription ->
        retained =
          events
          |> :queue.to_list()
          |> Enum.filter(&(&1.sequence > sequence))
          |> :queue.from_list()

        updated = %{
          subscription
          | mode: :snapshot_draining,
            pending_incremental_events: retained,
            boundary_sequence: sequence
        }

        state =
          state
          |> put_subscription(pid, updated)
          |> advance_cursor_after_snapshot(sequence)

        {:reply, :ok, state}

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

      %{credits: credits, max_buffered_events: maximum} = subscription ->
        {:noreply,
         put_subscription(state, pid, %{subscription | credits: min(credits + 1, maximum)})}
    end
  end

  def handle_cast({:unregister, pid}, state), do: {:noreply, drop_subscription(state, pid)}

  @impl true
  def handle_info({ref, result}, %{read_task: %Task{ref: ref}} = state) do
    Process.demonitor(ref, [:flush])
    state = %{state | reading: false, read_task: nil}
    {:noreply, apply_read_result(state, result)}
  end

  def handle_info({ref, _result}, state) when is_reference(ref), do: {:noreply, state}

  def handle_info({:DOWN, ref, :process, _pid, reason}, %{read_task: %Task{ref: ref}} = state) do
    state = %{state | reading: false, read_task: nil}

    case reason do
      :normal ->
        {:noreply, finish_read(state)}

      _ ->
        error =
          ElixirDB.Error.internal_error("subscription changes read failed", %{
            cause: inspect(reason)
          })

        {:noreply, finish_read(fail_all(state, error))}
    end
  end

  def handle_info({:DOWN, ref, :process, pid, _reason}, state) do
    case Map.get(state.subscriptions, pid) do
      %{monitor_ref: ^ref} -> {:noreply, drop_subscription(state, pid)}
      _ -> {:noreply, state}
    end
  end

  def handle_info({:database_changed, uuid, _sequence}, %{uuid: uuid} = state),
    do: {:noreply, schedule_read(state)}

  def handle_info({:database_maintenance, uuid, %{new_floor: floor}}, %{uuid: uuid} = state)
      when is_integer(floor) and floor > state.cursor_sequence do
    error =
      ElixirDB.Error.history_truncated(
        "subscription hub cursor is below the retention floor",
        %{retention_floor: floor}
      )

    {:noreply, begin_history_reset(state, error)}
  end

  def handle_info({:database_maintenance, uuid, _event}, %{uuid: uuid} = state),
    do: {:noreply, schedule_read(state)}

  def handle_info({:database_closed, uuid}, %{uuid: uuid} = state) do
    state = cancel_read_task(state)
    Enum.each(state.subscriptions, fn {pid, _} -> send(pid, :subscription_closed) end)

    {:stop, :normal,
     %{state | subscriptions: %{}, notifier_ref: nil, reading: false, read_task: nil}}
  end

  defp ensure_notifier(%{notifier_ref: nil} = state) do
    with {:ok, identity} <-
           DatabaseCatalog.command_as(state.uuid, :subscription, {:command, :identity, %{}}),
         sequence when is_integer(sequence) <-
           MapAccess.get(identity, :current_sequence, 0),
         {:ok, ref, _notifier_sequence} <- ChangeNotifier.subscribe(state.uuid, sequence),
         {:ok, identity_after} <-
           DatabaseCatalog.command_as(state.uuid, :subscription, {:command, :identity, %{}}) do
      current = MapAccess.get(identity_after, :current_sequence, sequence)

      state = %{
        state
        | notifier_ref: ref,
          cursor_sequence: max(state.cursor_sequence, sequence)
      }

      state = if current > sequence, do: schedule_read(state), else: state
      {:ok, state}
    else
      {:error, %ElixirDB.Error{} = error} ->
        {:error, error}

      {:error, reason} ->
        {:error,
         ElixirDB.Error.database_unavailable("subscription notifier subscribe failed", %{
           cause: inspect(reason)
         })}

      other ->
        {:error,
         ElixirDB.Error.database_unavailable("subscription notifier subscribe failed", %{
           cause: inspect(other)
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
      pending_incremental_events: :queue.new()
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

  defp schedule_read(%{resetting: true} = state), do: state

  defp schedule_read(%{reading: true} = state), do: %{state | read_pending: true}

  defp schedule_read(%{read_task: %Task{}} = state), do: %{state | read_pending: true}

  defp schedule_read(state) do
    if subscription_hub_reads_paused?(state.uuid) do
      %{state | read_pending: true}
    else
      uuid = state.uuid
      since = state.cursor_sequence

      task =
        Task.Supervisor.async_nolink(ElixirDB.TaskSupervisor, fn ->
          fetch_batch(uuid, since)
        end)

      %{state | reading: true, read_pending: false, read_task: task}
    end
  end

  defp subscription_hub_reads_paused?(uuid) when is_binary(uuid) do
    case Application.get_env(:elixir_db, :subscription_hub_pause_reads) do
      ^uuid -> true
      true -> true
      _ -> false
    end
  end

  defp finish_read(%{read_pending: true} = state),
    do: schedule_read(%{state | read_pending: false})

  defp finish_read(state), do: state

  defp fetch_batch(uuid, since) do
    # Test barrier: in-flight hub tasks must not acquire after pause is set.
    if subscription_hub_reads_paused?(uuid) do
      {:ok, %{normalize_changes_result(%{}) | last_sequence: since}}
    else
      fetch_batch_unpaused(uuid, since)
    end
  end

  defp fetch_batch_unpaused(uuid, since) do
    case DatabaseCatalog.command_as(
           uuid,
           :subscription,
           {:command, :read_changes, %{since: since, limit: @batch_limit}}
         ) do
      {:ok, %{results: []} = result} ->
        {:ok, normalize_changes_result(result)}

      {:ok, %{results: results} = result} ->
        fetch_revision_envelopes(uuid, normalize_changes_result(result), results)

      {:ok, result} when is_map(result) ->
        results = MapAccess.get(result, :results, [])
        normalized = normalize_changes_result(result)

        if results == [] do
          {:ok, normalized}
        else
          fetch_revision_envelopes(uuid, normalized, results)
        end

      {:error, error} ->
        {:error, error}
    end
  end

  defp normalize_changes_result(result) when is_map(result) do
    Page.new(
      MapAccess.get(result, :results, []),
      MapAccess.get(result, :last_sequence, 0),
      MapAccess.get(result, :has_more, false)
    )
  end

  defp fetch_revision_envelopes(uuid, result, results) do
    requests =
      Enum.map(results, fn result_item ->
        %{document_id: result_item.document_id, revision_id: result_item.winning_revision}
      end)

    case DatabaseCatalog.command_as(
           uuid,
           :subscription,
           {:command, :get_revisions_batch, %{requests: requests}}
         ) do
      {:ok, envelopes} -> attach_envelopes(result, results, envelopes)
      {:error, error} -> {:error, error}
    end
  end

  defp attach_envelopes(result, results, envelopes) do
    case validate_envelopes(results, envelopes) do
      :ok -> {:ok, Map.put(result, :envelopes, envelopes)}
      {:error, error} -> {:error, error}
    end
  end

  defp apply_read_result(%{resetting: true} = state, {:ok, _result}),
    do: finish_read(state)

  defp apply_read_result(state, {:ok, %{results: []} = result}) do
    has_more = Map.get(result, :has_more, false)
    last_sequence = Map.get(result, :last_sequence, state.cursor_sequence)

    state =
      if is_integer(last_sequence),
        do: %{state | cursor_sequence: max(state.cursor_sequence, last_sequence)},
        else: state

    if has_more, do: schedule_read(state), else: finish_read(state)
  end

  defp apply_read_result(state, {:ok, %{results: results, envelopes: envelopes} = result}) do
    last_sequence = Map.fetch!(result, :last_sequence)
    has_more = Map.get(result, :has_more, false)

    state =
      state
      |> fanout(results, envelopes)
      |> Map.put(:cursor_sequence, last_sequence)

    if has_more, do: schedule_read(state), else: finish_read(state)
  end

  defp apply_read_result(state, {:error, %ElixirDB.Error{code: :history_truncated} = error}) do
    finish_read(begin_history_reset(state, error))
  end

  defp apply_read_result(state, {:error, error}), do: finish_read(fail_all(state, error))

  defp begin_history_reset(%{resetting: true} = state, error) do
    floor = retention_floor(error)

    state =
      if is_integer(floor),
        do: %{state | reset_floor: max(state.reset_floor || 0, floor)},
        else: state

    state
    |> cancel_read_task()
    |> reset_all_subscriptions()
  end

  defp begin_history_reset(state, error) do
    floor = retention_floor(error)

    state =
      cancel_read_task(%{
        state
        | resetting: true,
          reset_floor: floor,
          read_pending: false
      })

    reset_all_subscriptions(state)
  end

  defp reset_all_subscriptions(state) do
    Enum.reduce(state.subscriptions, state, fn {pid, subscription}, acc ->
      updated = %{
        subscription
        | mode: :pending_snapshot,
          pending_incremental_events: :queue.new(),
          boundary_sequence: nil,
          credits: subscription.max_buffered_events
      }

      send(pid, :subscription_reset)
      put_subscription(acc, pid, updated)
    end)
  end

  defp advance_cursor_after_snapshot(state, sequence) when is_integer(sequence) do
    state = %{state | cursor_sequence: max(state.cursor_sequence, sequence)}

    cond do
      state.resetting and is_integer(state.reset_floor) and
          state.cursor_sequence >= state.reset_floor ->
        schedule_read(%{state | resetting: false, reset_floor: nil})

      state.resetting and is_nil(state.reset_floor) ->
        schedule_read(%{state | resetting: false})

      true ->
        state
    end
  end

  defp advance_cursor_after_snapshot(state, _sequence), do: state

  defp retention_floor(%ElixirDB.Error{details: details}) when is_map(details) do
    MapAccess.get(details, :retention_floor)
  end

  defp retention_floor(_), do: nil

  defp validate_envelopes(results, envelopes) when length(results) != length(envelopes) do
    {:error,
     ElixirDB.Error.integrity_violation("revision batch size does not match changes batch size")}
  end

  defp validate_envelopes(results, envelopes) do
    Enum.zip(results, envelopes)
    |> Enum.reduce_while(:ok, fn {result, envelope}, :ok ->
      if envelope_matches?(result, envelope) do
        {:cont, :ok}
      else
        {:halt,
         {:error,
          ElixirDB.Error.integrity_violation("revision batch entry does not match changes entry")}}
      end
    end)
  end

  defp envelope_matches?(result, envelope) do
    MapAccess.get(envelope, :id) == result.document_id and
      MapAccess.get(envelope, :revision) == result.winning_revision
  end

  defp fanout(state, results, envelopes) do
    Enum.zip(results, envelopes)
    |> Enum.reduce(state, fn {result, envelope}, acc ->
      event = %{sequence: result.sequence, envelope: envelope}
      deliver_or_buffer(acc, event)
    end)
  end

  defp fail_all(state, error) do
    Enum.each(state.subscriptions, fn {pid, _} -> send(pid, {:subscription_error, error}) end)
    state
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
        if :queue.len(events) >= maximum do
          send(
            pid,
            {:subscription_overloaded,
             ElixirDB.Error.subscription_overloaded("subscription delivery buffer is full")}
          )

          drop_subscription(state, pid)
        else
          put_subscription(state, pid, %{
            subscription
            | pending_incremental_events: :queue.in(event, events)
          })
        end
    end
  end

  defp deliver_pending(state, pid, sequence) do
    case Map.get(state.subscriptions, pid) do
      %{pending_incremental_events: events} ->
        events
        |> :queue.to_list()
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
        put_subscription(state, pid, %{subscription | pending_incremental_events: :queue.new()})
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
       when map_size(subscriptions) == 0 and is_reference(ref) do
    ChangeNotifier.unsubscribe(state.uuid, ref)
    %{state | notifier_ref: nil}
  end

  defp maybe_release_notifier(state), do: state

  defp cancel_read_task(%{read_task: %Task{pid: pid} = task} = state) do
    _ = Task.Supervisor.terminate_child(ElixirDB.TaskSupervisor, pid)
    Process.demonitor(task.ref, [:flush])
    %{state | reading: false, read_task: nil, read_pending: false}
  end

  defp cancel_read_task(state), do: state
end
