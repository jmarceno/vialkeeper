defmodule ElixirDB.Runtime.DatabaseAdmission do
  @moduledoc "Per-database bounded scheduler that admits owner work by service class."

  use GenServer

  alias ElixirDB.Error
  alias ElixirDB.Observability.Instrumentation.Database, as: DatabaseInstrumentation

  alias ElixirDB.Runtime.{
    AdmissionCapacity,
    AdmissionPolicy,
    AdmissionSchedule,
    AdmittedCommand,
    AdmittedCommandSupervisor,
    DatabaseOwner,
    Deadline,
    ServiceClass
  }

  @default_timeout 30_000

  defmodule Waiter do
    @moduledoc "Internal wait-queue entry tracked by `DatabaseAdmission`."
    @enforce_keys [
      :request_ref,
      :from,
      :caller_pid,
      :monitor_ref,
      :class,
      :enqueued_at_ms,
      :queue_depth_at_enqueue,
      :deadline_ms,
      :mode
    ]
    defstruct @enforce_keys ++ [:owner_fun, :trace_context, :probe_op]

    @type t :: %__MODULE__{
            request_ref: reference(),
            from: GenServer.from(),
            caller_pid: pid(),
            monitor_ref: reference(),
            class: ServiceClass.t(),
            enqueued_at_ms: integer(),
            queue_depth_at_enqueue: non_neg_integer(),
            deadline_ms: Deadline.t(),
            mode: :permit | {:execute, (-> term())},
            owner_fun: nil | (-> term()),
            trace_context: term(),
            probe_op: term() | nil
          }
  end

  defmodule ActivePermit do
    @moduledoc "Internal active permit tracked by `DatabaseAdmission`."
    @enforce_keys [
      :request_ref,
      :token,
      :caller_pid,
      :caller_monitor_ref,
      :class,
      :granted_at_ms,
      :mode,
      :deadline_ms
    ]
    defstruct @enforce_keys ++
                [
                  :from,
                  :owner_fun,
                  :executor_pid,
                  :executor_monitor_ref,
                  :trace_context,
                  :probe_op,
                  executor_started?: false
                ]

    @type t :: %__MODULE__{
            request_ref: reference(),
            token: reference(),
            caller_pid: pid(),
            caller_monitor_ref: reference() | :DOWN,
            class: ServiceClass.t(),
            granted_at_ms: integer(),
            mode: :permit | {:execute, (-> term())},
            deadline_ms: Deadline.t(),
            from: GenServer.from() | nil,
            owner_fun: nil | (-> term()),
            executor_pid: pid() | nil,
            executor_monitor_ref: reference() | nil,
            trace_context: term() | nil,
            probe_op: term() | nil,
            executor_started?: boolean()
          }
  end

  @type service_class :: ServiceClass.t()
  @type waiter :: Waiter.t()
  @type active_permit :: ActivePermit.t()
  @type t :: %__MODULE__{
          uuid: binary(),
          limit: pos_integer(),
          policy: AdmissionPolicy.t(),
          schedule: [service_class()],
          cursor: non_neg_integer(),
          queues: %{service_class() => :queue.queue(reference())},
          queued_count: non_neg_integer(),
          waiting_by_ref: %{reference() => waiter()},
          active: active_permit() | nil,
          closing?: boolean(),
          admitted_command_supervisor: pid(),
          idle_waiters: [GenServer.from()],
          peak_occupancy: non_neg_integer()
        }

  @enforce_keys [
    :uuid,
    :limit,
    :policy,
    :schedule,
    :cursor,
    :queues,
    :queued_count,
    :waiting_by_ref,
    :active,
    :closing?,
    :admitted_command_supervisor,
    :idle_waiters,
    :peak_occupancy
  ]
  defstruct @enforce_keys

  def start_link({uuid, limit, policy}),
    do: GenServer.start_link(__MODULE__, {uuid, limit, policy}, name: via(uuid))

  def via(uuid), do: {:via, Registry, {ElixirDB.Runtime.DatabaseRegistry, {:admission, uuid}}}

  @spec with_permit(binary(), service_class(), timeout(), (-> term())) ::
          term() | {:error, Error.t()}
  def with_permit(uuid, class, :infinity, fun) when is_binary(uuid) and is_function(fun, 0) do
    do_with_permit(uuid, class, :infinity, :infinity, fun)
  end

  def with_permit(uuid, class, timeout, fun)
      when is_binary(uuid) and is_function(fun, 0) and is_integer(timeout) and timeout >= 0 do
    do_with_permit(uuid, class, Deadline.from_timeout(timeout), timeout, fun)
  end

  defp do_with_permit(uuid, class, deadline_ms, timeout, fun) do
    unless ServiceClass.valid?(class),
      do: raise(ArgumentError, "invalid service class #{inspect(class)}")

    request_ref = make_ref()

    try do
      uuid
      |> acquire_permit(request_ref, class, deadline_ms, timeout)
      |> run_acquired_permit(uuid, request_ref, fun)
    catch
      :exit, reason ->
        cancel_on_acquire_exit(uuid, request_ref, reason)
    end
  end

  defp acquire_permit(uuid, request_ref, class, deadline_ms, timeout) do
    call(uuid, {:acquire, request_ref, class, deadline_ms, :permit}, timeout)
  end

  defp run_acquired_permit({:ok, token}, uuid, request_ref, fun) do
    fun.()
  after
    release(uuid, request_ref, token)
  end

  defp run_acquired_permit(other, _uuid, _request_ref, _fun), do: other

  defp cancel_on_acquire_exit(uuid, request_ref, reason) do
    cancel(uuid, request_ref)
    exit(reason)
  end

  @spec with_token(binary(), (-> term())) :: term() | {:error, Error.t()}
  def with_token(uuid, fun) when is_binary(uuid) and is_function(fun, 0) do
    with_permit(uuid, :foreground, @default_timeout, fun)
  end

  @spec execute(binary(), service_class(), term(), timeout()) ::
          term() | {:error, Error.t()}
  def execute(uuid, class, command, timeout \\ @default_timeout) do
    execute_with_deadline(uuid, class, command, Deadline.from_timeout(timeout))
  end

  @spec execute_with_deadline(binary(), service_class(), term(), Deadline.t()) ::
          term() | {:error, Error.t()}
  def execute_with_deadline(uuid, class, command, :infinity) when is_binary(uuid) do
    execute_owner_with_deadline(
      uuid,
      class,
      fn -> DatabaseOwner.command(uuid, command, :infinity) end,
      :infinity,
      probe_op_from_command(command)
    )
  end

  def execute_with_deadline(uuid, class, command, deadline_ms)
      when is_binary(uuid) and is_integer(deadline_ms) do
    execute_owner_with_deadline(
      uuid,
      class,
      fn -> DatabaseOwner.command(uuid, command, Deadline.call_timeout(deadline_ms)) end,
      deadline_ms,
      probe_op_from_command(command)
    )
  end

  @spec execute_owner(binary(), service_class(), (-> term()), timeout(), term()) ::
          term() | {:error, Error.t()}
  def execute_owner(uuid, class, owner_fun, timeout \\ @default_timeout, probe_op \\ nil)
      when is_binary(uuid) and is_function(owner_fun, 0) do
    execute_owner_with_deadline(uuid, class, owner_fun, Deadline.from_timeout(timeout), probe_op)
  end

  defp execute_owner_with_deadline(uuid, class, owner_fun, :infinity, probe_op)
       when is_binary(uuid) and is_function(owner_fun, 0) do
    execute_owner_with_deadline_impl(uuid, class, owner_fun, :infinity, probe_op)
  end

  defp execute_owner_with_deadline(uuid, class, owner_fun, deadline_ms, probe_op)
       when is_binary(uuid) and is_function(owner_fun, 0) and is_integer(deadline_ms) do
    execute_owner_with_deadline_impl(uuid, class, owner_fun, deadline_ms, probe_op)
  end

  defp execute_owner_with_deadline_impl(uuid, class, owner_fun, deadline_ms, probe_op) do
    unless ServiceClass.valid?(class),
      do: raise(ArgumentError, "invalid service class #{inspect(class)}")

    trace_context = OpenTelemetry.Ctx.get_current()
    request_ref = make_ref()

    if Deadline.exhausted?(deadline_ms) do
      deadline_error()
    else
      try do
        call(
          uuid,
          {:acquire, request_ref, class, deadline_ms, {:execute, owner_fun}, trace_context,
           probe_op},
          Deadline.call_timeout(deadline_ms)
        )
      catch
        :exit, reason ->
          cancel(uuid, request_ref)
          exit(reason)
      end
    end
  end

  @spec release(binary(), reference(), reference()) :: :ok | {:error, Error.t()}
  def release(uuid, request_ref, token) do
    call(uuid, {:release, request_ref, token}, 5_000)
  end

  @spec cancel(binary(), reference()) :: :ok | {:error, Error.t()}
  def cancel(uuid, request_ref) do
    call(uuid, {:cancel, request_ref}, 5_000)
  end

  @spec begin_close(binary()) :: :ok | {:error, Error.t()}
  def begin_close(uuid), do: call(uuid, :begin_close)

  @spec cancel_close(binary()) :: :ok | {:error, Error.t()}
  def cancel_close(uuid), do: call(uuid, :cancel_close)

  @spec closing?(binary()) :: {:ok, boolean()} | {:error, Error.t()}
  def closing?(uuid), do: call(uuid, :closing?)

  @spec await_idle(binary(), timeout()) :: :ok | {:error, Error.t()}
  def await_idle(uuid, timeout), do: call(uuid, :await_idle, timeout)

  @spec stats(binary()) :: {:ok, map()} | {:error, Error.t()}
  def stats(uuid), do: call(uuid, :stats)

  @spec reset_peak_occupancy(binary()) :: :ok | {:error, Error.t()}
  def reset_peak_occupancy(uuid), do: call(uuid, :reset_peak_occupancy)

  @spec active_count(binary()) :: {:ok, non_neg_integer()} | {:error, Error.t()}
  def active_count(uuid) do
    with {:ok, stats} <- stats(uuid) do
      active =
        case Map.fetch!(stats, :active_class) do
          nil -> 0
          _ -> 1
        end

      {:ok, active}
    end
  end

  @impl true
  def init({uuid, limit, %AdmissionPolicy{} = policy}) do
    case AdmittedCommandSupervisor.lookup(uuid) do
      {:ok, admitted_command_supervisor} ->
        :ok = sync_owner_before_accepting(uuid)

        schedule = AdmissionSchedule.build(policy)
        queues = Map.new(ServiceClass.classes(), fn class -> {class, :queue.new()} end)

        {:ok,
         %__MODULE__{
           uuid: uuid,
           limit: limit,
           policy: policy,
           schedule: schedule,
           cursor: 0,
           queues: queues,
           queued_count: 0,
           waiting_by_ref: %{},
           active: nil,
           closing?: false,
           admitted_command_supervisor: admitted_command_supervisor,
           idle_waiters: [],
           peak_occupancy: 0
         }}

      {:error, :not_found} ->
        {:stop, {:admitted_command_supervisor_not_found, uuid}}
    end
  end

  @impl true
  def handle_call(:begin_close, _from, state) do
    state =
      state
      |> Map.put(:closing?, true)
      |> fail_all_queued()

    {:reply, :ok, notify_idle(state)}
  end

  def handle_call(:cancel_close, _from, state) do
    {:reply, :ok, %{state | closing?: false}}
  end

  def handle_call(:closing?, _from, state), do: {:reply, {:ok, state.closing?}, state}

  def handle_call(:await_idle, from, state) do
    if idle?(state) do
      {:reply, :ok, state}
    else
      {:noreply, %{state | idle_waiters: [from | state.idle_waiters]}}
    end
  end

  def handle_call(:stats, _from, state) do
    {:reply, {:ok, stats_snapshot(state)}, state}
  end

  def handle_call(:reset_peak_occupancy, _from, state) do
    {:reply, :ok, %{state | peak_occupancy: total_occupancy(state)}}
  end

  def handle_call({:acquire, request_ref, class, deadline_ms, mode}, from, state) do
    handle_acquire(request_ref, class, deadline_ms, mode, :undefined, from, state, nil)
  end

  def handle_call({:acquire, request_ref, class, deadline_ms, mode, trace_context}, from, state) do
    handle_acquire(request_ref, class, deadline_ms, mode, trace_context, from, state, nil)
  end

  def handle_call(
        {:acquire, request_ref, class, deadline_ms, mode, trace_context, probe_op},
        from,
        state
      ) do
    handle_acquire(request_ref, class, deadline_ms, mode, trace_context, from, state, probe_op)
  end

  def handle_call({:admitted_command_begin, request_ref, executor_pid}, _from, state) do
    case state.active do
      %ActivePermit{
        request_ref: ^request_ref,
        executor_pid: ^executor_pid,
        executor_started?: false
      } = active ->
        if caller_abandoned?(active) do
          {:reply, :cancel, state |> abandon_pre_start(active) |> grant_loop()}
        else
          {:reply, :proceed, %{state | active: %{active | executor_started?: true}}}
        end

      _ ->
        {:reply, :cancel, state}
    end
  end

  def handle_call({:release, request_ref, token}, _from, state) do
    case state.active do
      %ActivePermit{request_ref: ^request_ref, token: ^token} = active ->
        {:reply, :ok, grant_loop(clear_active(state, active))}

      %ActivePermit{request_ref: ^request_ref} ->
        {:reply, :ok, state}

      _ ->
        {:reply, :ok, state}
    end
  end

  def handle_call({:cancel, request_ref}, _from, state) do
    state =
      case state.active do
        %ActivePermit{request_ref: ^request_ref, mode: :permit} = active ->
          clear_active(state, active)

        %ActivePermit{request_ref: ^request_ref, executor_started?: true} = active ->
          abandon_post_start(state, active)

        %ActivePermit{request_ref: ^request_ref, from: from} = active ->
          if from, do: GenServer.reply(from, deadline_error())
          abandon_pre_start(state, active)

        _ ->
          cancel_queued_waiter(state, request_ref)
      end

    {:reply, :ok, grant_loop(state)}
  end

  defp cancel_queued_waiter(%__MODULE__{} = state, request_ref) do
    case Map.fetch(state.waiting_by_ref, request_ref) do
      {:ok, %Waiter{from: from}} ->
        if from, do: GenServer.reply(from, deadline_error())
        remove_queued_waiter_with_outcome(state, request_ref, :cancelled)

      :error ->
        state
    end
  end

  @impl true
  def handle_info({:executor_drain, request_ref}, state) do
    case state.active do
      %ActivePermit{request_ref: ^request_ref} = active ->
        case sync_owner_for_drain(state.uuid) do
          :ok ->
            {:noreply, grant_loop(clear_active(state, active))}

          :retry ->
            Process.send_after(self(), {:executor_drain, request_ref}, drain_sync_retry_ms())
            {:noreply, state}
        end

      _ ->
        {:noreply, state}
    end
  end

  def handle_info({:admitted_command_done, request_ref, result}, state) do
    case state.active do
      %ActivePermit{request_ref: ^request_ref, from: from} = active ->
        if from, do: GenServer.reply(from, result)

        {:noreply, grant_loop(clear_active(state, active))}

      _ ->
        {:noreply, state}
    end
  end

  def handle_info({:DOWN, monitor_ref, :process, pid, reason}, state) do
    state =
      state
      |> handle_executor_down(monitor_ref, reason)
      |> handle_active_caller_down(monitor_ref, pid)
      |> remove_queued_by_monitor(monitor_ref)

    {:noreply, grant_loop(state)}
  end

  defp call(uuid, message, timeout \\ @default_timeout) do
    case Registry.lookup(ElixirDB.Runtime.DatabaseRegistry, {:admission, uuid}) do
      [{pid, _}] ->
        try do
          GenServer.call(pid, message, timeout)
        catch
          :exit, reason ->
            if no_process_exit?(reason) do
              {:error, closed_error()}
            else
              exit(reason)
            end
        end

      [] ->
        {:error, closed_error()}
    end
  end

  defp no_process_exit?(:noproc), do: true
  defp no_process_exit?({:noproc, _details}), do: true
  defp no_process_exit?({{:noproc, _details}, _call}), do: true
  defp no_process_exit?(_reason), do: false

  defp handle_acquire(request_ref, class, deadline_ms, mode, trace_context, from, state, probe_op) do
    cond do
      state.closing? ->
        record_admission_wait(
          state.uuid,
          class,
          :closed,
          0,
          queue_depth(state),
          queue_depth(state)
        )

        {:reply, {:error, closed_error()}, state}

      not ServiceClass.valid?(class) ->
        {:reply, {:error, Error.invalid_request("invalid admission service class")}, state}

      Map.has_key?(state.waiting_by_ref, request_ref) ->
        {:reply, {:error, Error.invalid_request("duplicate admission request ref")}, state}

      accepts?(state, class) ->
        caller_pid = caller_pid(from)
        monitor_ref = Process.monitor(caller_pid)
        enqueued_at_ms = System.monotonic_time(:millisecond)

        waiter = %Waiter{
          request_ref: request_ref,
          from: from,
          caller_pid: caller_pid,
          monitor_ref: monitor_ref,
          class: class,
          enqueued_at_ms: enqueued_at_ms,
          queue_depth_at_enqueue: queue_depth(state) + 1,
          deadline_ms: deadline_ms,
          mode: mode,
          owner_fun: owner_fun_from_mode(mode),
          trace_context: trace_context,
          probe_op: probe_op
        }

        state =
          state
          |> enqueue_waiter(waiter)
          |> track_peak_occupancy()

        notify_test_hook(:enqueued, waiter.request_ref, class, probe_op, waiter.caller_pid)

        {:noreply, grant_loop(state)}

      true ->
        DatabaseInstrumentation.overload(state.uuid)
        queue_depth = queue_depth(state)

        record_admission_wait(
          state.uuid,
          class,
          :rejected,
          0,
          queue_depth,
          0
        )

        {:reply,
         {:error,
          Error.database_overloaded("database admission limit reached", %{service_class: class})},
         state}
    end
  end

  defp closed_error, do: Error.database_closed("database admission is closed")

  defp caller_pid({pid, _tag}) when is_pid(pid), do: pid

  defp owner_fun_from_mode({:execute, owner_fun}), do: owner_fun
  defp owner_fun_from_mode(:permit), do: nil

  defp enqueue_waiter(%__MODULE__{} = state, %Waiter{} = waiter) do
    queue = Map.fetch!(state.queues, waiter.class)
    queues = Map.put(state.queues, waiter.class, :queue.in(waiter.request_ref, queue))

    %{
      state
      | queues: queues,
        queued_count: state.queued_count + 1,
        waiting_by_ref: Map.put(state.waiting_by_ref, waiter.request_ref, waiter)
    }
  end

  defp remove_queued_waiter(%__MODULE__{} = state, request_ref, demonitor_caller \\ true) do
    case Map.fetch(state.waiting_by_ref, request_ref) do
      {:ok, %Waiter{monitor_ref: monitor_ref}} ->
        if demonitor_caller, do: Process.demonitor(monitor_ref, [:flush])

        %{
          state
          | queued_count: state.queued_count - 1,
            waiting_by_ref: Map.delete(state.waiting_by_ref, request_ref)
        }

      :error ->
        state
    end
  end

  defp remove_queued_waiter_with_outcome(%__MODULE__{} = state, request_ref, outcome) do
    case Map.fetch(state.waiting_by_ref, request_ref) do
      {:ok, %Waiter{} = waiter} ->
        state = remove_queued_waiter(state, request_ref)
        record_waiter_outcome(state.uuid, waiter, outcome, queue_depth(state))
        state

      :error ->
        state
    end
  end

  defp remove_queued_by_monitor(%__MODULE__{} = state, monitor_ref) do
    case Enum.find(state.waiting_by_ref, fn {_ref, waiter} -> waiter.monitor_ref == monitor_ref end) do
      {request_ref, _} -> remove_queued_waiter_with_outcome(state, request_ref, :cancelled)
      nil -> state
    end
  end

  defp fail_all_queued(%__MODULE__{} = state) do
    Enum.reduce(state.waiting_by_ref, state, fn {request_ref, waiter}, acc ->
      GenServer.reply(waiter.from, {:error, closed_error()})
      acc = remove_queued_waiter(acc, request_ref)
      record_waiter_outcome(acc.uuid, waiter, :closed, queue_depth(acc))
      acc
    end)
  end

  defp grant_loop(%__MODULE__{active: active} = state) when active != nil, do: state

  defp grant_loop(%__MODULE__{} = state) do
    case select_grant(state) do
      :none ->
        state

      {:grant, request_ref, class, queues, next_cursor, waiter} ->
        grant_waiter(%{state | queues: queues, cursor: next_cursor}, request_ref, class, waiter)
    end
  end

  defp select_grant(%__MODULE__{} = state) do
    schedule_len = length(state.schedule)

    if schedule_len == 0 or all_queues_empty?(state.queues) do
      :none
    else
      scan_grant(state, schedule_len, 0)
    end
  end

  defp scan_grant(state, schedule_len, offset) when offset < schedule_len do
    index = rem(state.cursor + offset, schedule_len)
    {:ok, class} = Enum.fetch(state.schedule, index)

    case dequeue_valid(state.queues, class, state.waiting_by_ref) do
      {nil, _queues} ->
        scan_grant(state, schedule_len, offset + 1)

      {request_ref, queues} ->
        waiter = Map.fetch!(state.waiting_by_ref, request_ref)
        {:grant, request_ref, class, queues, rem(index + 1, schedule_len), waiter}
    end
  end

  defp scan_grant(_state, _schedule_len, _offset), do: :none

  defp grant_waiter(%__MODULE__{} = state, request_ref, class, %Waiter{} = waiter) do
    state = remove_queued_waiter(state, request_ref, false)
    queue_depth_at_grant = queue_depth(state)

    if Deadline.exhausted?(waiter.deadline_ms) do
      Process.demonitor(waiter.monitor_ref, [:flush])
      GenServer.reply(waiter.from, deadline_error())

      record_admission_wait(
        state.uuid,
        class,
        :cancelled,
        wait_duration_native(waiter.enqueued_at_ms, System.monotonic_time(:millisecond)),
        waiter.queue_depth_at_enqueue,
        queue_depth_at_grant
      )

      grant_loop(state)
    else
      notify_test_hook(:granted, request_ref, class, waiter.probe_op)
      probe_admission_class(class, waiter.probe_op)
      token = make_ref()
      granted_at_ms = System.monotonic_time(:millisecond)

      record_admission_wait(
        state.uuid,
        class,
        :granted,
        wait_duration_native(waiter.enqueued_at_ms, granted_at_ms),
        waiter.queue_depth_at_enqueue,
        queue_depth_at_grant
      )

      case waiter.mode do
        :permit ->
          GenServer.reply(waiter.from, {:ok, token})

          active = %ActivePermit{
            request_ref: request_ref,
            token: token,
            caller_pid: waiter.caller_pid,
            caller_monitor_ref: waiter.monitor_ref,
            class: class,
            granted_at_ms: granted_at_ms,
            mode: :permit,
            deadline_ms: waiter.deadline_ms,
            from: nil
          }

          state |> Map.put(:active, active) |> track_peak_occupancy()

        {:execute, _owner_fun} ->
          active = %ActivePermit{
            request_ref: request_ref,
            token: token,
            caller_pid: waiter.caller_pid,
            caller_monitor_ref: waiter.monitor_ref,
            class: class,
            granted_at_ms: granted_at_ms,
            mode: {:execute, waiter.owner_fun},
            deadline_ms: waiter.deadline_ms,
            from: waiter.from,
            owner_fun: waiter.owner_fun,
            trace_context: waiter.trace_context,
            probe_op: waiter.probe_op
          }

          state
          |> Map.put(:active, active)
          |> track_peak_occupancy()
          |> start_executor()
      end
    end
  end

  defp start_executor(%__MODULE__{active: %ActivePermit{} = active} = state) do
    args = %{
      uuid: state.uuid,
      scheduler_pid: self(),
      request_ref: active.request_ref,
      owner_fun: active.owner_fun,
      deadline_ms: active.deadline_ms,
      trace_context: active.trace_context,
      probe_op: active.probe_op
    }

    case AdmittedCommand.start(state.admitted_command_supervisor, args) do
      {:ok, executor_pid} ->
        executor_monitor_ref = Process.monitor(executor_pid)

        active = %{
          active
          | executor_pid: executor_pid,
            executor_monitor_ref: executor_monitor_ref
        }

        %{state | active: active}

      {:error, reason} ->
        if active.from,
          do:
            GenServer.reply(
              active.from,
              {:error,
               Error.internal_error("failed to start admitted command", %{reason: inspect(reason)})}
            )

        grant_loop(clear_active(state, active))
    end
  end

  defp handle_executor_down(%__MODULE__{} = state, monitor_ref, reason) do
    case state.active do
      %ActivePermit{executor_monitor_ref: ^monitor_ref, executor_started?: false} = active ->
        reply_executor_crash(active, reason)
        clear_active(state, active)

      %ActivePermit{executor_monitor_ref: ^monitor_ref, executor_started?: true} = active ->
        reply_executor_crash(active, reason)

        if active.executor_monitor_ref,
          do: Process.demonitor(active.executor_monitor_ref, [:flush])

        send(self(), {:executor_drain, active.request_ref})

        %{
          state
          | active: %{
              active
              | from: nil,
                executor_pid: nil,
                executor_monitor_ref: nil
            }
        }

      _ ->
        state
    end
  end

  defp reply_executor_crash(%ActivePermit{from: from}, reason) do
    if from,
      do:
        GenServer.reply(
          from,
          {:error,
           Error.internal_error("admitted command executor crashed", %{
             reason: inspect(reason)
           })}
        )
  end

  defp handle_active_caller_down(%__MODULE__{} = state, monitor_ref, _pid) do
    case state.active do
      %ActivePermit{caller_monitor_ref: ^monitor_ref, executor_started?: true} = active ->
        abandon_post_start(state, active)

      %ActivePermit{caller_monitor_ref: ^monitor_ref} = active ->
        abandon_pre_start(state, active)

      _ ->
        state
    end
  end

  defp abandon_post_start(%__MODULE__{} = state, %ActivePermit{} = active) do
    if active.caller_monitor_ref && active.caller_monitor_ref != :DOWN,
      do: Process.demonitor(active.caller_monitor_ref, [:flush])

    %{state | active: %{active | caller_monitor_ref: :DOWN, from: nil}}
  end

  defp abandon_pre_start(%__MODULE__{} = state, %ActivePermit{} = active) do
    state
    |> terminate_executor(active)
    |> clear_active(active)
  end

  defp caller_abandoned?(%ActivePermit{caller_monitor_ref: :DOWN}), do: true

  defp caller_abandoned?(%ActivePermit{caller_pid: caller_pid}) do
    not Process.alive?(caller_pid)
  end

  defp terminate_executor(%__MODULE__{} = state, %ActivePermit{executor_pid: pid})
       when is_pid(pid) do
    # AdmittedCommand may be blocked inside handle_info/2 (test sync gate). GenServers
    # trap exits, so :shutdown would not interrupt that receive; :kill is required.
    if Process.alive?(pid), do: Process.exit(pid, :kill)
    state
  end

  defp terminate_executor(%__MODULE__{} = state, _active), do: state

  defp clear_active(%__MODULE__{} = state, %ActivePermit{} = active) do
    if active.caller_monitor_ref && active.caller_monitor_ref != :DOWN,
      do: Process.demonitor(active.caller_monitor_ref, [:flush])

    if active.executor_monitor_ref,
      do: Process.demonitor(active.executor_monitor_ref, [:flush])

    state
    |> Map.put(:active, nil)
    |> track_peak_occupancy()
    |> notify_idle()
  end

  defp dequeue_valid(queues, class, waiting_by_ref) do
    queue = Map.fetch!(queues, class)
    dequeue_valid_loop(queue, class, queues, waiting_by_ref)
  end

  defp dequeue_valid_loop(queue, class, queues, waiting_by_ref) do
    case :queue.out(queue) do
      {{:value, request_ref}, rest} ->
        if Map.has_key?(waiting_by_ref, request_ref) do
          {request_ref, Map.put(queues, class, rest)}
        else
          dequeue_valid_loop(rest, class, Map.put(queues, class, rest), waiting_by_ref)
        end

      {:empty, _} ->
        {nil, queues}
    end
  end

  defp all_queues_empty?(queues) do
    Enum.all?(queues, fn {_class, queue} -> :queue.is_empty(queue) end)
  end

  defp accepts?(state, class) do
    AdmissionCapacity.accepts?(
      state.limit,
      AdmissionPolicy.reserved_slots(state.policy),
      occupancy(state),
      class
    )
  end

  defp occupancy(%__MODULE__{} = state) do
    AdmissionCapacity.occupancy_from_counts(queued_counts(state), active_class(state))
  end

  defp queued_counts(%__MODULE__{} = state) do
    Map.new(ServiceClass.classes(), fn class ->
      count =
        Enum.count(state.waiting_by_ref, fn {_ref, %Waiter{class: waiter_class}} ->
          waiter_class == class
        end)

      {class, count}
    end)
  end

  defp queue_depth(%__MODULE__{} = state), do: state.queued_count

  defp total_occupancy(%__MODULE__{} = state) do
    queue_depth(state) + if(state.active, do: 1, else: 0)
  end

  defp active_class(%__MODULE__{active: %ActivePermit{class: class}}), do: class
  defp active_class(%__MODULE__{active: nil}), do: nil

  defp idle?(%__MODULE__{} = state), do: state.active == nil and state.queued_count == 0

  defp notify_idle(%__MODULE__{} = state) do
    if idle?(state) do
      Enum.each(state.idle_waiters, &GenServer.reply(&1, :ok))
      %{state | idle_waiters: []}
    else
      state
    end
  end

  defp deadline_error do
    {:error,
     Error.new(
       :internal_error,
       "database command timed out",
       %{reason: :deadline_exhausted},
       retryable: true
     )}
  end

  defp sync_owner_before_accepting(uuid) do
    try do
      DatabaseOwner.sync(uuid, :infinity)
    catch
      :exit, _ -> :ok
    end

    :ok
  end

  defp sync_owner_for_drain(uuid) do
    timeout = Application.get_env(:elixir_db, :executor_drain_sync_timeout, :infinity)

    try do
      DatabaseOwner.sync(uuid, timeout)
      :ok
    catch
      :exit, _ -> :retry
    end
  end

  defp drain_sync_retry_ms do
    Application.get_env(:elixir_db, :executor_drain_sync_retry_ms, 10)
  end

  defp probe_op_from_command({:command, op, _}) when is_atom(op), do: op
  defp probe_op_from_command(_), do: nil

  defp probe_admission_class(class, probe_op) do
    case Application.get_env(:elixir_db, :admission_class_probe) do
      {pid, ref} when is_pid(pid) and is_reference(ref) ->
        send(pid, {ref, :admission_grant, class, probe_op})

      _ ->
        :ok
    end
  end

  defp stats_snapshot(%__MODULE__{} = state) do
    queued = queued_counts(state)

    %{
      active_class: active_class(state),
      queued_foreground: Map.fetch!(queued, :foreground),
      queued_subscription: Map.fetch!(queued, :subscription),
      queued_replication: Map.fetch!(queued, :replication),
      queued_maintenance: Map.fetch!(queued, :maintenance),
      total_occupancy: total_occupancy(state),
      peak_occupancy: state.peak_occupancy,
      closing?: state.closing?
    }
  end

  defp track_peak_occupancy(%__MODULE__{} = state) do
    %{state | peak_occupancy: max(state.peak_occupancy, total_occupancy(state))}
  end

  defp notify_test_hook(:enqueued, request_ref, class, probe_op, caller_pid) do
    case Application.get_env(:elixir_db, :admission_test_hook) do
      {pid, ref} when is_pid(pid) and is_reference(ref) ->
        send(pid, {ref, :enqueued, request_ref, class, probe_op, caller_pid})

      _ ->
        :ok
    end
  end

  defp notify_test_hook(event, request_ref, class, probe_op) do
    case Application.get_env(:elixir_db, :admission_test_hook) do
      {pid, ref} when is_pid(pid) and is_reference(ref) ->
        send(pid, {ref, event, request_ref, class, probe_op})

      _ ->
        :ok
    end
  end

  defp record_waiter_outcome(uuid, %Waiter{} = waiter, outcome, queue_depth_at_grant) do
    record_admission_wait(
      uuid,
      waiter.class,
      outcome,
      wait_duration_native(waiter.enqueued_at_ms, System.monotonic_time(:millisecond)),
      waiter.queue_depth_at_enqueue,
      queue_depth_at_grant
    )
  end

  defp record_admission_wait(
         uuid,
         class,
         outcome,
         duration,
         queue_depth_at_enqueue,
         queue_depth_at_grant
       ) do
    DatabaseInstrumentation.admission_wait(
      uuid,
      class,
      outcome,
      duration,
      queue_depth_at_enqueue,
      queue_depth_at_grant
    )
  end

  defp wait_duration_native(enqueued_at_ms, now_ms)
       when is_integer(enqueued_at_ms) and is_integer(now_ms) do
    wait_ms = max(0, now_ms - enqueued_at_ms)
    System.convert_time_unit(wait_ms, :millisecond, :native)
  end
end
