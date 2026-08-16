defmodule VialKeeper.Runtime.ReadPool do
  @moduledoc """
  Bounded FIFO scheduler for classified snapshot reads on one disk database.

  Occupancy is `active_snapshots + queued_reads` and never exceeds
  `read_pool_size + read_queue_limit`. Overflow is the existing retryable
  `database_overloaded` error. Writes do not wait on this queue.
  """
  use GenServer

  alias VialKeeper.Deadline
  alias VialKeeper.Error
  alias VialKeeper.Observability.Instrumentation.Database, as: DatabaseInstrumentation
  alias VialKeeper.Runtime.{CommandContext, ServiceClass}

  defmodule Waiter do
    @moduledoc "Internal FIFO wait-queue entry tracked by `ReadPool`."
    @enforce_keys [
      :request_ref,
      :from,
      :caller_pid,
      :monitor_ref,
      :class,
      :command,
      :authority,
      :deadline_ms,
      :enqueued_at_ms,
      :queue_depth_at_enqueue,
      :probe_op,
      :trace_context
    ]
    defstruct @enforce_keys

    @type t :: %__MODULE__{
            request_ref: reference(),
            from: GenServer.from(),
            caller_pid: pid(),
            monitor_ref: reference(),
            class: ServiceClass.t(),
            command: term(),
            authority: CommandContext.t(),
            deadline_ms: Deadline.t(),
            enqueued_at_ms: integer(),
            queue_depth_at_enqueue: non_neg_integer(),
            probe_op: term() | nil,
            trace_context: term()
          }
  end

  defmodule Job do
    @moduledoc "Work assigned to one `ReadWorker`."
    @enforce_keys [
      :request_ref,
      :from,
      :class,
      :command,
      :authority,
      :deadline_ms,
      :probe_op,
      :trace_context,
      :cancelled?,
      :timer_ref
    ]
    defstruct @enforce_keys

    @type t :: %__MODULE__{
            request_ref: reference(),
            from: GenServer.from(),
            class: ServiceClass.t(),
            command: term(),
            authority: CommandContext.t(),
            deadline_ms: Deadline.t(),
            probe_op: term() | nil,
            trace_context: term(),
            cancelled?: boolean(),
            timer_ref: reference() | nil
          }
  end

  def start_link({uuid, pool_size, queue_limit}),
    do: GenServer.start_link(__MODULE__, {uuid, pool_size, queue_limit}, name: via(uuid))

  def via(uuid),
    do: {:via, Registry, {VialKeeper.Runtime.DatabaseRegistry, {:read_pool, uuid}, :disabled}}

  @spec enabled?(binary()) :: boolean()
  def enabled?(uuid) when is_binary(uuid) do
    match?(
      [{_pid, :enabled}],
      Registry.lookup(VialKeeper.Runtime.DatabaseRegistry, {:read_pool, uuid})
    )
  end

  @spec execute(binary(), ServiceClass.t(), term(), Deadline.t()) :: term() | {:error, Error.t()}
  def execute(uuid, class, command, :infinity) when is_binary(uuid) do
    do_execute(uuid, class, command, :infinity)
  end

  def execute(uuid, class, command, deadline_ms)
      when is_binary(uuid) and is_integer(deadline_ms) do
    do_execute(uuid, class, command, deadline_ms)
  end

  @spec register(binary(), pid(), (-> :ok | :unsupported)) :: :ok | {:error, Error.t()}
  def register(uuid, worker_pid, interrupt_fun)
      when is_binary(uuid) and is_pid(worker_pid) and is_function(interrupt_fun, 0) do
    call(uuid, {:register, worker_pid, interrupt_fun}, 5_000)
  end

  @spec complete(binary(), pid(), Job.t()) :: :reply | :discard | {:error, Error.t()}
  def complete(uuid, worker_pid, %Job{} = job)
      when is_binary(uuid) and is_pid(worker_pid) do
    call(uuid, {:complete, worker_pid, job}, 5_000)
  end

  @spec begin_close(binary()) :: :ok | {:error, Error.t()}
  def begin_close(uuid) when is_binary(uuid) do
    case Registry.lookup(VialKeeper.Runtime.DatabaseRegistry, {:read_pool, uuid}) do
      [{_pid, _}] -> call(uuid, :begin_close, VialKeeper.Config.shutdown_timeout())
      [] -> :ok
    end
  end

  @spec cancel_close(binary()) :: :ok | {:error, Error.t()}
  def cancel_close(uuid) when is_binary(uuid) do
    case Registry.lookup(VialKeeper.Runtime.DatabaseRegistry, {:read_pool, uuid}) do
      [{_pid, _}] -> call(uuid, :cancel_close, VialKeeper.Config.shutdown_timeout())
      [] -> :ok
    end
  end

  @spec close_readers(binary()) :: :ok | {:error, Error.t()}
  def close_readers(uuid) when is_binary(uuid) do
    case Registry.lookup(VialKeeper.Runtime.DatabaseRegistry, {:read_pool, uuid}) do
      [{_pid, _}] -> call(uuid, :close_readers, VialKeeper.Config.shutdown_timeout())
      [] -> :ok
    end
  end

  @spec quiesce(binary(), reference(), Deadline.t()) :: :ok | {:error, Error.t()}
  def quiesce(uuid, token, :infinity) when is_binary(uuid) and is_reference(token),
    do: do_quiesce(uuid, token, :infinity)

  def quiesce(uuid, token, deadline_ms)
      when is_binary(uuid) and is_reference(token) and is_integer(deadline_ms) do
    do_quiesce(uuid, token, deadline_ms)
  end

  @spec resume(binary(), reference()) :: :ok | {:error, Error.t()}
  def resume(uuid, token) when is_binary(uuid) and is_reference(token) do
    case Registry.lookup(VialKeeper.Runtime.DatabaseRegistry, {:read_pool, uuid}) do
      [{_pid, _}] -> call(uuid, {:resume, token}, 5_000)
      [] -> :ok
    end
  end

  @spec with_quiesce(binary(), Deadline.t(), (-> term())) :: term() | {:error, Error.t()}
  def with_quiesce(uuid, deadline, fun) when is_binary(uuid) and is_function(fun, 0) do
    token = make_ref()
    started = System.monotonic_time()
    result = quiesce(uuid, token, deadline)

    DatabaseInstrumentation.read_pool_quiesce(
      uuid,
      System.monotonic_time() - started
    )

    case result do
      :ok ->
        try do
          fun.()
        after
          _ = resume(uuid, token)
        end

      {:error, _} = error ->
        # A timed-out quiesce may still have registered the token in the pool
        # (its reply was dropped); same-sender ordering guarantees this resume
        # lands after that quiesce. Resuming an unknown token is a no-op.
        _ = resume(uuid, token)
        error
    end
  end

  @spec stats(binary()) :: {:ok, map()} | {:error, Error.t()}
  def stats(uuid) when is_binary(uuid), do: call(uuid, :stats, 5_000)

  @impl true
  def init({uuid, pool_size, queue_limit}) do
    {:ok,
     %{
       uuid: uuid,
       pool_size: pool_size,
       queue_limit: queue_limit,
       idle: :queue.new(),
       busy: %{},
       waiters: :queue.new(),
       waiting_by_ref: %{},
       worker_monitors: %{},
       interrupts: %{},
       closing?: false,
       close_from: nil,
       quiesce_tokens: %{},
       quiesce_froms: []
     }}
  end

  @impl true
  def handle_call({:register, worker_pid, interrupt_fun}, _from, state)
      when is_pid(worker_pid) and is_function(interrupt_fun, 0) do
    {:reply, :ok, register_worker(state, worker_pid, interrupt_fun)}
  end

  def handle_call(:begin_close, from, state) do
    state =
      state
      |> Map.put(:closing?, true)
      |> fail_waiters()
      |> mark_registry(:disabled)

    if map_size(state.busy) == 0 do
      {:reply, :ok, state}
    else
      {:noreply, %{state | close_from: from}}
    end
  end

  def handle_call(:cancel_close, _from, %{closing?: false, close_from: nil} = state) do
    {:reply, :ok, state}
  end

  def handle_call(:cancel_close, _from, state) do
    state =
      state
      |> maybe_reply_close_waiter()
      |> Map.put(:closing?, false)
      |> mark_registry(:enabled)
      |> maybe_resume_grants()

    {:reply, :ok, state}
  end

  def handle_call({:quiesce, token}, from, state) do
    # The caller is monitored so a killed exclusive cannot leave the pool
    # paused forever (an untrappable exit skips the caller's resume).
    monitor_ref = Process.monitor(caller_pid(from))
    state = %{state | quiesce_tokens: Map.put(state.quiesce_tokens, token, monitor_ref)}

    if map_size(state.busy) == 0 do
      {:reply, :ok, state}
    else
      {:noreply, %{state | quiesce_froms: [{token, from} | state.quiesce_froms]}}
    end
  end

  def handle_call({:resume, token}, _from, state) do
    state = clear_quiesce_token(state, token)
    {:reply, :ok, maybe_resume_grants(state)}
  end

  def handle_call(:close_readers, _from, state) do
    timeout = VialKeeper.Config.shutdown_timeout()
    Enum.each(worker_pids(state), &close_worker_reader(&1, timeout))

    {:reply, :ok, state}
  end

  def handle_call(:stats, _from, state) do
    {:reply,
     {:ok,
      %{
        active: map_size(state.busy),
        queued: queued_count(state),
        workers: worker_count(state),
        closing?: state.closing?,
        quiescing?: map_size(state.quiesce_tokens) > 0
      }}, state}
  end

  def handle_call({:execute, request_ref, class, command, deadline_ms, trace_context}, from, state) do
    {authority, inner} = unwrap_authority(command)
    probe_op = probe_op_from_command(inner)

    cond do
      state.closing? ->
        {:reply, {:error, closed_error()}, state}

      Map.has_key?(state.waiting_by_ref, request_ref) or busy_ref?(state, request_ref) ->
        {:reply, {:error, Error.invalid_request("duplicate read pool request ref")}, state}

      not ServiceClass.valid?(class) ->
        {:reply, {:error, Error.invalid_request("invalid admission service class")}, state}

      Deadline.exhausted?(deadline_ms) ->
        {:reply, {:error, deadline_error()}, state}

      true ->
        waiter = %Waiter{
          request_ref: request_ref,
          from: from,
          caller_pid: caller_pid(from),
          monitor_ref: Process.monitor(caller_pid(from)),
          class: class,
          command: inner,
          authority: authority,
          deadline_ms: deadline_ms,
          enqueued_at_ms: System.monotonic_time(:millisecond),
          queue_depth_at_enqueue: queued_count(state) + 1,
          probe_op: probe_op,
          trace_context: trace_context
        }

        {:noreply, admit(state, waiter)}
    end
  end

  def handle_call({:cancel, request_ref}, _from, state) do
    {:reply, :ok, cancel_request(state, request_ref)}
  end

  def handle_call({:complete, worker_pid, %Job{} = job}, _from, state) do
    {reply, state} = complete_job(state, worker_pid, job)
    {:reply, reply, state}
  end

  @impl true
  def handle_info({:interrupt_expired, worker_pid, request_ref}, state) do
    case Map.get(state.busy, worker_pid) do
      %Job{request_ref: ^request_ref} ->
        interrupt_worker(state, worker_pid)

      _ ->
        state
    end
    |> then(&{:noreply, &1})
  end

  @impl true
  def handle_info({:DOWN, monitor_ref, :process, pid, _reason}, state) do
    cond do
      Map.has_key?(state.worker_monitors, monitor_ref) ->
        {:noreply, worker_down(state, monitor_ref, pid)}

      quiesce_monitor?(state, monitor_ref) ->
        {:noreply, quiesce_caller_down(state, monitor_ref)}

      true ->
        {:noreply, caller_down(state, monitor_ref, pid)}
    end
  end

  defp do_execute(uuid, class, command, deadline_ms) do
    unless ServiceClass.valid?(class),
      do: raise(ArgumentError, "invalid service class #{inspect(class)}")

    if Deadline.exhausted?(deadline_ms) do
      {:error, deadline_error()}
    else
      request_ref = make_ref()
      trace_context = OpenTelemetry.Ctx.get_current()

      try do
        call(
          uuid,
          {:execute, request_ref, class, command, deadline_ms, trace_context},
          Deadline.call_timeout(deadline_ms)
        )
      catch
        :exit, reason ->
          _ = cancel(uuid, request_ref)
          translate_exit(reason)
      end
    end
  end

  defp cancel(uuid, request_ref) do
    call(uuid, {:cancel, request_ref}, 5_000)
  end

  defp call(uuid, message, timeout) do
    case Registry.lookup(VialKeeper.Runtime.DatabaseRegistry, {:read_pool, uuid}) do
      [{pid, _}] ->
        try do
          GenServer.call(pid, message, timeout)
        catch
          :exit, reason ->
            if Deadline.no_process_exit?(reason), do: {:error, closed_error()}, else: exit(reason)
        end

      [] ->
        {:error, closed_error()}
    end
  end

  defp translate_exit(reason) do
    cond do
      Deadline.genserver_call_timeout?(reason) ->
        {:error, deadline_error()}

      Deadline.no_process_exit?(reason) ->
        {:error, closed_error()}

      true ->
        exit(reason)
    end
  end

  defp do_quiesce(uuid, token, deadline_ms) do
    if Deadline.exhausted?(deadline_ms) do
      {:error, deadline_error()}
    else
      case Registry.lookup(VialKeeper.Runtime.DatabaseRegistry, {:read_pool, uuid}) do
        [{_pid, _}] ->
          try do
            call(uuid, {:quiesce, token}, Deadline.call_timeout(deadline_ms))
          catch
            :exit, reason -> translate_exit(reason)
          end

        [] ->
          :ok
      end
    end
  end

  defp admit(state, %Waiter{} = waiter) do
    cond do
      not paused?(state) and not :queue.is_empty(state.idle) ->
        grant_idle(state, waiter)

      queued_count(state) < state.queue_limit ->
        enqueue(state, waiter)

      true ->
        Process.demonitor(waiter.monitor_ref, [:flush])
        DatabaseInstrumentation.overload(state.uuid)

        DatabaseInstrumentation.read_pool_wait(
          state.uuid,
          waiter.class,
          :rejected,
          0,
          queued_count(state),
          0
        )

        GenServer.reply(waiter.from, {:error, overloaded_error()})
        state
    end
  end

  defp grant_idle(state, %Waiter{} = waiter) do
    {{:value, worker_pid}, idle} = :queue.out(state.idle)
    grant(%{state | idle: idle}, worker_pid, waiter)
  end

  defp grant(state, worker_pid, %Waiter{} = waiter) do
    Process.demonitor(waiter.monitor_ref, [:flush])
    timer_ref = schedule_interrupt(worker_pid, waiter)

    job = %Job{
      request_ref: waiter.request_ref,
      from: waiter.from,
      class: waiter.class,
      command: waiter.command,
      authority: waiter.authority,
      deadline_ms: waiter.deadline_ms,
      probe_op: waiter.probe_op,
      trace_context: waiter.trace_context,
      cancelled?: false,
      timer_ref: timer_ref
    }

    record_wait(state.uuid, waiter, :granted, queued_count(state))
    DatabaseInstrumentation.read_pool_active(state.uuid, 1)
    probe_grant(waiter.class, waiter.probe_op)
    GenServer.cast(worker_pid, {:run, job})

    %{state | busy: Map.put(state.busy, worker_pid, job)}
  end

  defp schedule_interrupt(_worker_pid, %Waiter{deadline_ms: :infinity}), do: nil

  defp schedule_interrupt(worker_pid, %Waiter{} = waiter) do
    Process.send_after(
      self(),
      {:interrupt_expired, worker_pid, waiter.request_ref},
      Deadline.remaining(waiter.deadline_ms)
    )
  end

  defp enqueue(state, %Waiter{} = waiter) do
    DatabaseInstrumentation.read_pool_queued(state.uuid, 1)

    %{
      state
      | waiters: :queue.in(waiter.request_ref, state.waiters),
        waiting_by_ref: Map.put(state.waiting_by_ref, waiter.request_ref, waiter)
    }
  end

  defp complete_job(state, worker_pid, %Job{}) do
    case Map.pop(state.busy, worker_pid) do
      {nil, _} ->
        {:discard, recycle_worker(state, worker_pid)}

      {%Job{} = active, busy} ->
        cancel_timer(active.timer_ref)
        DatabaseInstrumentation.read_pool_active(state.uuid, -1)
        probe_release(active.probe_op)

        reply = if active.cancelled?, do: :discard, else: :reply

        state =
          %{state | busy: busy}
          |> recycle_worker(worker_pid)
          |> maybe_finish_drain()

        {reply, state}
    end
  end

  defp recycle_worker(state, worker_pid) do
    if Process.alive?(worker_pid) do
      grant_loop(%{state | idle: :queue.in(worker_pid, state.idle)})
    else
      grant_loop(state)
    end
  end

  defp grant_loop(state) do
    cond do
      paused?(state) ->
        state

      :queue.is_empty(state.idle) ->
        state

      :queue.is_empty(state.waiters) ->
        state

      true ->
        case pop_waiter(state) do
          {nil, state} ->
            state

          {%Waiter{} = waiter, state} ->
            state
            |> grant_idle(waiter)
            |> grant_loop()
        end
    end
  end

  defp pop_waiter(state) do
    case :queue.out(state.waiters) do
      {:empty, waiters} ->
        {nil, %{state | waiters: waiters}}

      {{:value, request_ref}, waiters} ->
        case Map.pop(state.waiting_by_ref, request_ref) do
          {nil, waiting_by_ref} ->
            pop_waiter(%{state | waiters: waiters, waiting_by_ref: waiting_by_ref})

          {%Waiter{} = waiter, waiting_by_ref} ->
            DatabaseInstrumentation.read_pool_queued(state.uuid, -1)
            {waiter, %{state | waiters: waiters, waiting_by_ref: waiting_by_ref}}
        end
    end
  end

  defp register_worker(state, worker_pid, interrupt_fun) do
    monitor_ref = Process.monitor(worker_pid)

    state
    |> Map.update!(:idle, &:queue.in(worker_pid, &1))
    |> Map.update!(:worker_monitors, &Map.put(&1, monitor_ref, worker_pid))
    |> Map.update!(:interrupts, &Map.put(&1, worker_pid, interrupt_fun))
    |> mark_registry(:enabled)
    |> grant_loop()
  end

  defp worker_down(state, monitor_ref, pid) do
    monitors = Map.delete(state.worker_monitors, monitor_ref)
    idle = dequeue_pid(state.idle, pid)

    state = %{state | worker_monitors: monitors, idle: idle}

    case Map.pop(state.busy, pid) do
      {nil, busy} ->
        %{state | busy: busy, interrupts: Map.delete(state.interrupts, pid)}
        |> maybe_disable()
        |> maybe_finish_drain()

      {%Job{} = job, busy} ->
        cancel_timer(job.timer_ref)

        unless job.cancelled? do
          GenServer.reply(
            job.from,
            {:error, Error.internal_error("read worker exited before completing the snapshot")}
          )
        end

        DatabaseInstrumentation.read_pool_active(state.uuid, -1)
        probe_release(job.probe_op)

        %{state | busy: busy, interrupts: Map.delete(state.interrupts, pid)}
        |> maybe_disable()
        |> maybe_finish_drain()
    end
  end

  defp caller_down(state, monitor_ref, _pid) do
    case waiter_by_monitor(state, monitor_ref) do
      %Waiter{} = waiter ->
        Process.demonitor(monitor_ref, [:flush])
        record_wait(state.uuid, waiter, :cancelled, queued_count(state))
        remove_waiter(state, waiter.request_ref)

      nil ->
        state
    end
  end

  defp cancel_request(state, request_ref) do
    case Map.get(state.waiting_by_ref, request_ref) do
      %Waiter{} = waiter ->
        Process.demonitor(waiter.monitor_ref, [:flush])
        record_wait(state.uuid, waiter, :cancelled, queued_count(state))
        remove_waiter(state, request_ref)

      nil ->
        mark_busy_cancelled(state, request_ref)
    end
  end

  defp mark_busy_cancelled(state, request_ref) do
    Enum.reduce(state.busy, state, fn {pid, %Job{} = job}, acc ->
      if job.request_ref == request_ref do
        interrupt_worker(%{acc | busy: Map.put(acc.busy, pid, %{job | cancelled?: true})}, pid)
      else
        acc
      end
    end)
  end

  defp fail_waiters(state) do
    Enum.each(Map.values(state.waiting_by_ref), fn %Waiter{} = waiter ->
      Process.demonitor(waiter.monitor_ref, [:flush])
      record_wait(state.uuid, waiter, :closed, queued_count(state))
      GenServer.reply(waiter.from, {:error, closed_error()})
    end)

    if map_size(state.waiting_by_ref) > 0 do
      DatabaseInstrumentation.read_pool_queued(state.uuid, -map_size(state.waiting_by_ref))
    end

    %{state | waiters: :queue.new(), waiting_by_ref: %{}}
  end

  defp maybe_finish_drain(state) do
    state
    |> maybe_finish_close()
    |> maybe_finish_quiesce()
  end

  defp maybe_finish_close(%{closing?: true, close_from: from} = state)
       when not is_nil(from) and map_size(state.busy) == 0 do
    GenServer.reply(from, :ok)
    %{state | close_from: nil}
  end

  defp maybe_finish_close(state), do: state

  defp maybe_reply_close_waiter(%{close_from: from} = state) when not is_nil(from) do
    GenServer.reply(from, :ok)
    %{state | close_from: nil}
  end

  defp maybe_reply_close_waiter(state), do: state

  defp maybe_finish_quiesce(%{quiesce_froms: [_ | _] = froms} = state)
       when map_size(state.busy) == 0 do
    Enum.each(froms, fn {_token, from} -> GenServer.reply(from, :ok) end)
    %{state | quiesce_froms: []}
  end

  defp maybe_finish_quiesce(state), do: state

  defp maybe_resume_grants(state) do
    if paused?(state), do: state, else: grant_loop(state)
  end

  defp clear_quiesce_token(state, token) do
    case Map.pop(state.quiesce_tokens, token) do
      {nil, _tokens} ->
        state

      {monitor_ref, tokens} ->
        Process.demonitor(monitor_ref, [:flush])

        %{
          state
          | quiesce_tokens: tokens,
            quiesce_froms: Enum.reject(state.quiesce_froms, fn {t, _from} -> t == token end)
        }
    end
  end

  defp quiesce_monitor?(state, monitor_ref) do
    Enum.any?(state.quiesce_tokens, fn {_token, ref} -> ref == monitor_ref end)
  end

  defp quiesce_caller_down(state, monitor_ref) do
    {token, _ref} =
      Enum.find(state.quiesce_tokens, fn {_token, ref} -> ref == monitor_ref end)

    %{
      state
      | quiesce_tokens: Map.delete(state.quiesce_tokens, token),
        quiesce_froms: Enum.reject(state.quiesce_froms, fn {t, _from} -> t == token end)
    }
    |> maybe_resume_grants()
  end

  defp paused?(state), do: state.closing? or map_size(state.quiesce_tokens) > 0

  defp maybe_disable(state) do
    if :queue.is_empty(state.idle) and map_size(state.busy) == 0,
      do: mark_registry(state, :disabled),
      else: state
  end

  defp mark_registry(state, value) do
    _ =
      Registry.update_value(
        VialKeeper.Runtime.DatabaseRegistry,
        {:read_pool, uuid_key(state)},
        fn _ -> value end
      )

    state
  end

  defp uuid_key(%{uuid: uuid}), do: uuid

  defp remove_waiter(state, request_ref) do
    waiters =
      :queue.filter(fn queued_ref -> queued_ref != request_ref end, state.waiters)

    DatabaseInstrumentation.read_pool_queued(state.uuid, -1)

    %{state | waiters: waiters, waiting_by_ref: Map.delete(state.waiting_by_ref, request_ref)}
  end

  defp waiter_by_monitor(state, monitor_ref) do
    Enum.find_value(Map.values(state.waiting_by_ref), fn
      %Waiter{monitor_ref: ^monitor_ref} = waiter -> waiter
      _ -> nil
    end)
  end

  defp dequeue_pid(queue, pid) do
    :queue.filter(fn queued -> queued != pid end, queue)
  end

  defp queued_count(state), do: map_size(state.waiting_by_ref)

  defp worker_count(state), do: :queue.len(state.idle) + map_size(state.busy)

  defp worker_pids(state) do
    :queue.to_list(state.idle) ++ Map.keys(state.busy)
  end

  defp close_worker_reader(pid, timeout) do
    # A worker may die between the liveness check and the call; its exit is
    # already handled by the DOWN monitor, so close failure here is benign.
    if Process.alive?(pid) do
      try do
        GenServer.call(pid, :close_reader, timeout)
      catch
        :exit, reason ->
          if Deadline.genserver_call_timeout?(reason), do: exit(reason), else: :ok
      end
    end

    :ok
  end

  defp busy_ref?(state, request_ref) do
    Enum.any?(state.busy, fn {_pid, %Job{request_ref: ref}} -> ref == request_ref end)
  end

  defp interrupt_worker(state, worker_pid) do
    case Map.get(state.interrupts, worker_pid) do
      interrupt_fun when is_function(interrupt_fun, 0) ->
        _ = interrupt_fun.()

      _ ->
        :ok
    end

    state
  end

  defp cancel_timer(nil), do: :ok
  defp cancel_timer(timer_ref), do: Process.cancel_timer(timer_ref)

  defp unwrap_authority({:command_context, %CommandContext{} = authority, command}),
    do: {authority, command}

  defp unwrap_authority(command), do: {CommandContext.public(), command}

  defp probe_op_from_command({:command, op, _}) when is_atom(op), do: op
  defp probe_op_from_command(_), do: nil

  defp probe_grant(class, probe_op) do
    case Application.get_env(:vial_keeper, :read_pool_probe) do
      {pid, ref} when is_pid(pid) and is_reference(ref) ->
        send(pid, {ref, :read_pool_grant, class, probe_op})

      _ ->
        :ok
    end
  end

  defp probe_release(probe_op) do
    case Application.get_env(:vial_keeper, :read_pool_probe) do
      {pid, ref} when is_pid(pid) and is_reference(ref) ->
        send(pid, {ref, :read_pool_release, probe_op})

      _ ->
        :ok
    end
  end

  defp record_wait(uuid, %Waiter{} = waiter, outcome, queue_depth_at_grant) do
    DatabaseInstrumentation.read_pool_wait(
      uuid,
      waiter.class,
      outcome,
      wait_duration_native(waiter.enqueued_at_ms, System.monotonic_time(:millisecond)),
      waiter.queue_depth_at_enqueue,
      queue_depth_at_grant
    )
  end

  defp wait_duration_native(enqueued_at_ms, now_ms)
       when is_integer(enqueued_at_ms) and is_integer(now_ms) do
    wait_ms = max(0, now_ms - enqueued_at_ms)
    System.convert_time_unit(wait_ms, :millisecond, :native)
  end

  defp caller_pid({pid, _tag}) when is_pid(pid), do: pid

  defp closed_error, do: Error.database_closed("database is closed")

  defp overloaded_error,
    do: Error.database_overloaded("database read pool queue is full")

  defp deadline_error do
    Error.new(
      :internal_error,
      "database command timed out",
      %{reason: :deadline_exhausted},
      retryable: true
    )
  end
end
