defmodule ElixirDB.Runtime.ReadPool do
  @moduledoc """
  Bounded FIFO scheduler for classified snapshot reads on one disk database.

  Occupancy is `active_snapshots + queued_reads` and never exceeds
  `read_pool_size + read_queue_limit`. Overflow is the existing retryable
  `database_overloaded` error. Writes do not wait on this queue.
  """
  use GenServer

  alias ElixirDB.Error
  alias ElixirDB.Observability.Instrumentation.Database, as: DatabaseInstrumentation
  alias ElixirDB.Runtime.{CommandContext, Deadline, ServiceClass}

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
      :cancelled?
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
            cancelled?: boolean()
          }
  end

  def start_link({uuid, pool_size, queue_limit}),
    do: GenServer.start_link(__MODULE__, {uuid, pool_size, queue_limit}, name: via(uuid))

  def via(uuid),
    do: {:via, Registry, {ElixirDB.Runtime.DatabaseRegistry, {:read_pool, uuid}, :disabled}}

  @spec enabled?(binary()) :: boolean()
  def enabled?(uuid) when is_binary(uuid) do
    match?(
      [{_pid, :enabled}],
      Registry.lookup(ElixirDB.Runtime.DatabaseRegistry, {:read_pool, uuid})
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

  @spec register(binary(), pid()) :: :ok | {:error, Error.t()}
  def register(uuid, worker_pid) when is_binary(uuid) and is_pid(worker_pid) do
    call(uuid, {:register, worker_pid}, 5_000)
  end

  @spec complete(binary(), pid(), Job.t(), term()) :: :ok
  def complete(uuid, worker_pid, %Job{} = job, result)
      when is_binary(uuid) and is_pid(worker_pid) do
    GenServer.cast(via(uuid), {:complete, worker_pid, job, result})
  end

  @spec begin_close(binary()) :: :ok | {:error, Error.t()}
  def begin_close(uuid) when is_binary(uuid) do
    case Registry.lookup(ElixirDB.Runtime.DatabaseRegistry, {:read_pool, uuid}) do
      [{_pid, _}] -> call(uuid, :begin_close, ElixirDB.Config.shutdown_timeout())
      [] -> :ok
    end
  end

  @spec close_readers(binary()) :: :ok | {:error, Error.t()}
  def close_readers(uuid) when is_binary(uuid) do
    case Registry.lookup(ElixirDB.Runtime.DatabaseRegistry, {:read_pool, uuid}) do
      [{_pid, _}] -> call(uuid, :close_readers, ElixirDB.Config.shutdown_timeout())
      [] -> :ok
    end
  end

  @spec quiesce(binary(), Deadline.t()) :: :ok | {:error, Error.t()}
  def quiesce(uuid, :infinity) when is_binary(uuid), do: do_quiesce(uuid, :infinity)

  def quiesce(uuid, deadline_ms) when is_binary(uuid) and is_integer(deadline_ms) do
    do_quiesce(uuid, deadline_ms)
  end

  @spec resume(binary()) :: :ok | {:error, Error.t()}
  def resume(uuid) when is_binary(uuid) do
    case Registry.lookup(ElixirDB.Runtime.DatabaseRegistry, {:read_pool, uuid}) do
      [{_pid, _}] -> call(uuid, :resume, 5_000)
      [] -> :ok
    end
  end

  @spec with_quiesce(binary(), Deadline.t(), (-> term())) :: term() | {:error, Error.t()}
  def with_quiesce(uuid, deadline, fun) when is_binary(uuid) and is_function(fun, 0) do
    started = System.monotonic_time()
    result = quiesce(uuid, deadline)

    DatabaseInstrumentation.read_pool_quiesce(
      uuid,
      System.monotonic_time() - started
    )

    case result do
      :ok ->
        try do
          fun.()
        after
          _ = resume(uuid)
        end

      {:error, _} = error ->
        _ = resume(uuid)
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
       closing?: false,
       close_from: nil,
       quiesce_count: 0,
       quiesce_froms: []
     }}
  end

  @impl true
  def handle_call({:register, worker_pid}, _from, state) when is_pid(worker_pid) do
    {:reply, :ok, register_worker(state, worker_pid)}
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

  def handle_call(:quiesce, from, state) do
    state = %{state | quiesce_count: state.quiesce_count + 1}

    if map_size(state.busy) == 0 do
      {:reply, :ok, state}
    else
      {:noreply, %{state | quiesce_froms: [from | state.quiesce_froms]}}
    end
  end

  def handle_call(:resume, _from, state) do
    count = max(state.quiesce_count - 1, 0)
    state = %{state | quiesce_count: count}
    {:reply, :ok, maybe_resume_grants(state)}
  end

  def handle_call(:close_readers, _from, state) do
    pids = worker_pids(state)

    Enum.each(pids, fn pid ->
      if Process.alive?(pid) do
        GenServer.call(pid, :close_reader, ElixirDB.Config.shutdown_timeout())
      end
    end)

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
        quiescing?: state.quiesce_count > 0
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

  @impl true
  def handle_cast({:complete, worker_pid, %Job{} = job, result}, state) do
    {:noreply, complete_job(state, worker_pid, job, result)}
  end

  @impl true
  def handle_info({:DOWN, monitor_ref, :process, pid, _reason}, state) do
    if Map.has_key?(state.worker_monitors, monitor_ref) do
      {:noreply, worker_down(state, monitor_ref, pid)}
    else
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
    case Registry.lookup(ElixirDB.Runtime.DatabaseRegistry, {:read_pool, uuid}) do
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

  defp do_quiesce(uuid, deadline_ms) do
    if Deadline.exhausted?(deadline_ms) do
      {:error, deadline_error()}
    else
      case Registry.lookup(ElixirDB.Runtime.DatabaseRegistry, {:read_pool, uuid}) do
        [{_pid, _}] ->
          try do
            call(uuid, :quiesce, Deadline.call_timeout(deadline_ms))
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

    job = %Job{
      request_ref: waiter.request_ref,
      from: waiter.from,
      class: waiter.class,
      command: waiter.command,
      authority: waiter.authority,
      deadline_ms: waiter.deadline_ms,
      probe_op: waiter.probe_op,
      trace_context: waiter.trace_context,
      cancelled?: false
    }

    record_wait(state.uuid, waiter, :granted, queued_count(state))
    probe_grant(waiter.class, waiter.probe_op)
    GenServer.cast(worker_pid, {:run, job})

    %{state | busy: Map.put(state.busy, worker_pid, job)}
  end

  defp enqueue(state, %Waiter{} = waiter) do
    %{
      state
      | waiters: :queue.in(waiter.request_ref, state.waiters),
        waiting_by_ref: Map.put(state.waiting_by_ref, waiter.request_ref, waiter)
    }
  end

  defp complete_job(state, worker_pid, %Job{}, result) do
    case Map.pop(state.busy, worker_pid) do
      {nil, _} ->
        recycle_worker(state, worker_pid)

      {%Job{} = active, busy} ->
        unless active.cancelled? do
          GenServer.reply(active.from, result)
        end

        probe_release(active.probe_op)

        %{state | busy: busy}
        |> recycle_worker(worker_pid)
        |> maybe_finish_drain()
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
            {waiter, %{state | waiters: waiters, waiting_by_ref: waiting_by_ref}}
        end
    end
  end

  defp register_worker(state, worker_pid) do
    monitor_ref = Process.monitor(worker_pid)

    state
    |> Map.update!(:idle, &:queue.in(worker_pid, &1))
    |> Map.update!(:worker_monitors, &Map.put(&1, monitor_ref, worker_pid))
    |> mark_registry(:enabled)
    |> grant_loop()
  end

  defp worker_down(state, monitor_ref, pid) do
    monitors = Map.delete(state.worker_monitors, monitor_ref)
    idle = dequeue_pid(state.idle, pid)

    state = %{state | worker_monitors: monitors, idle: idle}

    case Map.pop(state.busy, pid) do
      {nil, busy} ->
        %{state | busy: busy}
        |> maybe_disable()
        |> maybe_finish_drain()

      {%Job{} = job, busy} ->
        unless job.cancelled? do
          GenServer.reply(
            job.from,
            {:error, Error.internal_error("read worker exited before completing the snapshot")}
          )
        end

        probe_release(job.probe_op)

        %{state | busy: busy}
        |> maybe_disable()
        |> maybe_finish_drain()
    end
  end

  defp caller_down(state, monitor_ref, _pid) do
    case waiter_by_monitor(state, monitor_ref) do
      %Waiter{} = waiter ->
        Process.demonitor(monitor_ref, [:flush])
        remove_waiter(state, waiter.request_ref)

      nil ->
        state
    end
  end

  defp cancel_request(state, request_ref) do
    case Map.get(state.waiting_by_ref, request_ref) do
      %Waiter{} = waiter ->
        Process.demonitor(waiter.monitor_ref, [:flush])
        remove_waiter(state, request_ref)

      nil ->
        mark_busy_cancelled(state, request_ref)
    end
  end

  defp mark_busy_cancelled(state, request_ref) do
    busy =
      Enum.reduce(state.busy, %{}, fn {pid, %Job{} = job}, acc ->
        if job.request_ref == request_ref,
          do: Map.put(acc, pid, %{job | cancelled?: true}),
          else: Map.put(acc, pid, job)
      end)

    %{state | busy: busy}
  end

  defp fail_waiters(state) do
    Enum.each(Map.values(state.waiting_by_ref), fn %Waiter{} = waiter ->
      Process.demonitor(waiter.monitor_ref, [:flush])
      record_wait(state.uuid, waiter, :closed, queued_count(state))
      GenServer.reply(waiter.from, {:error, closed_error()})
    end)

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

  defp maybe_finish_quiesce(state)
       when state.quiesce_count > 0 and map_size(state.busy) == 0 do
    Enum.each(state.quiesce_froms, &GenServer.reply(&1, :ok))
    %{state | quiesce_froms: []}
  end

  defp maybe_finish_quiesce(state), do: state

  defp maybe_resume_grants(state) do
    if paused?(state), do: state, else: grant_loop(state)
  end

  defp paused?(state), do: state.closing? or state.quiesce_count > 0

  defp maybe_disable(state) do
    if :queue.is_empty(state.idle) and map_size(state.busy) == 0,
      do: mark_registry(state, :disabled),
      else: state
  end

  defp mark_registry(state, value) do
    _ =
      Registry.update_value(
        ElixirDB.Runtime.DatabaseRegistry,
        {:read_pool, uuid_key(state)},
        fn _ -> value end
      )

    state
  end

  defp uuid_key(%{uuid: uuid}), do: uuid

  defp remove_waiter(state, request_ref) do
    waiters =
      :queue.filter(fn queued_ref -> queued_ref != request_ref end, state.waiters)

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

  defp busy_ref?(state, request_ref) do
    Enum.any?(state.busy, fn {_pid, %Job{request_ref: ref}} -> ref == request_ref end)
  end

  defp unwrap_authority({:command_context, %CommandContext{} = authority, command}),
    do: {authority, command}

  defp unwrap_authority(command), do: {CommandContext.public(), command}

  defp probe_op_from_command({:command, op, _}) when is_atom(op), do: op
  defp probe_op_from_command(_), do: nil

  defp probe_grant(class, probe_op) do
    case Application.get_env(:elixir_db, :read_pool_probe) do
      {pid, ref} when is_pid(pid) and is_reference(ref) ->
        send(pid, {ref, :read_pool_grant, class, probe_op})

      _ ->
        :ok
    end
  end

  defp probe_release(probe_op) do
    case Application.get_env(:elixir_db, :read_pool_probe) do
      {pid, ref} when is_pid(pid) and is_reference(ref) ->
        send(pid, {ref, :read_pool_release, probe_op})

      _ ->
        :ok
    end
  end

  defp record_wait(_uuid, _waiter, _outcome, _depth_at_grant), do: :ok

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
