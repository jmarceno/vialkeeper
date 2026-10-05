defmodule VialKeeper.Runtime.WriterPool do
  @moduledoc """
  Schedules concurrent document writes for one database onto its writer slots.

  Occupancy is `active + queued` and never exceeds
  `effective_writers + write_queue_limit`; overflow is the existing retryable
  `database_overloaded` error. A queued write waiting for a busy document
  (held by a running write or an earlier queued one) does not count.

  Two writes that touch the same document never run at the same time. A
  queued write may start when an idle slot exists and its document ids
  intersect neither the ids held by running writes nor the ids of any write
  queued before it, so writes to one document run in arrival order.

  Serial and exclusive commands run behind a barrier (`with_quiesce/3`): no
  new write starts and running ones finish first. Every barrier end bumps the
  cache epoch, which tells every writer process to drop its per-connection
  caches before its next transaction.
  """
  use GenServer

  alias VialKeeper.Commands
  alias VialKeeper.Deadline
  alias VialKeeper.Error
  alias VialKeeper.Observability.Instrumentation.Database, as: DatabaseInstrumentation
  alias VialKeeper.Runtime.{CommandContext, MutationCommands, ServiceClass, WriteKeys}
  alias VialKeeper.Storage.BackendContext
  alias VialKeeper.Storage.Lifecycle

  defmodule Waiter do
    @moduledoc "Internal FIFO wait-queue entry tracked by `WriterPool`."
    @enforce_keys [
      :request_ref,
      :from,
      :monitor_ref,
      :class,
      :command,
      :authority,
      :document_ids,
      :deadline_ms,
      :trace_context
    ]
    defstruct @enforce_keys

    @type t :: %__MODULE__{
            request_ref: reference(),
            from: GenServer.from(),
            monitor_ref: reference(),
            class: ServiceClass.t(),
            command: struct(),
            authority: CommandContext.t(),
            document_ids: [binary()],
            deadline_ms: Deadline.t(),
            trace_context: term()
          }
  end

  defmodule Job do
    @moduledoc "Work assigned to one `WriterSlot`."
    @enforce_keys [
      :request_ref,
      :from,
      :command,
      :authority,
      :document_ids,
      :deadline_ms,
      :trace_context,
      :cancelled?
    ]
    defstruct @enforce_keys

    @type t :: %__MODULE__{
            request_ref: reference(),
            from: GenServer.from(),
            command: struct(),
            authority: CommandContext.t(),
            document_ids: [binary()],
            deadline_ms: Deadline.t(),
            trace_context: term(),
            cancelled?: boolean()
          }
  end

  @type args :: {binary(), pos_integer(), pos_integer()}

  @spec start_link(args()) :: GenServer.on_start()
  def start_link({uuid, effective_writers, queue_limit}),
    do: GenServer.start_link(__MODULE__, {uuid, effective_writers, queue_limit}, name: via(uuid))

  @spec via(binary()) :: {:via, module(), term()}
  def via(uuid),
    do:
      {:via, Registry,
       {VialKeeper.Runtime.DatabaseRegistry, {:writer_pool, uuid}, %{status: :disabled, epoch: nil}}}

  @doc "True when the database has a writer pool with at least one live slot."
  @spec enabled?(binary()) :: boolean()
  def enabled?(uuid) when is_binary(uuid), do: match?(%{status: :enabled}, registry_value(uuid))

  @doc """
  Returns the writer cache epoch. It changes at the end of every barrier; it
  is 0 for a database without a writer pool.
  """
  @spec cache_epoch(binary()) :: non_neg_integer()
  def cache_epoch(uuid) when is_binary(uuid) do
    case registry_value(uuid) do
      %{epoch: epoch} when epoch != nil -> :atomics.get(epoch, 1)
      _no_pool -> 0
    end
  end

  @doc """
  Drops a writer process's per-connection caches when the cache epoch moved
  past `seen_epoch`, and returns the current epoch to remember.
  """
  @spec refresh_writer_caches(binary(), BackendContext.t(), non_neg_integer()) ::
          non_neg_integer()
  def refresh_writer_caches(uuid, %BackendContext{} = context, seen_epoch) do
    case cache_epoch(uuid) do
      ^seen_epoch ->
        seen_epoch

      epoch ->
        :ok = Lifecycle.reset_writer_caches(context)
        epoch
    end
  end

  @doc """
  Runs a concurrent write command on a writer slot and returns its result
  once the write is visible in the changes feed.
  """
  @spec execute(binary(), ServiceClass.t(), term(), Deadline.t()) :: term() | {:error, Error.t()}
  def execute(uuid, class, command, deadline) when is_binary(uuid) do
    unless ServiceClass.valid?(class),
      do: raise(ArgumentError, "invalid service class #{inspect(class)}")

    {authority, normalized} = unwrap(command)

    cond do
      Deadline.exhausted?(deadline) -> {:error, deadline_error()}
      match?(%_{}, normalized) -> submit(uuid, class, authority, normalized, deadline)
      true -> {:error, Error.invalid_request("unknown database command")}
    end
  end

  @doc "Registers a writer slot that is ready to run jobs."
  @spec register(binary(), pid()) :: :ok | {:error, Error.t()}
  def register(uuid, slot_pid) when is_binary(uuid) and is_pid(slot_pid),
    do: call(uuid, {:register, slot_pid}, VialKeeper.Config.shutdown_timeout())

  @doc "Reports a finished job; `:reply` tells the slot to answer the caller."
  @spec complete(binary(), pid(), Job.t()) :: :reply | :discard | {:error, Error.t()}
  def complete(uuid, slot_pid, %Job{} = job) when is_binary(uuid) and is_pid(slot_pid),
    do: call(uuid, {:complete, slot_pid, job}, VialKeeper.Config.shutdown_timeout())

  @doc "Stops admitting writes, fails queued ones, and waits for running ones."
  @spec begin_close(binary()) :: :ok | {:error, Error.t()}
  def begin_close(uuid) when is_binary(uuid),
    do: call_if_running(uuid, :begin_close, VialKeeper.Config.shutdown_timeout())

  @doc "Undoes `begin_close/1` after an aborted close."
  @spec cancel_close(binary()) :: :ok | {:error, Error.t()}
  def cancel_close(uuid) when is_binary(uuid),
    do: call_if_running(uuid, :cancel_close, VialKeeper.Config.shutdown_timeout())

  @doc "Closes every slot's writer connection."
  @spec close_writers(binary()) :: :ok | {:error, Error.t()}
  def close_writers(uuid) when is_binary(uuid),
    do: call_if_running(uuid, :close_writers, VialKeeper.Config.shutdown_timeout())

  @doc "Pauses new writes under `token` and waits until running ones finish."
  @spec quiesce(binary(), reference(), Deadline.t()) :: :ok | {:error, Error.t()}
  def quiesce(uuid, token, deadline) when is_binary(uuid) and is_reference(token) do
    cond do
      Deadline.exhausted?(deadline) -> {:error, deadline_error()}
      registry_value(uuid) == nil -> :ok
      true -> call(uuid, {:quiesce, token}, Deadline.call_timeout(deadline))
    end
  end

  @doc "Ends the barrier held under `token` and bumps the cache epoch."
  @spec resume(binary(), reference()) :: :ok | {:error, Error.t()}
  def resume(uuid, token) when is_binary(uuid) and is_reference(token),
    do: call_if_running(uuid, {:resume, token}, VialKeeper.Config.shutdown_timeout())

  @doc "Runs `fun` behind a writer barrier (see `quiesce/3`)."
  @spec with_quiesce(binary(), Deadline.t(), (-> result)) :: result | {:error, Error.t()}
        when result: term()
  def with_quiesce(uuid, deadline, fun) when is_binary(uuid) and is_function(fun, 0) do
    token = make_ref()

    case quiesce(uuid, token, deadline) do
      :ok ->
        try do
          fun.()
        after
          _ = resume(uuid, token)
        end

      {:error, _} = error ->
        # A timed-out quiesce may still hold the token in the pool; same-sender
        # ordering makes this resume land after it. Unknown tokens are a no-op.
        _ = resume(uuid, token)
        error
    end
  end

  @spec stats(binary()) :: {:ok, map()} | {:error, Error.t()}
  def stats(uuid) when is_binary(uuid), do: call(uuid, :stats, VialKeeper.Config.shutdown_timeout())

  @impl true
  def init({uuid, effective_writers, queue_limit}) do
    epoch = :atomics.new(1, signed: false)

    state = %{
      uuid: uuid,
      effective_writers: effective_writers,
      queue_limit: queue_limit,
      epoch: epoch,
      idle: :queue.new(),
      busy: %{},
      held: MapSet.new(),
      waiters: :queue.new(),
      slot_monitors: %{},
      closing?: false,
      close_from: nil,
      quiesce_tokens: %{},
      quiesce_froms: []
    }

    {:ok, mark_registry(state, :disabled)}
  end

  @impl true
  def handle_call({:register, slot_pid}, _from, state) do
    monitor_ref = Process.monitor(slot_pid)

    state =
      %{
        state
        | idle: :queue.in(slot_pid, state.idle),
          slot_monitors: Map.put(state.slot_monitors, monitor_ref, slot_pid)
      }
      |> mark_registry(if(state.closing?, do: :disabled, else: :enabled))
      |> grant_loop()

    {:reply, :ok, state}
  end

  def handle_call({:execute, request_ref, waiter_fields}, from, state) do
    cond do
      state.closing? ->
        {:reply, {:error, closed_error()}, state}

      over_limit?(state, waiter_fields.document_ids) ->
        DatabaseInstrumentation.overload(state.uuid)
        {:reply, {:error, Error.database_overloaded("database write queue is full")}, state}

      true ->
        waiter =
          struct!(
            Waiter,
            Map.merge(waiter_fields, %{
              request_ref: request_ref,
              from: from,
              monitor_ref: Process.monitor(caller_pid(from))
            })
          )

        {:noreply, grant_loop(%{state | waiters: :queue.in(waiter, state.waiters)})}
    end
  end

  def handle_call({:cancel, request_ref}, _from, state),
    do: {:reply, :ok, cancel_request(state, request_ref)}

  def handle_call({:complete, slot_pid, %Job{}}, _from, state) do
    case Map.pop(state.busy, slot_pid) do
      {nil, _busy} ->
        {:reply, :discard, state |> recycle_slot(slot_pid) |> grant_loop()}

      {%Job{} = job, busy} ->
        state =
          %{state | busy: busy, held: release(state.held, job.document_ids)}
          |> recycle_slot(slot_pid)
          |> finish_drain()
          |> grant_loop()

        {:reply, if(job.cancelled?, do: :discard, else: :reply), state}
    end
  end

  def handle_call(:begin_close, from, state) do
    %{state | closing?: true}
    |> fail_waiters()
    |> mark_registry(:disabled)
    |> reply_when_idle(&%{&1 | close_from: from})
  end

  def handle_call(:cancel_close, _from, state) do
    state =
      %{reply_close_waiter(state) | closing?: false}
      |> mark_registry(if(slot_count(state) > 0, do: :enabled, else: :disabled))
      |> grant_loop()

    {:reply, :ok, state}
  end

  def handle_call(:close_writers, _from, state) do
    timeout = VialKeeper.Config.shutdown_timeout()
    Enum.each(slot_pids(state), &close_slot_writer(&1, timeout))
    {:reply, :ok, state}
  end

  def handle_call({:quiesce, token}, from, state) do
    # The caller is monitored so a killed barrier holder cannot pause the pool
    # forever (an untrappable exit skips its resume).
    monitor_ref = Process.monitor(caller_pid(from))

    %{state | quiesce_tokens: Map.put(state.quiesce_tokens, token, monitor_ref)}
    |> reply_when_idle(&%{&1 | quiesce_froms: [{token, from} | &1.quiesce_froms]})
  end

  def handle_call({:resume, token}, _from, state),
    do: {:reply, :ok, state |> end_barrier(token) |> grant_loop()}

  def handle_call(:stats, _from, state) do
    {:reply,
     {:ok,
      %{
        active: map_size(state.busy),
        queued: :queue.len(state.waiters),
        slots: slot_count(state),
        held_documents: MapSet.size(state.held),
        cache_epoch: :atomics.get(state.epoch, 1),
        closing?: state.closing?,
        quiescing?: map_size(state.quiesce_tokens) > 0
      }}, state}
  end

  @impl true
  def handle_info({:DOWN, monitor_ref, :process, pid, _reason}, state) do
    cond do
      Map.has_key?(state.slot_monitors, monitor_ref) ->
        {:noreply, slot_down(state, monitor_ref, pid)}

      token = quiesce_token(state, monitor_ref) ->
        {:noreply, state |> end_barrier(token) |> grant_loop()}

      true ->
        {:noreply, caller_down(state, monitor_ref)}
    end
  end

  # Answers a drain request now when no write runs; otherwise `park` records
  # the caller, answered by `finish_drain/1` when the last write finishes.
  defp reply_when_idle(%{busy: busy} = state, _park) when map_size(busy) == 0,
    do: {:reply, :ok, state}

  defp reply_when_idle(state, park), do: {:noreply, park.(state)}

  defp submit(uuid, class, authority, command, deadline) do
    request_ref = make_ref()

    waiter_fields = %{
      class: class,
      command: command,
      authority: authority,
      document_ids: WriteKeys.document_ids(command),
      deadline_ms: deadline,
      trace_context: OpenTelemetry.Ctx.get_current()
    }

    case call(uuid, {:execute, request_ref, waiter_fields}, Deadline.call_timeout(deadline)) do
      {:written, outcome} -> MutationCommands.finish(outcome, command, uuid, deadline)
      {:error, %Error{} = error} -> deadline_cancel(uuid, request_ref, error)
    end
  end

  # A caller that stopped waiting withdraws its request; a running write still
  # finishes and its result is discarded.
  defp deadline_cancel(uuid, request_ref, %Error{details: %{reason: :deadline_exhausted}} = error) do
    _ = call_if_running(uuid, {:cancel, request_ref}, VialKeeper.Config.shutdown_timeout())
    {:error, error}
  end

  defp deadline_cancel(_uuid, _request_ref, error), do: {:error, error}

  defp grant_loop(state) do
    if paused?(state) or :queue.is_empty(state.idle) do
      state
    else
      case pop_eligible(:queue.to_list(state.waiters), state.held) do
        {nil, _waiters} ->
          state

        {%Waiter{} = waiter, waiters} ->
          {{:value, slot_pid}, idle} = :queue.out(state.idle)

          %{state | idle: idle, waiters: :queue.from_list(waiters)}
          |> grant(slot_pid, waiter)
          |> grant_loop()
      end
    end
  end

  # FIFO per document: a waiter may start only when none of its documents is
  # held by a running write or wanted by a waiter queued before it.
  defp pop_eligible(waiters, held), do: pop_eligible(waiters, held, [])

  defp pop_eligible([], _blocked, _skipped), do: {nil, nil}

  defp pop_eligible([%Waiter{} = waiter | rest], blocked, skipped) do
    if Enum.any?(waiter.document_ids, &MapSet.member?(blocked, &1)) do
      pop_eligible(rest, Enum.into(waiter.document_ids, blocked), [waiter | skipped])
    else
      {waiter, Enum.reverse(skipped, rest)}
    end
  end

  defp grant(state, slot_pid, %Waiter{} = waiter) do
    Process.demonitor(waiter.monitor_ref, [:flush])
    probe_grant(waiter.class)

    job = %Job{
      request_ref: waiter.request_ref,
      from: waiter.from,
      command: waiter.command,
      authority: waiter.authority,
      document_ids: waiter.document_ids,
      deadline_ms: waiter.deadline_ms,
      trace_context: waiter.trace_context,
      cancelled?: false
    }

    GenServer.cast(slot_pid, {:run, job})

    %{
      state
      | busy: Map.put(state.busy, slot_pid, job),
        held: Enum.into(job.document_ids, state.held)
    }
  end

  # Writes waiting for a busy document hold no slot and add no load until the
  # document frees up, so only writes that could start count against the limit.
  defp over_limit?(state, document_ids) do
    {ready, blocked} =
      state.waiters
      |> :queue.to_list()
      |> Enum.reduce({0, state.held}, fn %Waiter{document_ids: ids}, {ready, blocked} ->
        ready = if Enum.any?(ids, &MapSet.member?(blocked, &1)), do: ready, else: ready + 1
        {ready, Enum.into(ids, blocked)}
      end)

    not Enum.any?(document_ids, &MapSet.member?(blocked, &1)) and
      map_size(state.busy) + ready >= state.effective_writers + state.queue_limit
  end

  defp release(held, document_ids), do: Enum.reduce(document_ids, held, &MapSet.delete(&2, &1))

  defp recycle_slot(state, slot_pid) do
    if Process.alive?(slot_pid) and not queued_slot?(state, slot_pid),
      do: %{state | idle: :queue.in(slot_pid, state.idle)},
      else: state
  end

  defp queued_slot?(state, slot_pid), do: :queue.member(slot_pid, state.idle)

  defp slot_down(state, monitor_ref, pid) do
    state = %{
      state
      | slot_monitors: Map.delete(state.slot_monitors, monitor_ref),
        idle: :queue.filter(&(&1 != pid), state.idle)
    }

    state =
      case Map.pop(state.busy, pid) do
        {nil, _busy} ->
          state

        {%Job{} = job, busy} ->
          unless job.cancelled? do
            GenServer.reply(
              job.from,
              {:error, Error.internal_error("writer slot exited before completing the write")}
            )
          end

          %{state | busy: busy, held: release(state.held, job.document_ids)}
      end

    state
    |> mark_registry(
      if(slot_count(state) > 0 and not state.closing?, do: :enabled, else: :disabled)
    )
    |> finish_drain()
    |> grant_loop()
  end

  defp caller_down(state, monitor_ref) do
    case Enum.split_with(:queue.to_list(state.waiters), &(&1.monitor_ref == monitor_ref)) do
      {[], _waiters} -> state
      {_gone, waiters} -> %{state | waiters: :queue.from_list(waiters)}
    end
  end

  defp cancel_request(state, request_ref) do
    case Enum.split_with(:queue.to_list(state.waiters), &(&1.request_ref == request_ref)) do
      {[%Waiter{} = waiter], waiters} ->
        Process.demonitor(waiter.monitor_ref, [:flush])
        %{state | waiters: :queue.from_list(waiters)} |> grant_loop()

      {[], _waiters} ->
        busy =
          Map.new(state.busy, fn
            {pid, %Job{request_ref: ^request_ref} = job} -> {pid, %{job | cancelled?: true}}
            entry -> entry
          end)

        %{state | busy: busy}
    end
  end

  defp fail_waiters(state) do
    Enum.each(:queue.to_list(state.waiters), fn %Waiter{} = waiter ->
      Process.demonitor(waiter.monitor_ref, [:flush])
      GenServer.reply(waiter.from, {:error, closed_error()})
    end)

    %{state | waiters: :queue.new()}
  end

  defp finish_drain(%{busy: busy} = state) when map_size(busy) > 0, do: state

  defp finish_drain(state) do
    Enum.each(state.quiesce_froms, fn {_token, from} -> GenServer.reply(from, :ok) end)
    reply_close_waiter(%{state | quiesce_froms: []})
  end

  defp reply_close_waiter(%{close_from: nil} = state), do: state

  defp reply_close_waiter(%{close_from: from} = state) do
    GenServer.reply(from, :ok)
    %{state | close_from: nil}
  end

  defp end_barrier(state, token) do
    case Map.pop(state.quiesce_tokens, token) do
      {nil, _tokens} ->
        state

      {monitor_ref, tokens} ->
        Process.demonitor(monitor_ref, [:flush])
        :ok = :atomics.add(state.epoch, 1, 1)

        %{
          state
          | quiesce_tokens: tokens,
            quiesce_froms: Enum.reject(state.quiesce_froms, fn {t, _from} -> t == token end)
        }
    end
  end

  defp quiesce_token(state, monitor_ref) do
    Enum.find_value(state.quiesce_tokens, fn
      {token, ^monitor_ref} -> token
      _other -> nil
    end)
  end

  defp paused?(state), do: state.closing? or map_size(state.quiesce_tokens) > 0

  defp slot_count(state), do: map_size(state.slot_monitors)

  defp slot_pids(state), do: Map.values(state.slot_monitors)

  defp close_slot_writer(pid, timeout) do
    # A slot may die between the check and the call; its DOWN is handled.
    if Process.alive?(pid) do
      try do
        GenServer.call(pid, :close_writer, timeout)
      catch
        :exit, reason ->
          if Deadline.genserver_call_timeout?(reason), do: exit(reason), else: :ok
      end
    end

    :ok
  end

  defp mark_registry(state, status) do
    _ =
      Registry.update_value(
        VialKeeper.Runtime.DatabaseRegistry,
        {:writer_pool, state.uuid},
        fn _ -> %{status: status, epoch: state.epoch} end
      )

    state
  end

  defp registry_value(uuid) do
    case Registry.lookup(VialKeeper.Runtime.DatabaseRegistry, {:writer_pool, uuid}) do
      [{_pid, value}] -> value
      [] -> nil
    end
  end

  defp call_if_running(uuid, message, timeout) do
    if registry_value(uuid) == nil, do: :ok, else: call(uuid, message, timeout)
  end

  defp call(uuid, message, timeout) do
    case Registry.lookup(VialKeeper.Runtime.DatabaseRegistry, {:writer_pool, uuid}) do
      [{pid, _value}] ->
        try do
          GenServer.call(pid, message, timeout)
        catch
          :exit, reason -> translate_exit(reason)
        end

      [] ->
        {:error, closed_error()}
    end
  end

  defp translate_exit(reason) do
    cond do
      Deadline.genserver_call_timeout?(reason) -> {:error, deadline_error()}
      Deadline.no_process_exit?(reason) -> {:error, closed_error()}
      true -> exit(reason)
    end
  end

  defp unwrap({:command_context, %CommandContext{} = authority, command}),
    do: {authority, Commands.normalize(command)}

  defp unwrap(command), do: {CommandContext.public(), Commands.normalize(command)}

  defp caller_pid({pid, _tag}) when is_pid(pid), do: pid

  defp closed_error, do: Error.database_closed("database is closed")

  defp deadline_error do
    Error.new(
      :internal_error,
      "database command timed out",
      %{reason: :deadline_exhausted},
      retryable: true
    )
  end

  defp probe_grant(class) do
    case Application.get_env(:vial_keeper, :writer_pool_probe) do
      {pid, ref} when is_pid(pid) and is_reference(ref) ->
        send(pid, {ref, :writer_pool_grant, class, nil})

      _ ->
        :ok
    end
  end
end
