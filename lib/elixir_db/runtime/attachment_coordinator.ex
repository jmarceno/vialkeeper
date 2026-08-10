defmodule ElixirDB.Runtime.AttachmentCoordinator do
  @moduledoc "Coordinates attachment guards and serialized post-compact garbage collection."
  use GenServer

  alias ElixirDB.Runtime.DatabaseAdmission

  def start_link(uuid), do: GenServer.start_link(__MODULE__, uuid, name: via(uuid))

  def via(uuid),
    do: {:via, Registry, {ElixirDB.Runtime.DatabaseRegistry, {:attachment_coordinator, uuid}}}

  def acquire_read(uuid, caller_pid \\ self()) do
    call(uuid, {:acquire_read, caller_pid})
  end

  def acquire_write(uuid, caller_pid \\ self()) do
    call(uuid, {:acquire_write, caller_pid})
  end

  def acquire_reference(uuid, caller_pid \\ self()) do
    call(uuid, {:acquire_reference, caller_pid})
  end

  def release(uuid, guard_token) do
    call(uuid, {:release, guard_token})
  end

  def begin_gc(uuid, caller_pid \\ self()) do
    call(uuid, {:begin_gc, caller_pid}, :infinity)
  end

  def end_gc(uuid, gc_token) do
    call(uuid, {:end_gc, gc_token})
  end

  @doc """
  Spawns and tracks an asynchronous GC Task (post-compact seam).

  Increments `gc_scheduled` before returning so callers cannot observe idle
  between schedule and Task start. The Task monitor clears the count on exit
  (normal or kill).
  """
  def schedule_gc(uuid, module) when is_atom(module) do
    call(uuid, {:schedule_gc, module})
  end

  def begin_close(uuid) do
    call(uuid, :begin_close, :infinity)
  end

  def update_limits(uuid, attachments_config) when is_map(attachments_config) do
    call(uuid, {:update_limits, attachments_config})
  end

  def status(uuid) do
    call(uuid, :status)
  end

  defp call(uuid, message, timeout \\ 30_000) do
    case Registry.lookup(ElixirDB.Runtime.DatabaseRegistry, {:attachment_coordinator, uuid}) do
      [{pid, _}] ->
        try do
          GenServer.call(pid, message, timeout)
        catch
          :exit, _ ->
            {:error, ElixirDB.Error.database_closed("attachment coordinator is not running")}
        end

      [] ->
        {:error, ElixirDB.Error.database_closed("attachment coordinator is not running")}
    end
  end

  @impl true
  def init(uuid) do
    limits = load_limits(uuid)

    {:ok,
     %{
       uuid: uuid,
       closing: false,
       gc_barrier: false,
       gc_token: nil,
       gc_caller: nil,
       gc_monitor_ref: nil,
       gc_waiter: nil,
       # At most one queued begin_gc while a run is active (MAINT-009 / MAINT-010).
       gc_follow_up: nil,
       # Count of asynchronously scheduled GC Tasks not yet finished (post-compact).
       gc_scheduled: 0,
       gc_task_monitors: %{},
       close_waiters: [],
       guards: %{},
       monitors: %{},
       read_limit: limits.read_limit,
       write_limit: limits.write_limit,
       max_attachment_bytes: limits.max_attachment_bytes,
       active_reads: 0,
       active_writes: 0,
       active_references: 0
     }}
  end

  @impl true
  def handle_call({:update_limits, attachments}, _from, state) do
    {:reply, :ok, apply_limits(state, attachments)}
  end

  @impl true
  def handle_call(:status, _from, state) do
    {:reply, status_map(state), state}
  end

  @impl true
  def handle_call({:acquire_read, caller_pid}, _from, state) do
    with :ok <- ensure_caller(caller_pid),
         :ok <- ensure_open(state),
         :ok <- ensure_no_gc_barrier(state),
         :ok <- ensure_read_capacity(state) do
      token = make_token()
      {:reply, {:ok, token}, add_guard(state, caller_pid, :read, token)}
    else
      {:error, _} = error -> {:reply, error, state}
    end
  end

  @impl true
  def handle_call({:acquire_write, caller_pid}, _from, state) do
    with :ok <- ensure_caller(caller_pid),
         :ok <- ensure_open(state),
         :ok <- ensure_no_gc_barrier(state),
         :ok <- ensure_write_capacity(state) do
      token = make_token()
      max_bytes = state.max_attachment_bytes

      {:reply, {:ok, token, max_bytes},
       add_guard(state, caller_pid, :write, token, %{max_attachment_bytes: max_bytes})}
    else
      {:error, _} = error -> {:reply, error, state}
    end
  end

  @impl true
  def handle_call({:acquire_reference, caller_pid}, _from, state) do
    with :ok <- ensure_caller(caller_pid),
         :ok <- ensure_open(state),
         :ok <- ensure_no_gc_barrier(state) do
      token = make_token()
      {:reply, {:ok, token}, add_guard(state, caller_pid, :reference, token)}
    else
      {:error, _} = error -> {:reply, error, state}
    end
  end

  @impl true
  def handle_call({:release, guard_token}, {caller_pid, _tag}, state) do
    case release_guard_for_caller(state, guard_token, caller_pid) do
      {:ok, state} -> {:reply, :ok, maybe_complete_waiters(state)}
      {:error, _} = error -> {:reply, error, state}
    end
  end

  @impl true
  def handle_call({:schedule_gc, module}, _from, state) when is_atom(module) do
    case ensure_open(state) do
      :ok -> {:reply, :ok, start_scheduled_gc(state, module)}
      {:error, _} = error -> {:reply, error, state}
    end
  end

  @impl true
  def handle_call({:begin_gc, caller_pid}, from, %{gc_barrier: true} = state) do
    with :ok <- ensure_caller(caller_pid),
         :ok <- ensure_open(state) do
      case state.gc_follow_up do
        nil ->
          # Serialize the next GC so a post-compact trigger cannot race an
          # in-flight run and so the follow-up recomputes the live set.
          {:noreply, %{state | gc_follow_up: {from, caller_pid}}}

        {_queued_from, _queued_pid} ->
          # Further overlapping triggers are harmless: the queued follow-up
          # already covers a recomputation after the current barrier drops.
          {:reply, {:ok, :coalesced}, state}
      end
    else
      {:error, _} = error -> {:reply, error, state}
    end
  end

  @impl true
  def handle_call({:begin_gc, caller_pid}, from, state) do
    with :ok <- ensure_caller(caller_pid),
         :ok <- ensure_open(state) do
      monitor_ref = Process.monitor(caller_pid)
      state = %{state | gc_barrier: true, gc_caller: caller_pid, gc_monitor_ref: monitor_ref}

      if guard_count(state) == 0 do
        {token, state} = grant_gc_token(state)
        {:reply, {:ok, token}, state}
      else
        {:noreply, %{state | gc_waiter: from}}
      end
    else
      {:error, _} = error -> {:reply, error, state}
    end
  end

  @impl true
  def handle_call(
        {:end_gc, gc_token},
        {caller_pid, _tag},
        %{gc_token: gc_token, gc_caller: caller_pid} = state
      ) do
    state =
      state
      |> clear_gc_monitor()
      |> Map.put(:gc_token, nil)
      |> Map.put(:gc_barrier, false)
      |> Map.put(:gc_caller, nil)
      |> promote_gc_follow_up()

    {:reply, :ok, maybe_complete_waiters(state)}
  end

  @impl true
  def handle_call({:end_gc, _gc_token}, _from, state) do
    {:reply, {:error, ElixirDB.Error.invalid_request("invalid attachment gc token")}, state}
  end

  @impl true
  def handle_call(:begin_close, from, state) do
    state = abort_gc_for_close(state)
    state = %{state | closing: true, close_waiters: [from | state.close_waiters]}

    if drain_complete?(state) do
      {:reply, :ok, %{state | close_waiters: []}}
    else
      {:noreply, state}
    end
  end

  @impl true
  def handle_info({:DOWN, ref, :process, _pid, _reason}, state) do
    cond do
      ref == state.gc_monitor_ref ->
        handle_gc_caller_down(state)

      Map.has_key?(state.gc_task_monitors, ref) ->
        state = %{
          state
          | gc_task_monitors: Map.delete(state.gc_task_monitors, ref),
            gc_scheduled: max(state.gc_scheduled - 1, 0)
        }

        {:noreply, maybe_complete_waiters(state)}

      true ->
        handle_guard_down(ref, state)
    end
  end

  defp handle_gc_caller_down(state) do
    if state.gc_waiter do
      GenServer.reply(
        state.gc_waiter,
        {:error, ElixirDB.Error.database_closed("attachment gc caller terminated")}
      )
    end

    state =
      state
      |> Map.put(:gc_waiter, nil)
      |> Map.put(:gc_token, nil)
      |> Map.put(:gc_barrier, false)
      |> Map.put(:gc_caller, nil)
      |> Map.put(:gc_monitor_ref, nil)
      |> promote_gc_follow_up()

    {:noreply, maybe_complete_waiters(state)}
  end

  defp promote_gc_follow_up(%{gc_follow_up: {from, caller_pid}} = state) do
    case {ensure_caller(caller_pid), ensure_open(state)} do
      {:ok, :ok} ->
        monitor_ref = Process.monitor(caller_pid)

        %{
          state
          | gc_follow_up: nil,
            gc_barrier: true,
            gc_caller: caller_pid,
            gc_monitor_ref: monitor_ref,
            gc_token: nil,
            gc_waiter: from
        }

      {{:error, error}, _} ->
        GenServer.reply(from, {:error, error})
        %{state | gc_follow_up: nil}

      {_, {:error, error}} ->
        GenServer.reply(from, {:error, error})
        %{state | gc_follow_up: nil}
    end
  end

  defp promote_gc_follow_up(state), do: state

  defp start_scheduled_gc(state, module) do
    uuid = state.uuid
    {:ok, pid} = Task.start(fn -> _ = module.gc(uuid) end)
    ref = Process.monitor(pid)

    %{
      state
      | gc_scheduled: state.gc_scheduled + 1,
        gc_task_monitors: Map.put(state.gc_task_monitors, ref, pid)
    }
  end

  defp abort_gc_for_close(state) do
    Enum.each(state.gc_task_monitors, fn {ref, pid} ->
      _ = Process.demonitor(ref, [:flush])
      _ = Process.exit(pid, :kill)
    end)

    state =
      if state.gc_waiter do
        GenServer.reply(
          state.gc_waiter,
          {:error, ElixirDB.Error.database_closed("attachment coordinator is closing")}
        )

        %{state | gc_waiter: nil}
      else
        state
      end

    state =
      case state.gc_follow_up do
        {from, _pid} ->
          GenServer.reply(
            from,
            {:error, ElixirDB.Error.database_closed("attachment coordinator is closing")}
          )

          %{state | gc_follow_up: nil}

        nil ->
          state
      end

    state = %{state | gc_scheduled: 0, gc_task_monitors: %{}}

    if is_nil(state.gc_token) do
      state
      |> clear_gc_monitor()
      |> Map.merge(%{
        gc_token: nil,
        gc_barrier: false,
        gc_caller: nil,
        gc_waiter: nil,
        gc_follow_up: nil
      })
    else
      # An active caller-owned GC cannot be cancelled safely from the
      # coordinator. Keep its token and monitor until it calls end_gc/2 (or
      # terminates), so close never races physical deletion.
      state
    end
  end

  defp handle_guard_down(ref, state) do
    case Map.pop(state.monitors, ref) do
      {nil, _} ->
        {:noreply, state}

      {token, monitors} ->
        state = %{state | monitors: monitors}

        case release_guard(state, token) do
          {:ok, state} -> {:noreply, maybe_complete_waiters(state)}
          {:error, _} -> {:noreply, state}
        end
    end
  end

  defp clear_gc_monitor(%{gc_monitor_ref: nil} = state), do: state

  defp clear_gc_monitor(%{gc_monitor_ref: ref} = state) do
    _ = Process.demonitor(ref, [:flush])
    %{state | gc_monitor_ref: nil}
  end

  defp load_limits(uuid) do
    case DatabaseAdmission.execute(uuid, :maintenance, {:command, :identity, %{}}) do
      {:ok, %{config: config}} when is_map(config) ->
        limits_from_config(Map.get(config, "attachments", %{}))

      _ ->
        defaults = ElixirDB.Config.defaults()["attachments"]
        limits_from_config(defaults)
    end
  end

  defp limits_from_config(attachments) do
    defaults = ElixirDB.Config.defaults()["attachments"]

    %{
      read_limit:
        Map.get(attachments, "max_concurrent_attachment_reads") ||
          defaults["max_concurrent_attachment_reads"],
      write_limit:
        Map.get(attachments, "max_concurrent_attachment_writes") ||
          defaults["max_concurrent_attachment_writes"],
      max_attachment_bytes:
        Map.get(attachments, "max_attachment_bytes") || defaults["max_attachment_bytes"]
    }
  end

  defp apply_limits(state, attachments) do
    limits = limits_from_config(attachments)

    %{
      state
      | read_limit: limits.read_limit,
        write_limit: limits.write_limit,
        max_attachment_bytes: limits.max_attachment_bytes
    }
  end

  defp ensure_open(%{closing: true}),
    do: {:error, ElixirDB.Error.database_closed("attachment coordinator is closing")}

  defp ensure_open(_), do: :ok

  defp ensure_caller(caller_pid) when is_pid(caller_pid), do: :ok

  defp ensure_caller(_),
    do: {:error, ElixirDB.Error.invalid_request("attachment guard caller must be a process")}

  defp ensure_no_gc_barrier(%{gc_barrier: true}),
    do: {:error, ElixirDB.Error.attachment_overloaded("attachment gc barrier is active")}

  defp ensure_no_gc_barrier(_), do: :ok

  defp ensure_read_capacity(%{active_reads: active, read_limit: limit}) when active < limit, do: :ok

  defp ensure_read_capacity(_),
    do: {:error, ElixirDB.Error.attachment_overloaded("attachment read limit reached")}

  defp ensure_write_capacity(%{active_writes: active, write_limit: limit}) when active < limit,
    do: :ok

  defp ensure_write_capacity(_),
    do: {:error, ElixirDB.Error.attachment_overloaded("attachment write limit reached")}

  defp add_guard(state, caller_pid, kind, token, extra \\ []) do
    ref = Process.monitor(caller_pid)

    guard =
      extra
      |> Map.new()
      |> Map.merge(%{pid: caller_pid, kind: kind, monitor_ref: ref})

    state
    |> Map.update!(:guards, &Map.put(&1, token, guard))
    |> Map.put(:monitors, Map.put(state.monitors, ref, token))
    |> increment_kind(kind, 1)
  end

  defp release_guard(state, token) do
    case Map.pop(state.guards, token) do
      {nil, _} ->
        {:error, ElixirDB.Error.invalid_request("unknown attachment guard token")}

      {guard, guards} ->
        _ = Process.demonitor(guard.monitor_ref, [:flush])
        monitors = Map.delete(state.monitors, guard.monitor_ref)

        state =
          state
          |> Map.put(:guards, guards)
          |> Map.put(:monitors, monitors)
          |> increment_kind(guard.kind, -1)

        {:ok, state}
    end
  end

  defp release_guard_for_caller(state, token, caller_pid) do
    case Map.get(state.guards, token) do
      %{pid: ^caller_pid} ->
        release_guard(state, token)

      %{pid: _other} ->
        {:error, ElixirDB.Error.invalid_request("attachment guard belongs to another process")}

      nil ->
        {:error, ElixirDB.Error.invalid_request("unknown attachment guard token")}
    end
  end

  defp increment_kind(state, :read, delta),
    do: %{state | active_reads: state.active_reads + delta}

  defp increment_kind(state, :write, delta),
    do: %{state | active_writes: state.active_writes + delta}

  defp increment_kind(state, :reference, delta),
    do: %{state | active_references: state.active_references + delta}

  defp grant_gc_token(state) do
    token = make_token()
    {token, %{state | gc_token: token}}
  end

  defp maybe_complete_waiters(state) do
    state
    |> maybe_grant_gc()
    |> maybe_complete_close()
  end

  defp maybe_grant_gc(%{gc_barrier: true, gc_token: nil, gc_waiter: from} = state)
       when not is_nil(from) do
    if guard_count(state) == 0 do
      {token, state} = grant_gc_token(state)
      # Keep gc_monitor_ref until end_gc/caller DOWN so a killed GC process
      # cannot leave the exclusive barrier stuck.
      GenServer.reply(from, {:ok, token})
      %{state | gc_waiter: nil}
    else
      state
    end
  end

  defp maybe_grant_gc(state), do: state

  defp maybe_complete_close(%{closing: true, close_waiters: waiters} = state)
       when waiters != [] do
    if drain_complete?(state) do
      Enum.each(waiters, &GenServer.reply(&1, :ok))
      %{state | close_waiters: []}
    else
      state
    end
  end

  defp maybe_complete_close(state), do: state

  defp drain_complete?(state) do
    # Scheduled post-compact GC Tasks are tracked for observers (`gc_scheduled`)
    # but MUST NOT block close: they call back into DatabaseCatalog/owner and
    # would deadlock if close waited for them. Closing rejects new GC; in-flight
    # GC fails with database_closed and its Task monitor clears the count.
    guard_count(state) == 0 and is_nil(state.gc_token) and is_nil(state.gc_waiter) and
      is_nil(state.gc_follow_up)
  end

  defp guard_count(state),
    do: state.active_reads + state.active_writes + state.active_references

  defp make_token, do: make_ref()

  defp status_map(state) do
    %{
      active_reads: state.active_reads,
      active_writes: state.active_writes,
      active_references: state.active_references,
      read_limit: state.read_limit,
      write_limit: state.write_limit,
      max_attachment_bytes: state.max_attachment_bytes,
      closing: state.closing,
      gc_barrier: state.gc_barrier,
      gc_active: not is_nil(state.gc_token),
      gc_queued: not is_nil(state.gc_follow_up),
      gc_scheduled: state.gc_scheduled > 0
    }
  end
end
