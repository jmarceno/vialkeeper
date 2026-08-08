defmodule ElixirDB.Runtime.AttachmentCoordinator do
  @moduledoc false
  use GenServer

  alias ElixirDB.Runtime.DatabaseOwner

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
  def handle_call({:release, guard_token}, _from, state) do
    case release_guard(state, guard_token) do
      {:ok, state} -> {:reply, :ok, maybe_complete_waiters(state)}
      {:error, _} = error -> {:reply, error, state}
    end
  end

  @impl true
  def handle_call({:begin_gc, _caller_pid}, _from, %{gc_barrier: true} = state) do
    {:reply, {:error, ElixirDB.Error.attachment_overloaded("attachment gc is already active")},
     state}
  end

  @impl true
  def handle_call({:begin_gc, caller_pid}, from, state) do
    with :ok <- ensure_caller(caller_pid),
         :ok <- ensure_open(state) do
      monitor_ref = Process.monitor(caller_pid)
      state = %{state | gc_barrier: true, gc_caller: caller_pid, gc_monitor_ref: monitor_ref}

      if guard_count(state) == 0 do
        {token, state} = grant_gc_token(state)
        state = clear_gc_monitor(state)
        {:reply, {:ok, token}, state}
      else
        {:noreply, %{state | gc_waiter: from}}
      end
    else
      {:error, _} = error -> {:reply, error, state}
    end
  end

  @impl true
  def handle_call({:end_gc, gc_token}, _from, %{gc_token: gc_token} = state) do
    state =
      state
      |> clear_gc_monitor()
      |> Map.put(:gc_token, nil)
      |> Map.put(:gc_barrier, false)
      |> Map.put(:gc_caller, nil)

    {:reply, :ok, maybe_complete_waiters(state)}
  end

  @impl true
  def handle_call({:end_gc, _gc_token}, _from, state) do
    {:reply, {:error, ElixirDB.Error.invalid_request("invalid attachment gc token")}, state}
  end

  @impl true
  def handle_call(:begin_close, from, state) do
    state = %{state | closing: true, close_waiters: [from | state.close_waiters]}

    if drain_complete?(state) do
      {:reply, :ok, %{state | close_waiters: []}}
    else
      {:noreply, state}
    end
  end

  @impl true
  def handle_info({:DOWN, ref, :process, _pid, _reason}, state) do
    if ref == state.gc_monitor_ref do
      handle_gc_caller_down(state)
    else
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

    {:noreply, maybe_complete_waiters(state)}
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
    case DatabaseOwner.command(uuid, {:command, :identity, %{}}) do
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
      state = clear_gc_monitor(state)
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
    guard_count(state) == 0 and is_nil(state.gc_token) and is_nil(state.gc_waiter)
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
      gc_active: not is_nil(state.gc_token)
    }
  end
end
