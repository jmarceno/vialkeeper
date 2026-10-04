defmodule VialKeeper.Runtime.AdmittedCommand do
  @moduledoc """
  Executes admitted owner commands for one scheduler and reports each completion.

  The scheduler keeps one executor per database and reuses it between commands,
  so a command costs a message instead of a supervised process start. The
  executor runs one command at a time and replies to the caller directly. It
  lives under the same `:one_for_all` supervisor as its scheduler, so the two
  restart together.

  Each admitted command carries a start flag that the executor and the
  scheduler both race on with a compare-and-swap: the executor moves it from
  granted to started before it calls the owner, and the scheduler moves it
  from granted to cancelled when it abandons the command first. Exactly one of
  them wins, so a command either runs or is abandoned without a message round
  trip between them.
  """
  use GenServer

  alias VialKeeper.Deadline
  alias VialKeeper.Error

  @granted 0
  @started 1
  @cancelled 2

  @opaque start_flag :: :atomics.atomics_ref()

  @type run_args :: %{
          uuid: binary(),
          request_ref: reference(),
          caller_pid: pid(),
          start_flag: start_flag(),
          from: GenServer.from() | nil,
          owner_fun: (-> term()),
          deadline_ms: Deadline.t(),
          trace_context: term(),
          probe_op: term() | nil
        }

  @spec start(pid(), pid()) :: DynamicSupervisor.on_start_child()
  def start(supervisor, scheduler_pid) when is_pid(scheduler_pid) do
    spec = %{
      id: __MODULE__,
      start: {__MODULE__, :start_link, [scheduler_pid]},
      restart: :temporary,
      shutdown: 5_000
    }

    DynamicSupervisor.start_child(supervisor, spec)
  end

  @spec start_link(pid()) :: GenServer.on_start()
  def start_link(scheduler_pid), do: GenServer.start_link(__MODULE__, scheduler_pid)

  @doc "Hands one admitted command to an idle executor."
  @spec run(pid(), run_args()) :: :ok
  def run(executor_pid, %{} = args) do
    send(executor_pid, {:run, args})
    :ok
  end

  @doc "Creates the start flag for one admitted command, in the granted state."
  @spec new_start_flag() :: start_flag()
  def new_start_flag, do: :atomics.new(1, signed: false)

  @doc """
  Cancels a command whose executor has not started it.

  Returns `:started` when the executor already started the command (it will
  run to completion), or `:unstarted` when it has not and now never will.
  """
  @spec claim_unstarted(start_flag() | nil) :: :started | :unstarted
  def claim_unstarted(nil), do: :unstarted

  def claim_unstarted(start_flag) do
    case :atomics.compare_exchange(start_flag, 1, @granted, @cancelled) do
      :ok -> :unstarted
      @cancelled -> :unstarted
      @started -> :started
    end
  end

  @impl true
  def init(scheduler_pid), do: {:ok, %{scheduler_pid: scheduler_pid}}

  @impl true
  def handle_info({:run, args}, state) do
    sync_before_begin(args.uuid, args.probe_op)

    case begin(args) do
      :proceed ->
        result =
          run_owner_fun(
            args.uuid,
            args.owner_fun,
            args.deadline_ms,
            args.trace_context,
            args.probe_op
          )

        report_completion(state, args, result)

      :cancel ->
        :ok
    end

    {:noreply, state}
  end

  # A late reply to an owner call that already timed out, or a test gate
  # release sent after the gate was passed.
  def handle_info(_message, state), do: {:noreply, state}

  # A caller that is already gone is not started; the scheduler abandons the
  # command when it handles the caller's exit.
  defp begin(%{caller_pid: caller_pid, start_flag: start_flag}) do
    if Process.alive?(caller_pid) and
         :atomics.compare_exchange(start_flag, 1, @granted, @started) == :ok,
       do: :proceed,
       else: :cancel
  end

  # The executor replies first so the caller does not wait for the scheduler
  # hop. A caller that already gave up has a deactivated reply alias, so the
  # late reply is dropped.
  defp report_completion(state, %{from: from, request_ref: request_ref}, result) do
    if from, do: GenServer.reply(from, result)
    send(state.scheduler_pid, {:admitted_command_done, request_ref, self()})
  end

  defp sync_before_begin(uuid, probe_op) when is_binary(uuid) do
    case Application.get_env(:vial_keeper, :admitted_command_sync) do
      {pid, ref, ^uuid, only_op} when is_pid(pid) and only_op == probe_op ->
        wait_for_sync_gate(pid, ref, :before_begin)

      {pid, ref, ^uuid} when is_pid(pid) ->
        wait_for_sync_gate(pid, ref, :before_begin)

      {pid, ref} when is_pid(pid) ->
        wait_for_sync_gate(pid, ref, :before_begin)

      _ ->
        :ok
    end
  end

  defp sync_owner_body(uuid, probe_op) when is_binary(uuid) do
    case Application.get_env(:vial_keeper, :admitted_command_owner_body_sync) do
      {pid, ref, ^uuid, only_op} when is_pid(pid) and only_op == probe_op ->
        wait_for_sync_gate(pid, ref, :owner_body)

      {pid, ref, ^uuid} when is_pid(pid) ->
        wait_for_sync_gate(pid, ref, :owner_body)

      {pid, ref} when is_pid(pid) ->
        wait_for_sync_gate(pid, ref, :owner_body)

      _ ->
        :ok
    end
  end

  defp wait_for_sync_gate(pid, ref, event) do
    send(pid, {ref, event, self()})

    receive do
      {:go, ^ref} -> :ok
    end
  end

  defp run_owner_fun(uuid, owner_fun, deadline_ms, trace_context, probe_op) do
    if Deadline.exhausted?(deadline_ms) do
      {:error,
       Error.new(
         :internal_error,
         "database command timed out",
         %{reason: :deadline_exhausted},
         retryable: true
       )}
    else
      with_trace_context(trace_context, fn ->
        try do
          # Test barrier after executor_started? and before the owner call body.
          sync_owner_body(uuid, probe_op)
          owner_fun.()
        catch
          kind, reason ->
            {:error,
             Error.internal_error("admitted command failed", %{
               kind: kind,
               reason: inspect(reason)
             })}
        end
      end)
    end
  end

  defp with_trace_context(trace_context, fun) when is_function(fun, 0) do
    token = OpenTelemetry.Ctx.attach(trace_context)

    try do
      fun.()
    after
      OpenTelemetry.Ctx.detach(token)
    end
  end
end
