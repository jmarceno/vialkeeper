defmodule VialKeeper.Runtime.AdmittedCommand do
  @moduledoc "Executes one admitted owner command and reports its completion to the scheduler."
  use GenServer

  alias VialKeeper.Deadline
  alias VialKeeper.Error

  @type args :: %{
          uuid: binary(),
          scheduler_pid: pid(),
          request_ref: reference(),
          owner_fun: (-> term()),
          deadline_ms: Deadline.t(),
          trace_context: term(),
          probe_op: term() | nil
        }

  @spec start(pid(), args()) :: {:ok, pid()} | {:error, term()}
  def start(supervisor, %{} = args) do
    spec = %{
      id: {__MODULE__, args.request_ref},
      start: {__MODULE__, :start_link, [args]},
      restart: :temporary,
      shutdown: 5_000
    }

    DynamicSupervisor.start_child(supervisor, spec)
  end

  @spec start_link(args()) :: GenServer.on_start()
  def start_link(args), do: GenServer.start_link(__MODULE__, args)

  @impl true
  def init(%{scheduler_pid: scheduler_pid, request_ref: request_ref} = args) do
    send(self(), :run)
    {:ok, Map.put(args, :scheduler_pid, scheduler_pid) |> Map.put(:request_ref, request_ref)}
  end

  @impl true
  def handle_info(:run, state) do
    sync_before_begin(state.uuid, state[:probe_op])

    result =
      case GenServer.call(
             state.scheduler_pid,
             {:admitted_command_begin, state.request_ref, self()},
             5_000
           ) do
        :proceed ->
          {:run,
           run_owner_fun(
             state.uuid,
             state.owner_fun,
             state.deadline_ms,
             state.trace_context,
             state[:probe_op]
           )}

        :cancel ->
          :cancelled
      end

    try do
      :ok
    after
      report_completion(state, result)
    end

    {:stop, :normal, state}
  end

  defp report_completion(_state, :cancelled), do: :ok

  defp report_completion(state, {:run, result}) do
    send(state.scheduler_pid, {:admitted_command_done, state.request_ref, result})
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
