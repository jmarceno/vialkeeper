defmodule ElixirDB.Runtime.AdmittedCommand do
  @moduledoc false
  use GenServer

  alias ElixirDB.Error
  alias ElixirDB.Runtime.Deadline

  @type args :: %{
          scheduler_pid: pid(),
          request_ref: reference(),
          owner_fun: (-> term()),
          deadline_ms: Deadline.t(),
          trace_context: term()
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

  def start_link(args), do: GenServer.start_link(__MODULE__, args)

  @impl true
  def init(%{scheduler_pid: scheduler_pid, request_ref: request_ref} = args) do
    send(self(), :run)
    {:ok, Map.put(args, :scheduler_pid, scheduler_pid) |> Map.put(:request_ref, request_ref)}
  end

  @impl true
  def handle_info(:run, state) do
    sync_before_begin()

    result =
      case GenServer.call(
             state.scheduler_pid,
             {:admitted_command_begin, state.request_ref, self()},
             5_000
           ) do
        :proceed -> {:run, run_owner_fun(state.owner_fun, state.deadline_ms, state.trace_context)}
        :cancel -> :cancelled
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

  defp sync_before_begin do
    case Application.get_env(:elixir_db, :admitted_command_sync) do
      {pid, ref} when is_pid(pid) ->
        send(pid, {ref, :before_begin, self()})

        receive do
          {:go, ^ref} -> :ok
        end

      _ ->
        :ok
    end
  end

  defp run_owner_fun(owner_fun, deadline_ms, trace_context) do
    if Deadline.exhausted?(deadline_ms) do
      {:error,
       Error.internal_error("database command timed out", %{
         reason: :deadline_exhausted
       })}
    else
      with_trace_context(trace_context, fn ->
        try do
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
