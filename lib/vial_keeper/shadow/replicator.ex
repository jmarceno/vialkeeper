defmodule VialKeeper.Shadow.Replicator do
  @moduledoc "Managed pull process for one generation-fenced shadow replica."
  use GenServer

  alias VialKeeper.Error
  alias VialKeeper.Replication
  alias VialKeeper.Replication.{LocalEndpoint, Profile, RemoteEndpoint}
  alias VialKeeper.Runtime.ChildSpec
  alias VialKeeper.Shadow.Worker

  @default_retry_ms 1_000

  @type options :: keyword()

  @spec pull(map(), options()) :: {:ok, map()} | {:error, Error.t()}
  def pull(request, opts \\ []), do: run_once(request, opts)

  @spec start_link(options()) :: GenServer.on_start()
  def start_link(opts), do: GenServer.start_link(__MODULE__, opts, name: Keyword.get(opts, :name))

  def child_spec(opts) do
    request = Keyword.fetch!(opts, :request)
    id = {__MODULE__, request_value(request, "shadow_uuid"), request_value(request, "generation")}

    ChildSpec.worker(id, {__MODULE__, :start_link, [opts]}, :permanent)
  end

  @spec cancel(GenServer.server()) :: :ok
  def cancel(server),
    do: GenServer.call(server, :cancel, VialKeeper.Config.request_timeout_ms())

  @spec status(GenServer.server()) :: map()
  def status(server),
    do: GenServer.call(server, :status, VialKeeper.Config.request_timeout_ms())

  @impl true
  def init(opts) do
    request = Keyword.fetch!(opts, :request)
    send(self(), :pull)
    {:ok, %{request: request, options: opts, task: nil, timer: nil, status: :starting}}
  end

  @impl true
  def handle_call(:cancel, _from, %{task: nil} = state),
    do: {:reply, :ok, %{state | status: :cancelled}}

  def handle_call(:cancel, _from, %{task: task} = state) do
    _ = Task.shutdown(task, :brutal_kill)
    {:reply, :ok, %{state | task: nil, status: :cancelled}}
  end

  def handle_call(:status, _from, state),
    do: {:reply, %{status: state.status, task_running: not is_nil(state.task)}, state}

  @impl true
  def handle_info(:pull, %{status: :cancelled} = state), do: {:noreply, state}

  def handle_info(:pull, %{task: nil} = state) do
    task = Task.async(fn -> run_once(state.request, state.options) end)
    {:noreply, %{state | task: task, status: :running}}
  end

  def handle_info({ref, result}, %{task: %Task{ref: ref}} = state) do
    _ = Process.demonitor(ref, [:flush])
    handle_pull_result(result, %{state | task: nil})
  end

  def handle_info({:DOWN, ref, :process, _pid, reason}, %{task: %Task{ref: ref}} = state) do
    handle_pull_result(
      {:error, Error.database_unavailable("shadow pull task stopped", %{cause: reason})},
      %{state | task: nil}
    )
  end

  def handle_info(_message, state), do: {:noreply, state}

  @impl true
  def terminate(_reason, %{task: nil}), do: :ok
  def terminate(_reason, %{task: task}), do: Task.shutdown(task, :brutal_kill)

  defp handle_pull_result({:ok, _result}, state) do
    state = %{state | status: :ready}

    if continuous?(state.options) do
      {:noreply, schedule_pull(state, 0)}
    else
      {:noreply, state}
    end
  end

  defp handle_pull_result({:error, %Error{code: :shadow_replacement_required} = error}, state) do
    {:noreply, %{state | status: {:replacement_required, error}}}
  end

  defp handle_pull_result({:error, _error}, state) do
    if continuous?(state.options) do
      {:noreply, schedule_pull(%{state | status: :retrying}, retry_ms(state.options))}
    else
      {:noreply, %{state | status: :failed}}
    end
  end

  defp schedule_pull(state, delay), do: %{state | timer: Process.send_after(self(), :pull, delay)}

  defp run_once(request, opts) when is_map(request) do
    request = Map.new(request, fn {key, value} -> {to_string(key), value} end)

    with {:ok, profile} <- shadow_profile(request),
         {:ok, source} <- source_endpoint(request),
         {:ok, target} <-
           LocalEndpoint.new(request["shadow_uuid"],
             shadow: true,
             source_database_uuid: request["source_uuid"],
             generation: request["generation"],
             operation_id: request["operation_id"]
           ),
         result <-
           Replication.one_shot_endpoints(
             source,
             target,
             replication_options(request, profile, opts)
           ) do
      maybe_mark_ready(request, result, opts)
      result
    end
  end

  defp run_once(_request, _opts),
    do: {:error, Error.invalid_request("shadow replication request must be an object")}

  defp shadow_profile(request) do
    profile =
      Profile.shadow(
        source_database_uuid: request["source_uuid"],
        target_database_uuid: request["shadow_uuid"],
        generation: request["generation"],
        operation_id: request["operation_id"]
      )

    case Profile.validate(profile) do
      :ok -> {:ok, profile}
      {:error, error} -> {:error, error}
    end
  end

  defp source_endpoint(request) do
    if String.trim(request["source_base_url"] || "") == "" do
      LocalEndpoint.new(request["source_uuid"])
    else
      RemoteEndpoint.new(%{
        "database_uuid" => request["source_uuid"],
        "base_url" => request["source_base_url"],
        "auth_token" => request["source_bearer_token"]
      })
    end
  end

  defp replication_options(request, profile, opts) do
    %{
      profile: profile,
      direction: "pull",
      mode: Keyword.get(opts, :mode, "one_shot"),
      shadow_ready: request["state"] == "ready",
      wait_ms: Keyword.get(opts, :wait_ms, 1_000),
      wait_ms_for_read: Keyword.get(opts, :wait_ms_for_read, 0),
      batch: Keyword.get(opts, :batch, 100),
      phase_hook: readiness_phase_hook(request, opts)
    }
  end

  defp readiness_phase_hook(request, opts) do
    phase_hook = Keyword.get(opts, :phase_hook)

    if continuous?(opts) and Keyword.get(opts, :mark_ready, false) do
      fn phase, context -> readiness_phase(phase_hook, phase, context, request, opts) end
    else
      phase_hook
    end
  end

  defp readiness_phase(phase_hook, phase, context, request, opts) do
    case invoke_phase_hook(phase_hook, phase, context) do
      :ok -> mark_ready_at_phase(phase, request, context, opts)
      error -> error
    end
  end

  defp invoke_phase_hook(nil, _phase, _context), do: :ok
  defp invoke_phase_hook(phase_hook, phase, context), do: phase_hook.(phase, context)

  defp mark_ready_at_phase(:after_checkpoint_source, request, context, opts) do
    Worker.mark_ready(
      generation_request(request),
      Map.get(context, :since, 0),
      Keyword.get(opts, :worker_options, [])
    )
  end

  defp mark_ready_at_phase(_phase, _request, _context, _opts), do: :ok

  defp maybe_mark_ready(_request, {:error, _error}, _opts), do: :ok

  defp maybe_mark_ready(request, {:ok, result}, opts) do
    if Keyword.get(opts, :mark_ready, false) do
      worker_opts = Keyword.get(opts, :worker_options, [])

      _ =
        Worker.mark_ready(
          generation_request(request),
          result_value(result, :source_sequence),
          worker_opts
        )

      :ok
    end

    :ok
  end

  defp result_value(result, key) when is_map(result),
    do: Map.get(result, key, Map.get(result, to_string(key), 0))

  defp continuous?(opts), do: Keyword.get(opts, :mode, "one_shot") in [:continuous, "continuous"]
  defp retry_ms(opts), do: Keyword.get(opts, :retry_ms, @default_retry_ms)

  defp request_value(request, "shadow_uuid") when is_map(request),
    do: Map.get(request, "shadow_uuid", Map.get(request, :shadow_uuid))

  defp request_value(request, "generation") when is_map(request),
    do: Map.get(request, "generation", Map.get(request, :generation))

  defp request_value(request, key) when is_map(request), do: Map.get(request, key)

  defp generation_request(request) do
    Map.take(request, ~w(source_uuid shadow_uuid generation operation_id))
  end
end
