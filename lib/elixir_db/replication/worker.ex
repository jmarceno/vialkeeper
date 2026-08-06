defmodule ElixirDB.Replication.Worker do
  @moduledoc "Supervised replication state machine with cancellable bounded work."
  @behaviour :gen_statem

  @retryable_states [:running, :waiting, :backoff]

  def start_link(options), do: :gen_statem.start_link(__MODULE__, options, [])

  def child_spec(options) do
    %{
      id:
        {:replication_worker, options[:replication_id] || options["replication_id"] || make_ref()},
      start: {__MODULE__, :start_link, [options]},
      restart: :temporary,
      type: :worker
    }
  end

  @impl true
  def callback_mode, do: :handle_event_function

  @impl true
  def init(options) do
    replication_id = options[:replication_id] || options["replication_id"]

    with true <- is_binary(replication_id),
         {:ok, _} <- Registry.register(ElixirDB.Replication.WorkerRegistry, replication_id, %{}) do
      {:ok, :idle, %{options: options, attempts: 0, result: nil, error: nil}}
    else
      false ->
        {:stop, ElixirDB.Error.invalid_request("replication id is required")}

      {:error, {:already_registered, _pid}} ->
        {:stop, ElixirDB.Error.replication_already_running("replication worker is already running")}
    end
  end

  @impl true
  def handle_event(:cast, :start, :idle, data) do
    report(data, :running)
    {:next_state, :running, data, [{:next_event, :internal, :execute}]}
  end

  def handle_event(:internal, :execute, :running, data) do
    task =
      Task.Supervisor.async_nolink(ElixirDB.TaskSupervisor, fn ->
        ElixirDB.Replication.run(data.options.source, data.options.target, data.options)
      end)

    {:keep_state, Map.put(data, :task, task)}
  end

  def handle_event(:info, {ref, result}, :running, %{task: %{ref: ref}} = data) do
    Process.demonitor(ref, [:flush])
    data = Map.delete(data, :task)

    case result do
      {:ok, value} ->
        report(data, :completed, %{result: value})
        {:stop, :normal, %{data | result: value}}

      {:error, error} ->
        handle_failure(data, error)
    end
  end

  def handle_event(
        :info,
        {:DOWN, ref, :process, _pid, reason},
        :running,
        %{task: %{ref: ref}} = data
      ) do
    if reason in [:normal, :shutdown] do
      handle_failure(data, ElixirDB.Error.database_closed("replication task stopped"))
    else
      handle_failure(
        data,
        ElixirDB.Error.internal_error("replication task crashed", %{cause: inspect(reason)})
      )
    end
  end

  def handle_event(:state_timeout, :retry, :backoff, data),
    do: {:next_state, :running, data, [{:next_event, :internal, :execute}]}

  def handle_event(:cast, :cancel, state, data) when state in @retryable_states do
    if task = data[:task], do: Task.shutdown(task, :brutal_kill)
    report(data, :failed, %{error: ElixirDB.Error.database_closed("replication was cancelled")})

    {:stop, :normal, %{data | error: ElixirDB.Error.database_closed("replication was cancelled")}}
  end

  def handle_event(:cast, :cancel, _state, data), do: {:keep_state, data}
  def handle_event(:info, _event, _state, data), do: {:keep_state, data}
  def handle_event(_type, _event, _state, data), do: {:keep_state, data}

  @impl true
  def terminate(_reason, _state, data) do
    state = if data.error, do: :failed, else: if(data.result, do: :completed, else: :failed)
    report(data, state, %{result: data.result, error: data.error})
    :ok
  end

  defp handle_failure(data, error) do
    attempts = data.attempts + 1
    max_attempts = retry_option(data.options, :max_attempts, 8)

    if error.retryable and attempts < max_attempts do
      delay = retry_delay(data.options, attempts)
      report(data, :backoff, %{error: error, attempt: attempts, delay_ms: delay})

      {:next_state, :backoff, %{data | attempts: attempts, error: error},
       [{:state_timeout, delay, :retry}]}
    else
      report(data, :failed, %{error: error, attempt: attempts})
      {:stop, :normal, %{data | attempts: attempts, error: error}}
    end
  end

  defp report(data, state, details \\ %{}) do
    job_id = data.options[:job_id] || data.options["job_id"]
    if job_id, do: ElixirDB.Replication.JobManager.report(job_id, state, details)
  end

  defp retry_option(options, key, default) do
    retry = options[:retry] || options["retry"] || %{}
    retry[key] || retry[Atom.to_string(key)] || default
  end

  defp retry_delay(options, attempt) do
    base = retry_option(options, :base_delay_ms, 100)
    maximum = retry_option(options, :max_delay_ms, 30_000)
    jitter = retry_option(options, :jitter_ms, 250)
    deterministic_jitter = :erlang.phash2({options[:replication_id], attempt}, jitter + 1)
    min(maximum, trunc(base * :math.pow(2, attempt - 1)) + deterministic_jitter)
  end
end
