defmodule ElixirDB.Replication.Worker do
  @moduledoc """
  Supervised replication state machine with cancellable bounded work.

  States (Plan §7.7): idle, handshake, read_changes, diff, fetch_chains, import,
  checkpoint_target, checkpoint_source, waiting, backoff, completed, failed.

  Cancellation (REPL-018) is checked between phases after a phase Task completes.
  In-flight endpoint work is allowed to finish; the worker never brutal-kills a
  task mid-import or mid-checkpoint commit.

  `:completed` and `:failed` are brief real gen_statem states entered before stop
  so Plan §7.7 is literal for transition and fault tests.
  """
  @behaviour :gen_statem

  alias ElixirDB.Replication

  @active_states [
    :idle,
    :handshake,
    :read_changes,
    :diff,
    :fetch_chains,
    :import,
    :checkpoint_target,
    :checkpoint_source,
    :waiting,
    :backoff
  ]

  @terminal_states [:completed, :failed]

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
      data = %{
        options: normalize_options(options),
        attempts: 0,
        result: nil,
        error: nil,
        cancel_requested: false,
        context: nil,
        task: nil
      }

      # Notify the initial :idle state so observers see the full Plan §7.7 sequence from
      # :idle through :completed/:failed. notify_state/2 runs synchronously here (a direct
      # send) before the state machine loop starts, so the :idle notification is guaranteed
      # to arrive before any :start-driven phase notification.
      notify_state(data, :idle)
      {:ok, :idle, data}
    else
      false ->
        {:stop, ElixirDB.Error.invalid_request("replication id is required")}

      {:error, {:already_registered, _pid}} ->
        {:stop, ElixirDB.Error.replication_already_running("replication worker is already running")}
    end
  end

  @impl true
  def handle_event(:cast, :start, :idle, data) do
    enter_phase(:handshake, data)
  end

  def handle_event(:cast, :cancel, state, data) when state in @active_states do
    error = cancelled_error()

    if data[:task] do
      # Let the in-flight bounded phase finish; stop before the next transition.
      {:keep_state, %{data | cancel_requested: true, error: error}}
    else
      enter_terminal(:failed, %{data | error: error, cancel_requested: true}, %{error: error})
    end
  end

  def handle_event(:cast, :cancel, state, data) when state in @terminal_states,
    do: {:keep_state, data}

  def handle_event(:cast, :cancel, _state, data), do: {:keep_state, data}

  def handle_event(:state_timeout, :retry, :backoff, data) do
    if data.cancel_requested do
      stop_cancelled(data)
    else
      enter_phase(:handshake, %{data | context: nil})
    end
  end

  def handle_event(:state_timeout, :halt, state, data) when state in @terminal_states do
    {:stop, :normal, data}
  end

  def handle_event(:info, {ref, result}, state, %{task: %{ref: ref}} = data) do
    Process.demonitor(ref, [:flush])
    data = Map.delete(data, :task)

    if data.cancel_requested do
      stop_cancelled(data)
    else
      handle_phase_result(state, result, data)
    end
  end

  def handle_event(
        :info,
        {:DOWN, ref, :process, _pid, reason},
        state,
        %{task: %{ref: ref}} = data
      )
      when state in @active_states do
    data = Map.delete(data, :task)

    if data.cancel_requested do
      stop_cancelled(data)
    else
      error =
        if reason in [:normal, :shutdown] do
          ElixirDB.Error.database_closed("replication task stopped")
        else
          ElixirDB.Error.internal_error("replication task crashed", %{cause: inspect(reason)})
        end

      handle_failure(data, error)
    end
  end

  def handle_event(:info, _event, _state, data), do: {:keep_state, data}
  def handle_event(_type, _event, _state, data), do: {:keep_state, data}

  @impl true
  def terminate(_reason, state, _data) when state in @terminal_states, do: :ok

  def terminate(_reason, _state, data) do
    state = if data.error, do: :failed, else: if(data.result, do: :completed, else: :failed)
    report(data, state, %{result: data.result, error: data.error})
    :ok
  end

  defp enter_phase(phase, data) do
    if data.cancel_requested do
      stop_cancelled(data)
    else
      report(data, phase)
      notify_state(data, phase)
      task = async_phase(phase, data)
      {:next_state, phase, Map.put(data, :task, task)}
    end
  end

  defp enter_terminal(state, data, details) when state in @terminal_states do
    report(data, state, details)
    notify_state(data, state)
    {:next_state, state, data, [{:state_timeout, 0, :halt}]}
  end

  defp async_phase(phase, data) do
    options = data.options
    source = options.source
    target = options.target
    context = data.context

    Task.Supervisor.async_nolink(ElixirDB.TaskSupervisor, fn ->
      run_phase(phase, source, target, context, options)
    end)
  end

  defp run_phase(:handshake, source, target, _context, options),
    do: Replication.handshake(source, target, options)

  defp run_phase(:read_changes, source, _target, context, options),
    do: Replication.read_changes(source, context, options)

  defp run_phase(:diff, _source, target, context, options),
    do: Replication.diff(target, context, options)

  defp run_phase(:fetch_chains, source, _target, context, options),
    do: Replication.fetch_chains(source, context, options)

  defp run_phase(:import, _source, target, context, options),
    do: Replication.import_chains(target, context, options)

  defp run_phase(:checkpoint_target, source, target, context, options),
    do: Replication.checkpoint_target(source, target, context, options)

  defp run_phase(:checkpoint_source, source, _target, context, options),
    do: Replication.checkpoint_source(source, context, options)

  defp run_phase(:waiting, source, _target, context, options),
    do: Replication.wait_for_changes(source, context, options)

  defp handle_phase_result(:handshake, {:ok, context}, data) do
    data = %{data | context: context, attempts: 0, error: nil}

    if Replication.caught_up?(context) do
      enter_phase(:checkpoint_target, data)
    else
      enter_phase(:read_changes, data)
    end
  end

  defp handle_phase_result(:read_changes, {:ok, context}, data),
    do: enter_phase(:diff, %{data | context: context})

  defp handle_phase_result(:diff, {:ok, context}, data),
    do: enter_phase(:fetch_chains, %{data | context: context})

  defp handle_phase_result(:fetch_chains, {:ok, context}, data),
    do: enter_phase(:import, %{data | context: context})

  defp handle_phase_result(:import, {:ok, context}, data),
    do: enter_phase(:checkpoint_target, %{data | context: context})

  defp handle_phase_result(:checkpoint_target, {:ok, context}, data),
    do: enter_phase(:checkpoint_source, %{data | context: context})

  defp handle_phase_result(:checkpoint_source, {:ok, context}, data) do
    data = %{data | context: context}

    case Replication.next_after_checkpoint(context, data.options) do
      {:completed, result} ->
        enter_terminal(:completed, %{data | result: result}, %{result: result})

      {:waiting, context} ->
        enter_phase(:waiting, %{data | context: context})

      {:continue_batch, context} ->
        enter_phase(:read_changes, %{data | context: context})
    end
  end

  defp handle_phase_result(:waiting, {:ok, context}, data) do
    if context.selected == [] and context.terminal <= context.since do
      enter_phase(:waiting, %{data | context: context})
    else
      enter_phase(:read_changes, %{data | context: %{context | selected: []}})
    end
  end

  defp handle_phase_result(_state, {:error, error}, data), do: handle_failure(data, error)

  defp handle_phase_result(_state, other, data) do
    handle_failure(
      data,
      ElixirDB.Error.internal_error("unexpected replication phase result", %{
        cause: inspect(other)
      })
    )
  end

  defp handle_failure(data, error) do
    if data.cancel_requested do
      stop_cancelled(data)
    else
      attempts = data.attempts + 1
      max_attempts = retry_option(data.options, :max_attempts, 8)

      if error.retryable and attempts < max_attempts do
        delay = retry_delay(data.options, attempts)
        report(data, :backoff, %{error: error, attempt: attempts, delay_ms: delay})
        notify_state(data, :backoff)

        {:next_state, :backoff, %{data | attempts: attempts, error: error, context: nil},
         [{:state_timeout, delay, :retry}]}
      else
        enter_terminal(:failed, %{data | attempts: attempts, error: error}, %{
          error: error,
          attempt: attempts
        })
      end
    end
  end

  defp stop_cancelled(data) do
    error = data.error || cancelled_error()
    enter_terminal(:failed, %{data | error: error}, %{error: error})
  end

  defp cancelled_error,
    do: ElixirDB.Error.database_closed("replication was cancelled")

  defp report(data, state, details \\ %{}) do
    job_id = data.options[:job_id] || data.options["job_id"]
    if job_id, do: ElixirDB.Replication.JobManager.report(job_id, state, details)
    :ok
  end

  defp notify_state(data, state) do
    case data.options[:state_notify] || data.options["state_notify"] do
      pid when is_pid(pid) -> send(pid, {:replication_worker_state, state})
      _ -> :ok
    end
  end

  defp normalize_options(options) when is_map(options) do
    session_id = options[:session_id] || options["session_id"] || ElixirDB.UUID.v4()
    Map.put(options, :session_id, session_id)
  end

  defp normalize_options(options) when is_list(options) do
    session_id = Keyword.get(options, :session_id, ElixirDB.UUID.v4())
    Keyword.put(options, :session_id, session_id)
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
