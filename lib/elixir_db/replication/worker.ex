defmodule ElixirDB.Replication.Worker do
  @moduledoc """
  Supervised replication state machine with cancellable bounded work.

  States: idle, handshake, read_changes, diff, transfer,
  import, checkpoint_target, checkpoint_source, waiting, backoff, completed, failed.

  Cancellation (REPL-018) is checked between phases after a phase Task completes.
  In-flight endpoint work is allowed to finish; the worker never brutal-kills a
  task mid-import or mid-checkpoint commit.

  `:completed` and `:failed` are brief real gen_statem states entered before stop
  so transition and fault tests observe the complete state sequence.
  """
  @behaviour :gen_statem

  require OpenTelemetry.Tracer

  alias ElixirDB.MapAccess
  alias ElixirDB.Observability.Attributes
  alias ElixirDB.Observability.Meters
  alias ElixirDB.Replication
  alias ElixirDB.Replication.JobManager
  alias ElixirDB.Replication.TransferPipeline
  alias ElixirDB.Runtime.ChildSpec

  @active_states [
    :idle,
    :handshake,
    :install_boundaries,
    :bootstrap,
    :read_changes,
    :diff,
    :transfer,
    :import,
    :checkpoint_target,
    :checkpoint_source,
    :report_peer,
    :waiting,
    :backoff
  ]

  @terminal_states [:completed, :failed]

  def start_link(options), do: :gen_statem.start_link(__MODULE__, options, [])

  def child_spec(options) do
    ChildSpec.worker(
      {:replication_worker, MapAccess.get(options, :replication_id, make_ref())},
      {__MODULE__, :start_link, [options]},
      :temporary
    )
  end

  @impl true
  def callback_mode, do: :handle_event_function

  @impl true
  def init(options) do
    replication_id = MapAccess.get(options, :replication_id)

    with true <- is_binary(replication_id),
         {:ok, _} <- Registry.register(ElixirDB.Replication.WorkerRegistry, replication_id, %{}) do
      data = %{
        options: normalize_options(options),
        attempts: 0,
        result: nil,
        error: nil,
        cancel_requested: false,
        context: nil,
        task: nil,
        # OTel span context for the current replication batch (started in the
        # worker process so phase tasks inherit it), or nil between batches.
        batch_span_ctx: nil,
        # monotonic time the current batch started (native), or nil.
        batch_started: nil,
        # revisions written in the current batch's import phase (captured
        # before checkpoint_source resets context.imported).
        batch_revisions: 0
      }

      # Notify the initial :idle state so observers see the full state sequence from
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
      if state == :transfer do
        # The transfer coordinator owns its private supervisor and performs the
        # bounded child cleanup before returning its phase result.
        _ = TransferPipeline.cancel(data.task.pid)
      end

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
          ElixirDB.Error.internal_error("replication task crashed")
        end

      handle_failure(data, error)
    end
  end

  def handle_event(:info, _event, _state, data), do: {:keep_state, data}
  def handle_event(_type, _event, _state, data), do: {:keep_state, data}

  @impl true
  def terminate(_reason, state, _data) when state in @terminal_states, do: :ok

  def terminate(_reason, state, data) do
    cleanup_phase_task(state, data)

    # End any in-flight batch span so it doesn't leak.
    if data.batch_span_ctx do
      replication_id = MapAccess.get(data.options, :replication_id)
      _ = end_batch_span(data, replication_id, data.batch_revisions, :ok)
    end

    state = if data.error, do: :failed, else: if(data.result, do: :completed, else: :failed)
    report(data, state, %{result: data.result, error: data.error})
    :ok
  end

  defp cleanup_phase_task(:transfer, %{task: %{pid: phase_pid}}) when is_pid(phase_pid) do
    # Transfer owns a linked private supervisor. Ask it to perform its normal
    # bounded cleanup first, then stop the phase task so worker shutdown cannot
    # strand children. Committing phases intentionally remain cooperative.
    ref = Process.monitor(phase_pid)

    _ = TransferPipeline.cancel(phase_pid)

    case await_phase_down(ref) do
      :down ->
        :ok

      :timeout ->
        _ = Process.exit(phase_pid, :shutdown)
        _ = await_phase_down(ref)
        :ok
    end

    :ok
  end

  defp cleanup_phase_task(_state, _data), do: :ok

  defp await_phase_down(ref) do
    receive do
      {:DOWN, ^ref, :process, _pid, _reason} -> :down
    after
      5_000 -> :timeout
    end
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

    # Capture the current OTel context so the async phase task is a child of
    # the worker's trace. Without detach/attach the task would have
    # no parent and the trace would break across the Task.Supervisor boundary.
    ctx = OpenTelemetry.Ctx.get_current()

    Task.Supervisor.async_nolink(ElixirDB.TaskSupervisor, fn ->
      token = OpenTelemetry.Ctx.attach(ctx)

      try do
        run_phase(phase, source, target, context, options)
      after
        OpenTelemetry.Ctx.detach(token)
      end
    end)
  end

  defp run_phase(:handshake, source, target, _context, options),
    do: Replication.handshake(source, target, options)

  defp run_phase(:install_boundaries, source, target, context, options),
    do: Replication.install_boundaries(source, target, context, options)

  defp run_phase(:bootstrap, source, target, context, options),
    do: Replication.bootstrap(source, target, context, options)

  defp run_phase(:read_changes, source, _target, context, options),
    do: Replication.read_changes(source, context, options)

  defp run_phase(:diff, _source, target, context, options),
    do: Replication.diff(target, context, options)

  defp run_phase(:transfer, source, target, context, options),
    do: Replication.transfer(source, target, context, options)

  defp run_phase(:import, _source, target, context, options),
    do: Replication.import_chains(target, context, options)

  defp run_phase(:checkpoint_target, source, target, context, options),
    do: Replication.checkpoint_target(source, target, context, options)

  defp run_phase(:checkpoint_source, source, _target, context, options),
    do: Replication.checkpoint_source(source, context, options)

  defp run_phase(:report_peer, source, target, context, options),
    do: Replication.report_peer(source, target, context, options)

  defp run_phase(:waiting, source, _target, context, options),
    do: Replication.wait_for_changes(source, context, options)

  defp handle_phase_result(:handshake, {:ok, context}, data) do
    data = %{data | context: context, attempts: 0, error: nil}

    cond do
      context.bootstrap_required ->
        data = start_batch_span(data)
        enter_phase(:install_boundaries, data)

      context.boundary_refresh_required ->
        data = start_batch_span(data)
        enter_phase(:install_boundaries, data)

      Replication.caught_up?(context) ->
        data = start_batch_span(data)
        enter_phase(:checkpoint_target, data)

      true ->
        data = start_batch_span(data)
        enter_phase(:read_changes, data)
    end
  end

  defp handle_phase_result(:install_boundaries, {:ok, context}, data) do
    cond do
      context.bootstrap_required ->
        enter_phase(:bootstrap, %{data | context: context})

      Replication.caught_up?(context) ->
        enter_phase(:checkpoint_target, %{data | context: context})

      true ->
        enter_phase(:read_changes, %{data | context: context})
    end
  end

  defp handle_phase_result(:bootstrap, {:ok, context}, data),
    do:
      enter_phase(:checkpoint_target, %{
        data
        | context: context,
          batch_revisions: context_imported_count(context)
      })

  defp handle_phase_result(:read_changes, {:ok, context}, data) do
    if context.bootstrap_required do
      enter_phase(:install_boundaries, %{data | context: context})
    else
      enter_phase(:diff, %{data | context: context})
    end
  end

  defp handle_phase_result(:diff, {:ok, context}, data),
    do: enter_phase(:transfer, %{data | context: context})

  defp handle_phase_result(:transfer, {:ok, context}, data),
    do: enter_phase(:import, %{data | context: context})

  defp handle_phase_result(:import, {:ok, context}, data),
    do:
      enter_phase(:checkpoint_target, %{
        data
        | context: context,
          # Capture revisions written before checkpoint_source resets imported.
          batch_revisions: context_imported_count(context)
      })

  defp handle_phase_result(:checkpoint_target, {:ok, context}, data),
    do: enter_phase(:checkpoint_source, %{data | context: context})

  defp handle_phase_result(:checkpoint_source, {:ok, context}, data),
    do: enter_phase(:report_peer, %{data | context: context})

  defp handle_phase_result(:report_peer, {:ok, context}, data) do
    replication_id = MapAccess.get(data.options, :replication_id)

    data = end_batch_span(data, replication_id, data.batch_revisions, :ok)
    data = %{data | context: context}

    case Replication.next_after_checkpoint(context, data.options) do
      {:completed, result} ->
        enter_terminal(:completed, %{data | result: result}, %{result: result})

      {:waiting, context} ->
        enter_phase(:waiting, %{data | context: context})

      {:continue_batch, context} ->
        # Start a new batch span for the next batch.
        enter_phase(:read_changes, start_batch_span(%{data | context: context}))
    end
  end

  defp handle_phase_result(:waiting, {:ok, context}, data) do
    if context.selected == [] and context.terminal <= context.since do
      enter_phase(:waiting, %{data | context: context})
    else
      enter_phase(:read_changes, start_batch_span(%{data | context: %{context | selected: []}}))
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
    # Emit the batch span/metric on failure too. Uses the revisions captured at
    # import if any.
    replication_id = MapAccess.get(data.options, :replication_id)
    data = end_batch_span(data, replication_id, data.batch_revisions, {:error, error})

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
    job_id = MapAccess.get(data.options, :job_id)
    if job_id, do: JobManager.report(job_id, state, details)
    :ok
  end

  defp notify_state(data, state) do
    case MapAccess.get(data.options, :state_notify) do
      pid when is_pid(pid) -> send(pid, {:replication_worker_state, state})
      _ -> :ok
    end
  end

  # Starts a replication.batch span in the worker process and makes it current
  # so async phase tasks inherit the trace context. Returns updated
  # data with the span ctx and a native monotonic start timestamp. No-op when
  # the SDK is absent (start_span returns a non-recording span ctx).
  defp start_batch_span(data) do
    replication_id = MapAccess.get(data.options, :replication_id)

    span_ctx =
      OpenTelemetry.Tracer.start_span("elixir_db.replication.batch", %{
        kind: :internal,
        attributes: Attributes.build(replication_id: replication_id)
      })

    OpenTelemetry.Tracer.set_current_span(span_ctx)

    %{
      data
      | batch_span_ctx: span_ctx,
        batch_started: System.monotonic_time(),
        batch_revisions: 0
    }
  end

  # Ends the current batch span (if any), records the duration histogram and,
  # on error, sets error.code + status per §6.5. Resets batch fields.
  defp end_batch_span(%{batch_span_ctx: nil} = data, _replication_id, _revisions, _outcome),
    do: data

  defp end_batch_span(data, replication_id, revisions, outcome) do
    span_ctx = data.batch_span_ctx
    duration = System.monotonic_time() - (data.batch_started || System.monotonic_time())

    OpenTelemetry.Tracer.set_current_span(span_ctx)

    case outcome do
      :ok ->
        Meters.record(:"elixir_db.replication.batch.duration", duration,
          replication_id: replication_id,
          revisions_written: revisions
        )

      {:error, %ElixirDB.Error{} = error} ->
        Meters.record(:"elixir_db.replication.batch.duration", duration,
          replication_id: replication_id,
          error_code: error.code
        )

        _ =
          ElixirDB.Observability.Tracer.set_attributes(error_code: error.code)

        _ = ElixirDB.Observability.Tracer.apply_error_status(error)
    end

    OpenTelemetry.Span.end_span(span_ctx)
    # Reset current span to none so the worker process doesn't leak it.
    OpenTelemetry.Tracer.set_current_span(:undefined)

    %{data | batch_span_ctx: nil, batch_started: nil, batch_revisions: 0}
  end

  defp context_imported_count(%{imported: %{revisions_inserted: count}})
       when is_integer(count),
       do: count

  defp context_imported_count(%{imported: %{"revisions_inserted" => count}})
       when is_integer(count),
       do: count

  defp context_imported_count(_), do: 0

  defp normalize_options(options) when is_map(options) do
    session_id = MapAccess.get(options, :session_id) || ElixirDB.UUID.v4()
    Map.put(options, :session_id, session_id)
  end

  defp normalize_options(options) when is_list(options) do
    session_id = Keyword.get(options, :session_id, ElixirDB.UUID.v4())
    Keyword.put(options, :session_id, session_id)
  end

  defp retry_option(options, key, default) do
    retry = MapAccess.get(options, :retry, %{})
    MapAccess.get(retry, key) || default
  end

  defp retry_delay(options, attempt) do
    base = retry_option(options, :base_delay_ms, 100)
    maximum = retry_option(options, :max_delay_ms, 30_000)
    jitter = retry_option(options, :jitter_ms, 250)
    deterministic_jitter = :erlang.phash2({options[:replication_id], attempt}, jitter + 1)
    min(maximum, trunc(base * :math.pow(2, attempt - 1)) + deterministic_jitter)
  end
end
