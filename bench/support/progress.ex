defmodule VialKeeper.Bench.Progress do
  @moduledoc """
  Heartbeat and stall watchdog for long dataset-backed Mix benchmarks.

  Callers start one server per run and tick after each completed unit of work.
  The server prints at phase boundaries, every 10% (5% when a phase has fewer
  than 20 units), and on a 30-second heartbeat when percent has not moved.
  When a countable phase (`total > 1` and `processed < total`) receives no
  tick within the stall timeout, it prints a diagnostic dump, runs optional
  cleanup, and exits the owner process so a hung upload cannot sit silent.
  """

  use GenServer

  @call_timeout 5_000
  @default_stall_ms 300_000
  @default_heartbeat_ms 30_000
  @default_check_ms 1_000
  @large_percent_step 10
  @small_percent_step 5
  @small_total 20

  @type snapshot :: %{
          optional(:last_item_ms) => non_neg_integer(),
          optional(:last_tick_age_ms) => non_neg_integer(),
          optional(:peak_rss_bytes) => non_neg_integer(),
          optional(:phase) => binary() | nil,
          optional(:processed) => non_neg_integer(),
          optional(:rate_per_sec) => float() | nil,
          optional(:ticks) => non_neg_integer(),
          optional(:total) => non_neg_integer()
        }

  defstruct owner: nil,
            label: "bench",
            printer: nil,
            cleanup: nil,
            stall_timeout_ms: @default_stall_ms,
            heartbeat_ms: @default_heartbeat_ms,
            check_interval_ms: @default_check_ms,
            percent_step: @large_percent_step,
            phase: nil,
            total: 0,
            processed: 0,
            ticks: 0,
            last_printed_percent: 0,
            last_print_ms: 0,
            last_tick_ms: 0,
            phase_started_ms: 0,
            last_item_ms: 0,
            peak_rss_bytes: 0

  @spec default_stall_timeout_ms() :: pos_integer()
  def default_stall_timeout_ms, do: @default_stall_ms

  @spec with_run(keyword(), (pid() -> result)) :: result when result: var
  def with_run(opts, fun) when is_list(opts) and is_function(fun, 1) do
    {:ok, pid} = start(Keyword.put_new(opts, :owner, self()))

    try do
      fun.(pid)
    after
      stop(pid)
    end
  end

  @spec start(keyword()) :: {:ok, pid()} | {:error, term()}
  def start(opts) when is_list(opts) do
    GenServer.start(__MODULE__, Keyword.put_new(opts, :owner, self()))
  end

  @spec stop(pid() | nil) :: :ok
  def stop(nil), do: :ok

  def stop(pid) when is_pid(pid) do
    if Process.alive?(pid) do
      try do
        GenServer.stop(pid, :normal, @call_timeout)
      catch
        :exit, _reason -> :ok
      end
    else
      :ok
    end
  end

  @spec phase(pid() | nil, binary(), non_neg_integer()) :: :ok
  def phase(nil, _name, _total), do: :ok

  def phase(pid, name, total)
      when is_pid(pid) and is_binary(name) and is_integer(total) and total >= 0 do
    GenServer.call(pid, {:phase, name, total}, @call_timeout)
  end

  @spec tick(pid() | nil, pos_integer()) :: :ok
  def tick(pid, count \\ 1)
  def tick(nil, _count), do: :ok

  def tick(pid, count) when is_pid(pid) and is_integer(count) and count > 0 do
    GenServer.cast(pid, {:tick, count, now_ms()})
  end

  @spec report(pid() | nil, binary(), non_neg_integer(), non_neg_integer()) :: :ok
  def report(nil, _phase, _processed, _total), do: :ok

  def report(pid, phase, processed, total)
      when is_pid(pid) and is_binary(phase) and is_integer(processed) and processed >= 0 and
             is_integer(total) and total >= 0 do
    GenServer.cast(pid, {:report, phase, processed, total, now_ms()})
  end

  @spec complete(pid() | nil) :: :ok
  def complete(nil), do: :ok

  def complete(pid) when is_pid(pid) do
    GenServer.call(pid, :complete, @call_timeout)
  end

  @spec snapshot(pid()) :: snapshot()
  def snapshot(pid) when is_pid(pid) do
    GenServer.call(pid, :snapshot, @call_timeout)
  end

  @impl true
  def init(opts) do
    owner = Keyword.fetch!(opts, :owner)
    Process.monitor(owner)
    now = now_ms()

    state = %__MODULE__{
      owner: owner,
      label: Keyword.get(opts, :label, "bench"),
      printer: Keyword.get(opts, :printer),
      cleanup: Keyword.get(opts, :cleanup),
      stall_timeout_ms: stall_timeout(opts),
      heartbeat_ms: Keyword.get(opts, :heartbeat_ms, @default_heartbeat_ms),
      check_interval_ms: Keyword.get(opts, :check_interval_ms, @default_check_ms),
      last_print_ms: now,
      last_tick_ms: now,
      peak_rss_bytes: rss_bytes()
    }

    {:ok, schedule(state)}
  end

  @impl true
  def handle_call({:phase, name, total}, _from, state) do
    {:reply, :ok, enter_phase(state, name, total, now_ms())}
  end

  def handle_call(:complete, _from, state) do
    {:reply, :ok, finish_phase(state, now_ms())}
  end

  def handle_call(:snapshot, _from, state) do
    {:reply, snapshot_map(state, now_ms()), state}
  end

  @impl true
  def handle_cast({:tick, count, now}, state) do
    {:noreply, apply_tick(state, state.processed + count, count, now)}
  end

  def handle_cast({:report, phase, processed, total, now}, state) do
    state =
      if state.phase != phase do
        enter_phase(state, phase, total, now)
      else
        %{state | total: total, percent_step: percent_step(total)}
      end

    delta = max(processed - state.processed, 0)
    {:noreply, apply_tick(state, processed, max(delta, 1), now)}
  end

  @impl true
  def handle_info(:check, state) do
    now = now_ms()
    state = %{state | peak_rss_bytes: max(state.peak_rss_bytes, rss_bytes())}

    cond do
      stalled?(state, now) ->
        abort_stall(state, now)

      heartbeat_due?(state, now) ->
        {:noreply, schedule(print_line(state, now, "heartbeat"))}

      true ->
        {:noreply, schedule(state)}
    end
  end

  def handle_info({:DOWN, _ref, :process, pid, _reason}, %{owner: pid} = state) do
    {:stop, :normal, state}
  end

  def handle_info(_message, state), do: {:noreply, state}

  defp enter_phase(state, name, total, now) do
    state = finish_phase(state, now)

    next = %{
      state
      | phase: name,
        total: total,
        processed: 0,
        ticks: 0,
        last_printed_percent: 0,
        last_tick_ms: now,
        phase_started_ms: now,
        last_item_ms: 0,
        percent_step: percent_step(total)
    }

    emit(next, :info, "#{prefix(next)} start #{name} total=#{format_total(total)}")
    %{next | last_print_ms: now}
  end

  defp finish_phase(%{phase: nil} = state, _now), do: state

  defp finish_phase(state, now) do
    %{print_line(state, now, "done") | phase: nil}
  end

  defp apply_tick(state, processed, count, now) do
    last_item_ms = max(div(now - state.last_tick_ms, max(count, 1)), 0)

    next = %{
      state
      | processed: processed,
        ticks: state.ticks + 1,
        last_tick_ms: now,
        last_item_ms: last_item_ms,
        peak_rss_bytes: max(state.peak_rss_bytes, rss_bytes())
    }

    percent = percent(next.processed, next.total)

    cond do
      next.total > 0 and next.processed >= next.total and next.last_printed_percent < 100 ->
        print_line(next, now, "progress")

      percent >= next.last_printed_percent + next.percent_step ->
        print_line(next, now, "progress")

      next.total == 0 and next.processed > 0 and rem(next.processed, 5_000) == 0 ->
        print_line(next, now, "progress")

      true ->
        next
    end
  end

  defp stalled?(state, now) do
    is_integer(state.stall_timeout_ms) and state.stall_timeout_ms > 0 and is_binary(state.phase) and
      state.total > 1 and state.processed < state.total and
      now - state.last_tick_ms >= state.stall_timeout_ms
  end

  defp heartbeat_due?(state, now) do
    is_binary(state.phase) and now - state.last_print_ms >= state.heartbeat_ms
  end

  defp abort_stall(state, now) do
    message = stall_message(state, now)
    emit(state, :error, message)
    run_cleanup(state)
    _ = Process.exit(state.owner, {:bench_stall, message})
    {:stop, :normal, state}
  end

  defp stall_message(state, now) do
    age = now - state.last_tick_ms
    snap = snapshot_map(state, now)

    """
    [#{state.label}] STALL: no progress for #{format_ms(age)}s
      phase=#{state.phase} processed=#{state.processed}/#{format_total(state.total)} (#{percent(state.processed, state.total)}%)
      elapsed=#{format_ms(now - state.phase_started_ms)}s rate=#{format_rate(snap.rate_per_sec)}/s
      last_item=#{format_ms(state.last_item_ms)}s last_tick_age=#{format_ms(age)}s
      ticks=#{state.ticks} peak_rss=#{format_mib(state.peak_rss_bytes)}
      the last unit of work did not complete; inspect disk, memory, and the current attachment/document call
    """
  end

  defp print_line(state, now, tag) do
    percent = percent(state.processed, state.total)
    elapsed_ms = max(now - state.phase_started_ms, 0)
    rate = rate_per_sec(state.processed, elapsed_ms)
    remaining = remaining_ms(state, elapsed_ms)

    line =
      "#{prefix(state)} #{state.phase} #{state.processed}/#{format_total(state.total)} " <>
        "(#{percent}%) elapsed=#{format_ms(elapsed_ms)}s rate=#{format_rate(rate)}/s " <>
        "eta=#{format_ms(remaining)}s last_item=#{format_ms(state.last_item_ms)}s " <>
        "rss=#{format_mib(state.peak_rss_bytes)}" <> tag_suffix(tag)

    emit(state, :info, line)

    %{
      state
      | last_print_ms: now,
        last_printed_percent: percent,
        peak_rss_bytes: max(state.peak_rss_bytes, rss_bytes())
    }
  end

  defp snapshot_map(state, now) do
    elapsed_ms = max(now - state.phase_started_ms, 0)

    %{
      phase: state.phase,
      processed: state.processed,
      total: state.total,
      ticks: state.ticks,
      last_item_ms: state.last_item_ms,
      last_tick_age_ms: now - state.last_tick_ms,
      peak_rss_bytes: state.peak_rss_bytes,
      rate_per_sec: rate_per_sec(state.processed, elapsed_ms)
    }
  end

  defp percent(_processed, total) when total <= 0, do: 0
  defp percent(processed, total), do: min(100, div(processed * 100, total))

  defp percent_step(total) when total > 0 and total < @small_total, do: @small_percent_step
  defp percent_step(_total), do: @large_percent_step

  defp remaining_ms(%{processed: processed}, _elapsed) when processed <= 0, do: 0

  defp remaining_ms(%{total: total}, _elapsed) when total <= 0, do: 0

  defp remaining_ms(state, elapsed_ms) do
    remaining = max(state.total - state.processed, 0)
    round(elapsed_ms / state.processed * remaining)
  end

  defp rate_per_sec(processed, elapsed_ms) when processed > 0 and elapsed_ms > 0 do
    Float.round(processed * 1000 / elapsed_ms, 2)
  end

  defp rate_per_sec(_processed, _elapsed_ms), do: nil

  defp stall_timeout(opts) do
    case Keyword.get(opts, :stall_timeout_ms, @default_stall_ms) do
      value when is_integer(value) and value >= 0 -> value
      :infinity -> 0
      _other -> @default_stall_ms
    end
  end

  defp schedule(state) do
    _ = Process.send_after(self(), :check, state.check_interval_ms)
    state
  end

  defp run_cleanup(%{cleanup: fun}) when is_function(fun, 0), do: fun.()
  defp run_cleanup(_state), do: :ok

  defp emit(state, level, message) do
    case state.printer do
      fun when is_function(fun, 2) ->
        fun.(level, message)

      _ ->
        shell = Mix.shell()

        case level do
          :error -> shell.error(message)
          _ -> shell.info(message)
        end
    end
  end

  defp prefix(state), do: "[#{state.label}]"

  defp tag_suffix("progress"), do: ""
  defp tag_suffix(tag), do: " #{tag}"

  defp format_total(total) when total > 0, do: Integer.to_string(total)
  defp format_total(_total), do: "?"

  defp format_ms(ms) when is_integer(ms) and ms >= 0, do: Float.round(ms / 1000, 1)
  defp format_ms(_ms), do: 0.0

  defp format_rate(nil), do: "?"
  defp format_rate(rate), do: rate

  defp format_mib(bytes) when is_integer(bytes) and bytes > 0 do
    "#{Float.round(bytes / 1_048_576, 1)}MiB"
  end

  defp format_mib(_bytes), do: "?MiB"

  defp rss_bytes do
    case File.read("/proc/self/status") do
      {:ok, contents} ->
        case Regex.run(~r/^VmHWM:\s+(\d+)\s+kB$/m, contents, capture: :all_but_first) do
          [kilobytes] -> String.to_integer(kilobytes) * 1024
          _ -> :erlang.memory(:total)
        end

      {:error, _reason} ->
        :erlang.memory(:total)
    end
  end

  defp now_ms, do: System.monotonic_time(:millisecond)
end
