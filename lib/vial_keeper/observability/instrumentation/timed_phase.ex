defmodule VialKeeper.Observability.Instrumentation.TimedPhase do
  @moduledoc """
  Measures a zero-arity function and injects a closed-vocabulary phase probe.

  Attachment store and upload share this clock so each module only owns its
  phase atoms and metric names.
  """

  @type outcome :: :ok | :error

  defmacro __using__(opts) do
    metric = Keyword.fetch!(opts, :metric)
    event = Keyword.fetch!(opts, :event)

    quote do
      alias VialKeeper.Observability.Instrumentation.TimedPhase
      alias VialKeeper.Observability.Meters

      @doc "Measures one closed-vocabulary attachment phase."
      @spec phase(phase(), (-> result)) :: result when result: term()
      def phase(phase, fun) when phase in @phases and is_function(fun, 0) do
        TimedPhase.measure(fun, &emit_phase(phase, &1, &2))
      end

      @doc "Returns the closed phase vocabulary for diagnostics and tests."
      @spec phases() :: [phase()]
      def phases, do: @phases

      defp emit_phase(phase, duration, outcome) do
        Meters.record(unquote(metric), duration,
          attachment_phase: phase,
          outcome: outcome
        )

        :telemetry.execute(
          unquote(event),
          %{duration: duration},
          %{phase: phase, outcome: outcome}
        )
      end
    end
  end

  @spec measure((-> result), (integer(), outcome() -> term())) :: result when result: term()
  def measure(fun, emit) when is_function(fun, 0) and is_function(emit, 2) do
    started = System.monotonic_time()

    try do
      result = fun.()
      emit.(System.monotonic_time() - started, outcome(result))
      result
    catch
      kind, reason ->
        emit.(System.monotonic_time() - started, :error)
        :erlang.raise(kind, reason, __STACKTRACE__)
    end
  end

  defp outcome({:error, _reason}), do: :error
  defp outcome(_result), do: :ok
end
