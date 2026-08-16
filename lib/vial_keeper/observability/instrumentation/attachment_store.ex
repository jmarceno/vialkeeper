defmodule VialKeeper.Observability.Instrumentation.AttachmentStore do
  @moduledoc """
  Low-cardinality phase measurements for physical attachment writes.

  Phase metadata is a closed atom vocabulary and never includes paths, digests,
  attachment names, content types, or payload data.
  """

  alias VialKeeper.Observability.Meters

  @phases [
    :begin,
    :logical_hash,
    :compression_probe,
    :payload_write,
    :compression_finalize,
    :digest_finalize,
    :trailer_write,
    :file_sync,
    :file_close,
    :cas_install
  ]

  @typedoc "A bounded physical attachment-write phase."
  @type phase ::
          :begin
          | :logical_hash
          | :compression_probe
          | :payload_write
          | :compression_finalize
          | :digest_finalize
          | :trailer_write
          | :file_sync
          | :file_close
          | :cas_install

  @doc "Measures a physical attachment-write phase."
  @spec phase(phase(), (-> result)) :: result when result: term()
  def phase(phase, fun) when phase in @phases and is_function(fun, 0) do
    started = System.monotonic_time()

    try do
      result = fun.()
      emit(phase, System.monotonic_time() - started, outcome(result))
      result
    catch
      kind, reason ->
        emit(phase, System.monotonic_time() - started, :error)
        :erlang.raise(kind, reason, __STACKTRACE__)
    end
  end

  @doc "Returns the closed phase vocabulary for diagnostics and tests."
  @spec phases() :: [phase()]
  def phases, do: @phases

  defp emit(phase, duration, outcome) do
    Meters.record(:"vial_keeper.attachment.store.phase.duration", duration,
      attachment_phase: phase,
      outcome: outcome
    )

    :telemetry.execute(
      [:vial_keeper, :attachment, :store, :phase],
      %{duration: duration},
      %{phase: phase, outcome: outcome}
    )
  end

  defp outcome({:error, _reason}), do: :error
  defp outcome(_result), do: :ok
end
