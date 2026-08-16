defmodule VialKeeper.Observability.Instrumentation.AttachmentUpload do
  @moduledoc """
  Low-cardinality phase measurements for attachment upload orchestration.

  Phase metadata is a closed atom vocabulary and contains no database UUID,
  path, digest, attachment name, content type, or payload data.
  """

  alias VialKeeper.Observability.Meters

  @phases [
    :open_check,
    :writable_check,
    :bundle_lookup,
    :coordinator_wait,
    :physical_store,
    :pending_protection
  ]

  @typedoc "A bounded attachment-upload orchestration phase."
  @type phase ::
          :open_check
          | :writable_check
          | :bundle_lookup
          | :coordinator_wait
          | :physical_store
          | :pending_protection

  @doc "Measures an attachment-upload orchestration phase."
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
    Meters.record(:"vial_keeper.attachment.upload.phase.duration", duration,
      attachment_phase: phase,
      outcome: outcome
    )

    :telemetry.execute(
      [:vial_keeper, :attachment, :upload, :phase],
      %{duration: duration},
      %{phase: phase, outcome: outcome}
    )
  end

  defp outcome({:error, _reason}), do: :error
  defp outcome(_result), do: :ok
end
