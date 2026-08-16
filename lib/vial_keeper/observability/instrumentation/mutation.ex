defmodule VialKeeper.Observability.Instrumentation.Mutation do
  @moduledoc """
  Low-cardinality phase measurements for document mutations.

  The operation and phase vocabularies are closed so this instrumentation cannot
  acquire document identifiers, bodies, attachment names, or other customer data.
  Durations are native monotonic-time deltas.
  """

  alias VialKeeper.Observability.Meters

  @operation_key {__MODULE__, :operation}
  @operations [:put, :delete, :resolve, :bulk_write, :import]
  @phases [
    :validation,
    :canonical_encode,
    :strict_decode,
    :attachment_manifest,
    :catalog_route,
    :owner_queue,
    :transaction_begin,
    :fact_reads,
    :revision_hash,
    :revision_writes,
    :change_log,
    :attachment_metadata,
    :transaction_commit,
    :search_flush,
    :change_notifier
  ]

  @typedoc "A bounded document-mutation operation name."
  @type operation :: :put | :delete | :resolve | :bulk_write | :import

  @typedoc "A bounded document-mutation phase name."
  @type phase ::
          :validation
          | :canonical_encode
          | :strict_decode
          | :attachment_manifest
          | :catalog_route
          | :owner_queue
          | :transaction_begin
          | :fact_reads
          | :revision_hash
          | :revision_writes
          | :change_log
          | :attachment_metadata
          | :transaction_commit
          | :search_flush
          | :change_notifier

  @doc "Runs `fun` while making `operation` available to nested phase probes."
  @spec with_operation(operation(), (-> result)) :: result when result: term()
  def with_operation(operation, fun) when operation in @operations and is_function(fun, 0) do
    previous = Process.get(@operation_key)
    Process.put(@operation_key, operation)

    try do
      fun.()
    after
      restore_operation(previous)
    end
  end

  @doc "Measures a phase using the process-local mutation operation."
  @spec phase(phase(), (-> result)) :: result when result: term()
  def phase(phase, fun) when phase in @phases and is_function(fun, 0) do
    case Process.get(@operation_key) do
      operation when operation in @operations -> phase(operation, phase, fun)
      _other -> fun.()
    end
  end

  @doc "Measures one mutation phase and returns the result of `fun`."
  @spec phase(operation(), phase(), (-> result)) :: result when result: term()
  def phase(operation, phase, fun)
      when operation in @operations and phase in @phases and is_function(fun, 0) do
    started = System.monotonic_time()

    try do
      result = fun.()
      record(operation, phase, System.monotonic_time() - started, outcome(result))
      result
    catch
      kind, reason ->
        record(operation, phase, System.monotonic_time() - started, :error)
        :erlang.raise(kind, reason, __STACKTRACE__)
    end
  end

  @doc "Records an already measured phase duration."
  @spec record(operation(), phase(), non_neg_integer(), :ok | :error) :: :ok
  def record(operation, phase, duration, outcome \\ :ok)

  def record(operation, phase, duration, outcome)
      when operation in @operations and phase in @phases and is_integer(duration) and
             duration >= 0 and outcome in [:ok, :error] do
    attrs = [mutation_operation: operation, mutation_phase: phase, outcome: outcome]
    Meters.record(:"vial_keeper.document.mutation.phase.duration", duration, attrs)

    :telemetry.execute(
      [:vial_keeper, :document, :mutation, :phase],
      %{duration: duration},
      %{operation: operation, phase: phase, outcome: outcome}
    )

    :ok
  end

  @doc "Returns the closed phase vocabulary for diagnostics and tests."
  @spec phases() :: [phase()]
  def phases, do: @phases

  defp outcome({:error, _reason}), do: :error
  defp outcome(_result), do: :ok

  defp restore_operation(nil), do: Process.delete(@operation_key)
  defp restore_operation(previous), do: Process.put(@operation_key, previous)
end
