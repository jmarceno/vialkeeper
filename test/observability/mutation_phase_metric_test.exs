defmodule VialKeeper.Observability.MutationPhaseMetricTest do
  use VialKeeper.Observability.OtelCase, async: false

  alias VialKeeper.Eventual
  alias VialKeeper.Observability.Instrumentation.Mutation
  alias VialKeeper.Observability.TestMetricExporter

  @event [:vial_keeper, :document, :mutation, :phase]
  @metric "vial_keeper.document.mutation.phase.duration"

  test "emits only the closed mutation dimensions" do
    handler_id = {__MODULE__, make_ref()}
    parent = self()

    :ok =
      :telemetry.attach(
        handler_id,
        @event,
        fn event, measurements, metadata, pid ->
          send(pid, {:mutation_phase, event, measurements, metadata})
        end,
        parent
      )

    on_exit(fn -> :telemetry.detach(handler_id) end)

    assert :ok = Mutation.phase(:put, :canonical_encode, fn -> :ok end)

    assert_receive {:mutation_phase, @event, %{duration: duration}, metadata}
    assert is_integer(duration) and duration >= 0
    assert metadata == %{operation: :put, phase: :canonical_encode, outcome: :ok}

    Eventual.eventually(
      fn ->
        TestMetricExporter.datapoints_matching(@metric, %{
          mutation_operation: :put,
          mutation_phase: :canonical_encode,
          outcome: :ok
        }) != []
      end,
      message: "expected a document mutation phase datapoint"
    )
  end

  test "restores nested process-local operation state" do
    assert :ok =
             Mutation.with_operation(:put, fn ->
               Mutation.with_operation(:bulk_write, fn -> :ok end)
               Mutation.phase(:fact_reads, fn -> :ok end)
             end)

    Eventual.eventually(
      fn ->
        TestMetricExporter.datapoints_matching(@metric, %{
          mutation_operation: :put,
          mutation_phase: :fact_reads
        }) != []
      end,
      message: "expected the outer mutation operation to be restored"
    )
  end
end
