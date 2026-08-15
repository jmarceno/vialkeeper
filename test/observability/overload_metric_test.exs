defmodule VialKeeper.Observability.OverloadMetricTest do
  @moduledoc """
  Saturate admission and assert `vial_keeper.database.overload.count`
  increments and NO span is created (overload is not a unit of work). Also
  verify migrated modules no longer emit bare telemetry events.
  """

  use VialKeeper.Observability.OtelCase, async: false

  @moduletag :integration

  alias VialKeeper.Eventual
  alias VialKeeper.Observability.{TestExporter, TestMetricExporter}
  alias VialKeeper.Runtime.{AdmissionPolicy, AdmissionSupervisor, DatabaseAdmission}

  @metric "vial_keeper.database.overload.count"

  test "saturating admission increments overload.count and creates no span" do
    uuid = VialKeeper.UUID.v4()
    limit = 1

    {:ok, policy} =
      AdmissionPolicy.from_keyword(
        Keyword.merge(
          AdmissionPolicy.default_keyword(),
          foreground_reserved_slots: 0,
          subscription_reserved_slots: 0,
          replication_reserved_slots: 0,
          maintenance_reserved_slots: 0
        ),
        limit
      )

    {:ok, _supervisor} = AdmissionSupervisor.start_link({uuid, limit, policy})

    parent = self()

    # Hold the single admission token in another process.
    holder =
      spawn_link(fn ->
        DatabaseAdmission.with_token(uuid, fn ->
          send(parent, :acquired)

          receive do
            :release -> :ok
          after
            5_000 -> :ok
          end
        end)
      end)

    assert_receive :acquired, 1_000

    # The limit is reached, so this acquisition must overload.
    assert {:error, %VialKeeper.Error{code: :database_overloaded}} =
             DatabaseAdmission.with_token(uuid, fn ->
               flunk("overloaded request must not run")
             end)

    send(holder, :release)

    ref = Process.monitor(holder)
    assert_receive {:DOWN, ^ref, :process, _, _}, 2_000

    case Registry.lookup(VialKeeper.Runtime.DatabaseRegistry, {:admission_supervisor, uuid}) do
      [{pid, _}] -> Supervisor.stop(pid)
      [] -> :ok
    end

    # The periodic test metric reader exports every 50ms; poll for the
    # datapoint carrying this db.uuid.
    Eventual.eventually(
      fn ->
        TestMetricExporter.counter_sum(@metric, %{:"db.uuid" => uuid}) >= 1
      end,
      timeout: 2_000,
      message: "overload.count did not increment for #{uuid}"
    )

    assert TestExporter.spans_named("vial_keeper.database.overload") == [],
           "overload must be a counter only, not a span"
  end

  test "no bare telemetry emitter remains in the migrated modules" do
    # Migrated modules must not keep bare :telemetry.execute emitters; OTel
    # instrumentation owns overload and admission observability instead.
    for file <- [
          "lib/vial_keeper/runtime/database_admission.ex",
          "lib/vial_keeper/replication.ex"
        ] do
      source = File.read!(file)

      refute String.contains?(source, ":telemetry.execute"),
             "#{file} must not contain a bare :telemetry.execute (migrated to OTel)"
    end
  end
end
