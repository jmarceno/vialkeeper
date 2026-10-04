defmodule VialKeeper.ProbeTest do
  # Probes are node-global counters; tiers are toggled here, so run alone.
  use ExUnit.Case, async: false

  require VialKeeper.Probe

  alias VialKeeper.Observability.Instrumentation.{Mutation, SQLite}
  alias VialKeeper.Probe

  setup do
    tiers = Probe.enabled_tiers()

    on_exit(fn ->
      for tier <- [:standard, :detail] do
        if tier in tiers, do: Probe.enable(tier), else: Probe.disable(tier)
      end
    end)

    :ok
  end

  test "phase probe vocabularies match the instrumentation vocabularies" do
    assert Enum.map(Mutation.phases(), &Probe.mutation_probe/1) ==
             Keyword.fetch!(Probe.areas(), :mutation)

    assert SQLite.phases() |> Enum.map(&Probe.sqlite_phase_probe/1) |> Enum.sort() ==
             Probe.areas() |> Keyword.fetch!(:sqlite_phase) |> Enum.sort()
  end

  test "a standard probe records count, duration and a histogram bucket" do
    :ok = Probe.enable(:standard)
    before = Probe.snapshot()

    for _ <- 1..3 do
      :ok =
        Probe.measure :catalog_route do
          Process.sleep(2)
        end
    end

    assert %{catalog_route: stats} = Probe.diff(before, Probe.snapshot())
    assert stats.count == 3
    assert stats.total_ns >= 6_000_000
    assert Enum.sum(stats.buckets) == 3

    assert %{"catalog_route" => summary} = Probe.summarize(%{catalog_route: stats})
    assert %{"area" => "runtime", "tier" => "standard", "count" => 3} = summary
    assert summary["p50_le_ns"] in Probe.bucket_bounds_ns()
    assert summary["p50_le_ns"] >= 2_000_000
  end

  test "detail probes record only while the detail tier is enabled" do
    :ok = Probe.disable(:detail)
    before = Probe.snapshot()

    value =
      Probe.measure :sqlite_step do
        :value
      end

    assert value == :value

    refute Map.has_key?(Probe.diff(before, Probe.snapshot()), :sqlite_step)

    :ok = Probe.enable(:detail)
    assert :detail in Probe.enabled_tiers()

    :ok =
      Probe.measure :sqlite_step do
        :ok
      end

    assert %{sqlite_step: %{count: 1}} = Probe.diff(before, Probe.snapshot())
  end

  test "pre-measured durations are recorded under standard probes" do
    :ok = Probe.enable(:standard)
    before = Probe.snapshot()
    duration = System.convert_time_unit(3_000, :nanosecond, :native)

    :ok = Probe.add(:mutation_validation, duration)

    assert %{mutation_validation: %{count: 1, total_ns: 3_000}} =
             Probe.diff(before, Probe.snapshot())

    :ok = Probe.disable(:standard)
    :ok = Probe.add(:mutation_validation, duration)
    assert Probe.diff(before, Probe.snapshot()).mutation_validation.count == 1
  end

  test "an unknown probe name fails at compile time" do
    assert_raise CompileError, ~r/unknown performance probe :not_a_probe/, fn ->
      Code.eval_string("""
      require VialKeeper.Probe

      VialKeeper.Probe.measure :not_a_probe do
        :ok
      end
      """)
    end
  end

  test "every probe area is either standard or detail" do
    tiers =
      for {area, probes} <- Probe.areas(), probe <- probes, into: %{} do
        summary = Probe.summarize(%{probe => %Probe.Stats{count: 1, total_ns: 1, buckets: [1]}})
        {area, summary |> Map.fetch!(Atom.to_string(probe)) |> Map.fetch!("tier")}
      end

    assert %{codec: "detail", connection: "detail", storage: "standard", http: "standard"} =
             Map.take(tiers, [:codec, :connection, :storage, :http])
  end
end
