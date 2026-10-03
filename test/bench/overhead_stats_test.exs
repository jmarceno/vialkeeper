Code.require_file("../../bench/overhead/stats.exs", __DIR__)
Code.require_file("../../bench/overhead/sampler.exs", __DIR__)

defmodule VialKeeper.Bench.OverheadStatsTest do
  use ExUnit.Case, async: true

  alias VialKeeper.Benchmarks.Overhead.{Sampler, Stats}

  test "median interpolates the two middle values of an even sample" do
    assert Stats.median([4, 1, 3, 2]) == 2.5
    assert Stats.median([5, 1, 3]) == 3.0
    assert Stats.percentile([10, 20, 30, 40, 50], 0.9) == 46.0
  end

  test "paired deltas and ratios keep collection order" do
    reference = [100, 300, 200]
    candidate = [150, 330, 260]

    assert Stats.paired_deltas(reference, candidate) == [50, 30, 60]
    assert Stats.paired_ratios(reference, candidate) == [1.5, 1.1, 1.3]
  end

  test "paired comparison differs from a comparison of sorted samples" do
    # Candidate is always reference + 10, but the samples drift in opposite
    # directions, so sorting both series would pair unrelated samples.
    reference = [100, 500, 300, 200, 400]
    candidate = Enum.map(reference, &(&1 + 10))

    comparison = Stats.compare(reference, candidate)
    assert comparison["paired_delta_median_ns"] == 10.0
    assert comparison["paired_delta_ci95_ns"] == [10.0, 10.0]
  end

  test "bootstrap median interval is deterministic and brackets the median" do
    samples = Enum.map(1..200, &(1_000 + rem(&1 * 37, 101)))
    {low, high} = Stats.median_ci(samples)

    assert {low, high} == Stats.median_ci(samples)
    assert low <= Stats.median(samples)
    assert high >= Stats.median(samples)
  end

  test "relative half width is measured against the centre" do
    assert Stats.relative_half_width_pct({2.37, 2.43}, 2.40) |> Float.round(4) == 1.25
    assert Stats.relative_half_width_pct(nil, 1.0) == nil
  end

  test "summary omits p99 below one hundred samples" do
    summary = Stats.summarize(Enum.to_list(1..30), 10)
    assert summary["p99_ns"] == nil
    assert summary["median_ns_per_operation"] == 1.55
  end

  test "variant order rotates through every position" do
    variants = [:a, :b, :c]
    orders = Enum.map(0..5, &Sampler.order(variants, &1))

    assert Enum.map(orders, &List.first/1) |> Enum.sort() == [:a, :a, :b, :b, :c, :c]
    assert Enum.at(orders, 3) == Enum.reverse(Enum.at(orders, 0))
  end

  test "sampler stops on convergence and keeps inputs outside the timed call" do
    prepared = :counters.new(1, [])

    result =
      Sampler.run(
        variants: [:fast, :slow],
        reference: :fast,
        prepare: fn token ->
          :counters.add(prepared, 1, 1)
          token
        end,
        invoke: fn
          :fast, _input -> {:timed, 1_000, %{}}
          :slow, _input -> {:timed, 2_000, %{}}
        end,
        warmup: 2,
        min_iterations: 10,
        max_iterations: 100,
        target_ci_pct: 1.0,
        budget_ms: 60_000
      )

    assert result.stop_reason == "converged"
    assert Enum.count(result.samples) == 10
    assert :counters.get(prepared, 1) == 12
    assert Enum.all?(result.samples, &(&1.slow.ns == 2_000))
  end
end
