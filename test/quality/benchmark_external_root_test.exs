defmodule VialKeeper.Quality.ReachSmells.BenchmarkExternalRootTest do
  use ExUnit.Case, async: true

  alias VialKeeper.Quality.ReachSmellCase
  alias VialKeeper.Quality.ReachSmells.BenchmarkExternalRoot

  test "flags system temp and /tmp literals, and allows the benchmark root API" do
    bad =
      ReachSmellCase.parse!("""
      defmodule VialKeeper.Bench.Sample do
        def run, do: System.tmp_dir!()
      end
      """)

    literal =
      ReachSmellCase.parse!("""
      defmodule VialKeeper.Bench.Sample do
        def run, do: "/tmp/vialkeeper-bench"
      end
      """)

    good =
      ReachSmellCase.parse!("""
      defmodule VialKeeper.Bench.Sample do
        def run(context), do: VialKeeper.Bench.Root.work_run_path(context, "x", "y")
      end
      """)

    assert [%{kind: :vial_keeper_benchmark_external_root}] =
             BenchmarkExternalRoot.findings(bad, "bench/support/sample.ex")

    assert [%{kind: :vial_keeper_benchmark_external_root}] =
             BenchmarkExternalRoot.findings(literal, "bench/support/sample.ex")

    assert BenchmarkExternalRoot.findings(good, "bench/support/sample.ex") == []
  end
end
