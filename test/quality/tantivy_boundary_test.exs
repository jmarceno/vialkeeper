defmodule VialKeeper.Quality.ReachSmells.TantivyBoundaryTest do
  use ExUnit.Case, async: true

  alias VialKeeper.Quality.ReachSmellCase
  alias VialKeeper.Quality.ReachSmells.TantivyBoundary

  test "flags TantivyEx outside the search adapter and diagnostics control" do
    bad =
      ReachSmellCase.parse!("""
      defmodule Sample do
        def run(path, schema), do: TantivyEx.Index.create_in_dir(path, schema)
      end
      """)

    adapter =
      ReachSmellCase.parse!("""
      defmodule VialKeeper.Search.Tantivy do
        def run(path, schema), do: TantivyEx.Index.create_in_dir(path, schema)
      end
      """)

    diagnostics =
      ReachSmellCase.parse!("""
      defmodule VialKeeper.Bench.PerformanceDiagnostics do
        def run(path, schema), do: TantivyEx.Index.create_in_dir(path, schema)
      end
      """)

    assert [
             %{
               kind: :vial_keeper_tantivy_boundary,
               message:
                 "TantivyEx may be used only from VialKeeper.Search.Tantivy or VialKeeper.Bench.PerformanceDiagnostics"
             }
           ] = TantivyBoundary.findings(bad, "lib/sample.ex")

    assert TantivyBoundary.findings(adapter, "lib/sample.ex") == []
    assert TantivyBoundary.findings(diagnostics, "bench/support/sample.ex") == []
  end
end
