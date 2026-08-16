defmodule VialKeeper.Quality.ReachSmells.BoundedAsyncStreamTest do
  use ExUnit.Case, async: true

  alias VialKeeper.Quality.ReachSmellCase
  alias VialKeeper.Quality.ReachSmells.BoundedAsyncStream

  test "requires max_concurrency and a finite timeout" do
    bad =
      ReachSmellCase.parse!("""
      defmodule Sample do
        def run(items), do: Task.async_stream(items, & &1)
      end
      """)

    infinite =
      ReachSmellCase.parse!("""
      defmodule Sample do
        def run(items) do
          Task.async_stream(items, & &1, max_concurrency: 4, timeout: :infinity)
        end
      end
      """)

    good =
      ReachSmellCase.parse!("""
      defmodule Sample do
        def run(items) do
          Task.async_stream(items, & &1, max_concurrency: 4, timeout: 1_000)
        end
      end
      """)

    assert [%{kind: :vial_keeper_unbounded_async_stream}] =
             BoundedAsyncStream.findings(bad, "lib/sample.ex")

    assert [%{kind: :vial_keeper_unbounded_async_stream}] =
             BoundedAsyncStream.findings(infinite, "lib/sample.ex")

    assert BoundedAsyncStream.findings(good, "lib/sample.ex") == []
  end
end
