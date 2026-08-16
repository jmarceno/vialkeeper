defmodule VialKeeper.Quality.ReachSmells.ExplicitTaskTimeoutTest do
  use ExUnit.Case, async: true

  alias VialKeeper.Quality.ReachSmellCase
  alias VialKeeper.Quality.ReachSmells.ExplicitTaskTimeout

  test "flags implicit Task.await/1 and allows an explicit timeout" do
    bad =
      ReachSmellCase.parse!("""
      defmodule Sample do
        def run(task), do: Task.await(task)
      end
      """)

    good =
      ReachSmellCase.parse!("""
      defmodule Sample do
        def run(task), do: Task.await(task, 1_000)
      end
      """)

    assert [%{kind: :vial_keeper_implicit_task_timeout}] =
             ExplicitTaskTimeout.findings(bad, "lib/sample.ex")

    assert ExplicitTaskTimeout.findings(good, "lib/sample.ex") == []
  end
end
