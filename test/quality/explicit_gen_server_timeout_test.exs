defmodule VialKeeper.Quality.ReachSmells.ExplicitGenServerTimeoutTest do
  use ExUnit.Case, async: true

  alias VialKeeper.Quality.ReachSmellCase
  alias VialKeeper.Quality.ReachSmells.ExplicitGenServerTimeout

  test "flags implicit GenServer.call/2 and allows an explicit timeout" do
    bad =
      ReachSmellCase.parse!("""
      defmodule Sample do
        def run(pid), do: GenServer.call(pid, :ping)
      end
      """)

    piped =
      ReachSmellCase.parse!("""
      defmodule Sample do
        def run(pid), do: pid |> GenServer.call(:ping)
      end
      """)

    good =
      ReachSmellCase.parse!("""
      defmodule Sample do
        def run(pid), do: GenServer.call(pid, :ping, 1_000)
      end
      """)

    assert [%{kind: :vial_keeper_implicit_genserver_timeout}] =
             ExplicitGenServerTimeout.findings(bad, "lib/sample.ex")

    assert [%{kind: :vial_keeper_implicit_genserver_timeout}] =
             ExplicitGenServerTimeout.findings(piped, "lib/sample.ex")

    assert ExplicitGenServerTimeout.findings(good, "lib/sample.ex") == []
  end
end
