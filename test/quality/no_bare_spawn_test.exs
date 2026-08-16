defmodule VialKeeper.Quality.ReachSmells.NoBareSpawnTest do
  use ExUnit.Case, async: true

  alias VialKeeper.Quality.ReachSmellCase
  alias VialKeeper.Quality.ReachSmells.NoBareSpawn

  test "flags bare spawn and allows the attachment coordinator" do
    bad =
      ReachSmellCase.parse!("""
      defmodule Sample do
        def run, do: spawn(fn -> :ok end)
      end
      """)

    allowed =
      ReachSmellCase.parse!("""
      defmodule VialKeeper.Runtime.AttachmentCoordinator do
        def run, do: Task.start_link(fn -> :ok end)
      end
      """)

    assert [%{kind: :vial_keeper_bare_spawn}] = NoBareSpawn.findings(bad, "lib/sample.ex")
    assert NoBareSpawn.findings(allowed, "lib/sample.ex") == []
  end
end
