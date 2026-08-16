defmodule VialKeeper.Quality.ReachSmells.JustifiedSuppressionsTest do
  use ExUnit.Case, async: true

  alias VialKeeper.Quality.ReachSmells.JustifiedSuppressions

  test "requires a quality reason beside suppressions" do
    tmp = Path.join(System.tmp_dir!(), "vk-quality-#{System.unique_integer([:positive])}.ex")

    File.write!(tmp, """
    defmodule Sample do
      # credo:disable-for-next-line Credo.Check.Refactor.LongQuoteBlocks
      def run, do: :ok
    end
    """)

    justified =
      Path.join(System.tmp_dir!(), "vk-quality-ok-#{System.unique_integer([:positive])}.ex")

    File.write!(justified, """
    defmodule Sample do
      # quality:reason quote injection is required for adapter contract tests
      # credo:disable-for-next-line Credo.Check.Refactor.LongQuoteBlocks
      def run, do: :ok
    end
    """)

    try do
      assert [%{kind: :vial_keeper_unjustified_suppression}] = JustifiedSuppressions.scan_file(tmp)
      assert JustifiedSuppressions.scan_file(justified) == []
    after
      File.rm(tmp)
      File.rm(justified)
    end
  end
end
