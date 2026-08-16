defmodule VialKeeper.PathSafetyPropertiesTest do
  @moduledoc "Generated paths cannot escape an approved filesystem root."

  use ExUnit.Case, async: true
  use ExUnitProperties

  alias VialKeeper.PathSafety

  @root "/approved/vialkeeper-root"

  property "relative segments stay inside the approved root" do
    check all(
            segments <- StreamData.list_of(path_segment(), min_length: 1, max_length: 6),
            max_runs: 40
          ) do
      full = Path.expand(Path.join(segments), @root)
      assert PathSafety.within_root?(full, @root)
    end
  end

  property "parent traversal and foreign absolute paths stay outside the root" do
    check all(
            extra <- StreamData.list_of(path_segment(), max_length: 3),
            max_runs: 40
          ) do
      escaped = Path.expand(Path.join([@root, "..", "outside" | extra]))
      refute PathSafety.within_root?(escaped, @root)
      refute PathSafety.within_root?("/etc/passwd", @root)
    end
  end

  defp path_segment do
    StreamData.string([?a..?z, ?0..?9], min_length: 1, max_length: 12)
  end
end
