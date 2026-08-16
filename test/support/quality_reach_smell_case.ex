defmodule VialKeeper.Quality.ReachSmellCase do
  @moduledoc "Parses a source snippet for custom Reach smell tests."

  def parse!(source) when is_binary(source) do
    Sourceror.parse_string!(source)
  end
end
