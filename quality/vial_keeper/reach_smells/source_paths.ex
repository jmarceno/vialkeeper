defmodule VialKeeper.Quality.ReachSmells.SourcePaths do
  @moduledoc "Classifies source files for VialKeeper-specific Reach smells."

  @spec lib?(Path.t()) :: boolean()
  def lib?(file), do: under?(file, ["lib"])

  @spec bench_support?(Path.t()) :: boolean()
  def bench_support?(file), do: under?(file, ["bench", "support"])

  @spec quality?(Path.t()) :: boolean()
  def quality?(file), do: under?(file, ["quality"])

  @spec production_or_bench?(Path.t()) :: boolean()
  def production_or_bench?(file), do: lib?(file) or bench_support?(file)

  defp under?(file, segments) when is_binary(file) do
    file
    |> Path.expand()
    |> Path.relative_to(File.cwd!())
    |> Path.split()
    |> List.starts_with?(segments)
  end

  defp under?(_file, _segments), do: false
end
