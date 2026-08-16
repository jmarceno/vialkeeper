defmodule VialKeeper.Quality.ReachSmells.Files do
  @moduledoc "Loads source files from a Reach project for smell checks."

  alias Reach.Smell.Source
  alias VialKeeper.Quality.ReachSmells.SourcePaths

  @spec scan(Reach.Project.t(), (Path.t() -> boolean()), (Macro.t(), Path.t() -> [term()])) :: [
          term()
        ]
  def scan(project, predicate, scanner) do
    project
    |> Source.module_files()
    |> Enum.filter(predicate)
    |> Enum.flat_map(fn file ->
      file
      |> File.read!()
      |> Sourceror.parse_string!()
      |> scanner.(file)
    end)
  end

  @spec lib?(Path.t()) :: boolean()
  def lib?(file), do: SourcePaths.lib?(file)

  @spec production_or_bench?(Path.t()) :: boolean()
  def production_or_bench?(file), do: SourcePaths.production_or_bench?(file)

  @spec not_quality?(Path.t()) :: boolean()
  def not_quality?(file), do: not SourcePaths.quality?(file)
end
