defmodule VialKeeper.Quality.ReachSmells.Check do
  @moduledoc """
  AST-level test seam shared by VialKeeper custom Reach smells.

  Production checks still run through `Reach.Smell.Check`. Tests call `findings/2`
  with a parsed snippet instead of a full Reach project.
  """

  @callback findings(Macro.t(), Path.t()) :: [term()]
end
