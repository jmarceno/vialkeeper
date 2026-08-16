defmodule VialKeeper.Quality.ReachSmells.TantivyBoundary do
  @moduledoc """
  Confines `TantivyEx` to `VialKeeper.Search.Tantivy`.

  `VialKeeper.Bench.PerformanceDiagnostics` may call TantivyEx because it is the
  raw native control on the same-disk diagnostic ladder, not a product search
  implementation.
  """

  @behaviour Reach.Smell.Check
  @behaviour VialKeeper.Quality.ReachSmells.Check

  alias VialKeeper.Quality.ReachSmells.{AST, Files}

  @kind :vial_keeper_tantivy_boundary

  @allowed [
    [:VialKeeper, :Search, :Tantivy],
    [:VialKeeper, :Bench, :PerformanceDiagnostics]
  ]

  @impl true
  def kinds, do: [@kind]

  @impl true
  def run(project), do: Files.scan(project, &Files.not_quality?/1, &findings/2)

  @impl true
  @spec findings(Macro.t(), Path.t()) :: [Reach.Smell.Finding.t()]
  def findings(ast, file) do
    AST.walk_modules(ast, fn module, body ->
      if module in @allowed do
        []
      else
        tantivy_findings(body, file)
      end
    end)
  end

  defp tantivy_findings(body, file) do
    aliases = AST.aliases(body)

    AST.each_call(body, fn module_ast, _name, _args, meta ->
      case AST.expand_alias(module_ast, aliases) do
        [:TantivyEx | _rest] ->
          [
            AST.finding(
              @kind,
              "TantivyEx may be used only from VialKeeper.Search.Tantivy or VialKeeper.Bench.PerformanceDiagnostics",
              file,
              meta
            )
          ]

        _other ->
          []
      end
    end)
  end
end
