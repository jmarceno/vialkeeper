defmodule VialKeeper.Quality.ReachSmells.BenchmarkExternalRoot do
  @moduledoc """
  Forbids `System.tmp_dir/0`, `System.tmp_dir!/0`, and `/tmp` path literals
  under `VialKeeper.Bench.*`.

  Large datasets and work databases must use the benchmark root abstraction.
  """

  @behaviour Reach.Smell.Check
  @behaviour VialKeeper.Quality.ReachSmells.Check

  alias VialKeeper.Quality.ReachSmells.{AST, Files, SourcePaths}

  @kind :vial_keeper_benchmark_external_root

  @impl true
  def kinds, do: [@kind]

  @impl true
  def run(project), do: Files.scan(project, &SourcePaths.bench_support?/1, &findings/2)

  @impl true
  @spec findings(Macro.t(), Path.t()) :: [Reach.Smell.Finding.t()]
  def findings(ast, file) do
    tmp_dir_findings(ast, file) ++ tmp_literal_findings(ast, file)
  end

  defp tmp_dir_findings(ast, file) do
    AST.each_call(ast, fn module_ast, name, _args, meta ->
      if system?(module_ast) and name in [:tmp_dir, :tmp_dir!] do
        [
          AST.finding(
            @kind,
            "benchmark code must use the approved external root, not System.#{name}",
            file,
            meta
          )
        ]
      else
        []
      end
    end)
  end

  defp tmp_literal_findings(ast, file) do
    {_ast, findings} =
      Macro.prewalk(ast, [], fn
        "/tmp" = node, acc ->
          {node,
           [AST.finding(@kind, "literal /tmp path is forbidden in benchmark code", file, []) | acc]}

        <<"/tmp/", _rest::binary>> = node, acc ->
          {node,
           [AST.finding(@kind, "literal /tmp path is forbidden in benchmark code", file, []) | acc]}

        node, acc ->
          {node, acc}
      end)

    findings
  end

  defp system?(module_ast) do
    match?({:__aliases__, _, [:System]}, AST.unwrap(module_ast))
  end
end
