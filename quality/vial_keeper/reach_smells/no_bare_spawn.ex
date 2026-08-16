defmodule VialKeeper.Quality.ReachSmells.NoBareSpawn do
  @moduledoc """
  Forbids unsupervised `spawn`, `spawn_link`, `Task.start`, and `Task.start_link` in `lib/`.

  `VialKeeper.Runtime.AttachmentCoordinator` may start a linked GC task because
  that process is owned and monitored by the coordinator.
  """

  @behaviour Reach.Smell.Check
  @behaviour VialKeeper.Quality.ReachSmells.Check

  alias VialKeeper.Quality.ReachSmells.{AST, Files}

  @kind :vial_keeper_bare_spawn

  @allowed [
    [:VialKeeper, :Runtime, :AttachmentCoordinator]
  ]

  @impl true
  def kinds, do: [@kind]

  @impl true
  def run(project), do: Files.scan(project, &Files.lib?/1, &findings/2)

  @impl true
  @spec findings(Macro.t(), Path.t()) :: [Reach.Smell.Finding.t()]
  def findings(ast, file) do
    AST.walk_modules(ast, fn module, body ->
      if module in @allowed do
        []
      else
        spawn_findings(body, file)
      end
    end)
  end

  defp spawn_findings(body, file) do
    AST.each_call(body, fn module_ast, name, _args, meta ->
      cond do
        kernel?(module_ast) and name in [:spawn, :spawn_link] ->
          [
            AST.finding(
              @kind,
              "use a supervised process instead of #{name}/n",
              file,
              meta
            )
          ]

        task?(module_ast) and name in [:start, :start_link] ->
          [
            AST.finding(
              @kind,
              "use Task.Supervisor instead of Task.#{name}/n",
              file,
              meta
            )
          ]

        true ->
          []
      end
    end)
  end

  defp kernel?(:kernel), do: true

  defp kernel?(module_ast) do
    match?({:__aliases__, _, [:Kernel]}, AST.unwrap(module_ast))
  end

  defp task?(module_ast) do
    match?({:__aliases__, _, [:Task]}, AST.unwrap(module_ast))
  end
end
