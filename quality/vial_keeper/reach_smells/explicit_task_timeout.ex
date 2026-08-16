defmodule VialKeeper.Quality.ReachSmells.ExplicitTaskTimeout do
  @moduledoc """
  Requires explicit timeouts on `Task.await/1`, `Task.yield/1`, and `Task.shutdown/1` in `lib/`.
  """

  @behaviour Reach.Smell.Check
  @behaviour VialKeeper.Quality.ReachSmells.Check

  alias VialKeeper.Quality.ReachSmells.{AST, Files}

  @kind :vial_keeper_implicit_task_timeout
  @names MapSet.new([:await, :yield, :shutdown])

  @impl true
  def kinds, do: [@kind]

  @impl true
  def run(project), do: Files.scan(project, &Files.lib?/1, &findings/2)

  @impl true
  @spec findings(Macro.t(), Path.t()) :: [Reach.Smell.Finding.t()]
  def findings(ast, file) do
    AST.each_call(ast, fn
      module_ast, name, args, meta ->
        if task?(module_ast) and name in @names and match?([_task], args) do
          [
            AST.finding(
              @kind,
              "Task.#{name}/1 uses OTP's default timeout; pass an explicit timeout",
              file,
              meta
            )
          ]
        else
          []
        end
    end)
  end

  defp task?(module_ast) do
    match?({:__aliases__, _, [:Task]}, AST.unwrap(module_ast))
  end
end
