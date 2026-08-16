defmodule VialKeeper.Quality.ReachSmells.ExplicitGenServerTimeout do
  @moduledoc """
  Requires an explicit timeout on `GenServer.call/2` in `lib/` and `bench/support/`.

  OTP's 5-second default is not an approved control-plane budget. Callers must
  pass a timeout or use a wrapper that does.
  """

  @behaviour Reach.Smell.Check
  @behaviour VialKeeper.Quality.ReachSmells.Check

  alias VialKeeper.Quality.ReachSmells.{AST, Files}

  @kind :vial_keeper_implicit_genserver_timeout

  @impl true
  def kinds, do: [@kind]

  @impl true
  def run(project), do: Files.scan(project, &Files.production_or_bench?/1, &findings/2)

  @impl true
  @spec findings(Macro.t(), Path.t()) :: [Reach.Smell.Finding.t()]
  def findings(ast, file) do
    AST.each_call(ast, fn
      module_ast, :call, args, meta ->
        if genserver?(module_ast) and match?([_server, _request], args) do
          [
            AST.finding(
              @kind,
              "GenServer.call/2 uses OTP's default timeout; pass an explicit timeout",
              file,
              meta
            )
          ]
        else
          []
        end

      _module, _name, _args, _meta ->
        []
    end)
  end

  defp genserver?(module_ast) do
    match?({:__aliases__, _, [:GenServer]}, AST.unwrap(module_ast))
  end
end
