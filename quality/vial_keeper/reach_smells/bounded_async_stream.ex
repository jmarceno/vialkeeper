defmodule VialKeeper.Quality.ReachSmells.BoundedAsyncStream do
  @moduledoc """
  Requires `Task.async_stream` in `lib/` to set `max_concurrency` and `timeout`.

  `timeout: :infinity` is forbidden in production code unless a local suppression
  records why the operation must run to completion.
  """

  @behaviour Reach.Smell.Check
  @behaviour VialKeeper.Quality.ReachSmells.Check

  alias VialKeeper.Quality.ReachSmells.{AST, Files}

  @kind :vial_keeper_unbounded_async_stream

  @impl true
  def kinds, do: [@kind]

  @impl true
  def run(project), do: Files.scan(project, &Files.lib?/1, &findings/2)

  @impl true
  @spec findings(Macro.t(), Path.t()) :: [Reach.Smell.Finding.t()]
  def findings(ast, file) do
    AST.each_call(ast, fn
      module_ast, :async_stream, args, meta ->
        if task?(module_ast) do
          stream_findings(file, args, meta)
        else
          []
        end

      _module, _name, _args, _meta ->
        []
    end)
  end

  defp stream_findings(file, args, meta) do
    opts = options(args)

    cond do
      is_nil(opts) ->
        [
          AST.finding(
            @kind,
            "Task.async_stream must set max_concurrency and timeout",
            file,
            meta
          )
        ]

      missing_option?(opts, :max_concurrency) or missing_option?(opts, :timeout) ->
        [
          AST.finding(
            @kind,
            "Task.async_stream must set max_concurrency and timeout",
            file,
            meta
          )
        ]

      infinite_timeout?(opts) ->
        [
          AST.finding(
            @kind,
            "Task.async_stream timeout: :infinity is forbidden in lib/ unless suppressed",
            file,
            meta
          )
        ]

      true ->
        []
    end
  end

  defp options([_enumerable, _fun, opts]), do: AST.option_keyword(opts)
  defp options([_enumerable, _mod, _fun, _args, opts]), do: AST.option_keyword(opts)
  defp options(_args), do: nil

  defp missing_option?(opts, key), do: not Keyword.has_key?(opts, key)

  defp infinite_timeout?(opts) do
    AST.unwrap(Keyword.get(opts, :timeout)) == :infinity
  end

  defp task?(module_ast) do
    match?({:__aliases__, _, [:Task]}, AST.unwrap(module_ast))
  end
end
