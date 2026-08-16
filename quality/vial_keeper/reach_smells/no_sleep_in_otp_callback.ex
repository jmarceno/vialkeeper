defmodule VialKeeper.Quality.ReachSmells.NoSleepInOtpCallback do
  @moduledoc """
  Forbids `Process.sleep/1` and `:timer.sleep/1` lexically inside GenServer
  `handle_call`, `handle_cast`, `handle_info`, and `handle_continue` callbacks.
  """

  @behaviour Reach.Smell.Check
  @behaviour VialKeeper.Quality.ReachSmells.Check

  alias VialKeeper.Quality.ReachSmells.{AST, Files}

  @kind :vial_keeper_sleep_in_otp_callback
  @callbacks [:handle_call, :handle_cast, :handle_info, :handle_continue]

  @impl true
  def kinds, do: [@kind]

  @impl true
  def run(project), do: Files.scan(project, &Files.lib?/1, &findings/2)

  @impl true
  @spec findings(Macro.t(), Path.t()) :: [Reach.Smell.Finding.t()]
  def findings(ast, file) do
    {_ast, findings} =
      Macro.prewalk(ast, [], fn
        {:def, meta, [{:when, _, [{name, _, args}, _guard]}, body]} = node, acc
        when name in @callbacks ->
          {node, sleep_findings(file, {name, meta, args}, body) ++ acc}

        {:defp, meta, [{:when, _, [{name, _, args}, _guard]}, body]} = node, acc
        when name in @callbacks ->
          {node, sleep_findings(file, {name, meta, args}, body) ++ acc}

        {:def, meta, [{name, _, args}, body]} = node, acc when name in @callbacks ->
          {node, sleep_findings(file, {name, meta, args}, body) ++ acc}

        {:defp, meta, [{name, _, args}, body]} = node, acc when name in @callbacks ->
          {node, sleep_findings(file, {name, meta, args}, body) ++ acc}

        node, acc ->
          {node, acc}
      end)

    findings
  end

  defp sleep_findings(file, _head, body) do
    AST.each_call(body, fn
      module_ast, :sleep, _args, meta ->
        if sleep_module?(module_ast) do
          [
            AST.finding(
              @kind,
              "do not sleep inside OTP callbacks; use a timer message or supervised worker",
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

  defp sleep_module?(module_ast) do
    case AST.unwrap(module_ast) do
      {:__aliases__, _, [:Process]} -> true
      :timer -> true
      {:__aliases__, _, [:timer]} -> true
      _other -> false
    end
  end
end
