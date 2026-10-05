defmodule Mix.Tasks.Test.ContainerReplication do
  @moduledoc """
  Runs the optional three-container replication drill.

  This task is the only supported entry point. The drill is excluded from
  `mix test`, `mix check.fast`, `mix check.integration`, and `mix check.full`.

      mix test.container_replication
      mix test.container_replication --burst 48
  """

  use Mix.Task

  @shortdoc "Runs the optional three-container replication drill"

  @switches [burst: :integer]

  @impl Mix.Task
  @spec run([binary()]) :: :ok
  def run(args) do
    {options, positional, invalid} = OptionParser.parse(args, strict: @switches)

    if positional != [] or invalid != [] do
      Mix.raise("invalid arguments: #{inspect(positional ++ invalid)}\n\n#{usage()}")
    end

    arm_burst!(options[:burst])
    System.put_env("VIAL_KEEPER_CONTAINER_REPLICATION", "1")

    Mix.Task.rerun("test", [
      "--warnings-as-errors",
      "--only",
      "container_replication",
      "test/manual/container_replication_test.exs"
    ])

    :ok
  end

  defp arm_burst!(nil), do: :ok

  defp arm_burst!(count) when is_integer(count) and count > 0 do
    System.put_env("VIAL_KEEPER_CONTAINER_REPLICATION_BURST", Integer.to_string(count))
    :ok
  end

  defp arm_burst!(count),
    do: Mix.raise("`--burst` must be a positive integer, got #{inspect(count)}")

  defp usage do
    "mix test.container_replication [--burst N]"
  end
end
