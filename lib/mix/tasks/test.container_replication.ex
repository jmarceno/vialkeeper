defmodule Mix.Tasks.Test.ContainerReplication do
  @moduledoc """
  Runs the optional three-container replication drill.

  This task is the only supported entry point. The drill is excluded from
  `mix test`, `mix check.fast`, `mix check.integration`, and `mix check.full`.

      mix test.container_replication
      mix test.container_replication --burst 48
      mix test.container_replication --duration 3600 --burst 48
  """

  use Mix.Task

  @shortdoc "Runs the optional three-container replication drill"

  @switches [burst: :integer, duration: :integer]
  @armed_env "VIAL_KEEPER_CONTAINER_REPLICATION"
  @burst_env "VIAL_KEEPER_CONTAINER_REPLICATION_BURST"
  @duration_env "VIAL_KEEPER_CONTAINER_REPLICATION_DURATION"

  @impl Mix.Task
  @spec run([binary()]) :: :ok
  def run(args) do
    options = parse_options!(args)
    previous = Enum.map([@armed_env, @burst_env, @duration_env], &{&1, System.get_env(&1)})

    try do
      arm_burst!(options[:burst])
      System.put_env(@duration_env, Integer.to_string(options[:duration]))
      System.put_env(@armed_env, "1")

      Mix.Task.rerun("test", [
        "--warnings-as-errors",
        "--only",
        "container_replication",
        "test/manual/container_replication_test.exs"
      ])

      :ok
    after
      Enum.each(previous, fn
        {key, nil} -> System.delete_env(key)
        {key, value} -> System.put_env(key, value)
      end)
    end
  end

  @doc "Validates drill arguments; duration is the minimum workload time in seconds."
  @spec parse_options!([binary()]) :: [{:burst, pos_integer()} | {:duration, non_neg_integer()}]
  def parse_options!(args) do
    {options, positional, invalid} = OptionParser.parse(args, strict: @switches)

    if positional != [] or invalid != [] do
      Mix.raise("invalid arguments: #{inspect(positional ++ invalid)}\n\n#{usage()}")
    end

    Enum.each(options, fn
      {_key, count} when is_integer(count) and count > 0 -> :ok
      {key, count} -> Mix.raise("`--#{key}` must be a positive integer, got #{inspect(count)}")
    end)

    Keyword.put_new(options, :duration, 0)
  end

  defp arm_burst!(nil), do: :ok

  defp arm_burst!(count) when is_integer(count) and count > 0 do
    System.put_env(@burst_env, Integer.to_string(count))
    :ok
  end

  defp usage do
    "mix test.container_replication [--burst N] [--duration SECONDS]"
  end
end
