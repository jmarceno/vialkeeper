defmodule VialKeeper.Manual.ContainerReplicationOptionsTest do
  @moduledoc "Argument validation for the opt-in replication drill."

  use ExUnit.Case, async: true

  alias Mix.Tasks.Test.ContainerReplication

  test "the default remains a single cycle and duration supports a 90-day soak" do
    assert ContainerReplication.parse_options!([]) == [duration: 0]

    options =
      ContainerReplication.parse_options!(["--duration", "7776000", "--burst", "48"])

    assert options[:duration] == 90 * 24 * 60 * 60
    assert options[:burst] == 48
  end

  test "invalid workload sizes and durations fail before the drill is armed" do
    for flag <- ["--burst", "--duration"], value <- ["0", "-1", "1.5", "invalid"] do
      assert_raise Mix.Error, fn ->
        ContainerReplication.run([flag, value])
      end
    end
  end

  test "unknown, missing, and positional arguments fail closed" do
    for args <- [["--duration"], ["--burst"], ["--unknown", "1"], ["3600"]] do
      assert_raise Mix.Error, fn ->
        ContainerReplication.run(args)
      end
    end
  end
end
