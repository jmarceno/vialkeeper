defmodule ElixirDB.Runtime.CommandIOTest do
  @moduledoc """
  Classification of command envelopes into `:read`, `:write`, and `:exclusive`
  IO classes, including the shadow-read allow-list invariant.
  """
  use ExUnit.Case, async: true

  alias ElixirDB.Commands
  alias ElixirDB.Runtime.{CommandIO, DatabaseCommandPolicy}

  test "every command envelope has exactly one IO class" do
    classified = CommandIO.classes()

    assert MapSet.new(Map.keys(classified)) == MapSet.new(Commands.command_types()),
           "CommandIO.classes/0 must cover Commands.command_types/0"

    Enum.each(classified, fn {module, class} ->
      assert class in [:read, :write, :exclusive],
             "#{inspect(module)} must be :read, :write, or :exclusive"

      assert CommandIO.classify(struct(module)) == class
    end)
  end

  test "shadow-read allow-list is a subset of :read" do
    shadow_read_types =
      DatabaseCommandPolicy.shadow_policy()
      |> Enum.filter(fn {_module, classes} -> :shadow_read in classes end)
      |> Enum.map(&elem(&1, 0))

    Enum.each(shadow_read_types, fn module ->
      assert CommandIO.classify(struct(module)) == :read,
             "shadow-read #{inspect(module)} must classify as :read"
    end)
  end

  test "shadow-control exclusive commands stay exclusive" do
    # IntegrityCheck and Close are allowed on shadow_control, not shadow_read.
    # They remain exclusive IO so compact/close still drain readers.
    assert CommandIO.classify(%Commands.IntegrityCheck{}) == :exclusive
    assert CommandIO.classify(%Commands.Close{}) == :exclusive
  end
end
