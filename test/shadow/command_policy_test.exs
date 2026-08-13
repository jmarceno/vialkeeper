defmodule ElixirDB.Shadow.CommandPolicyTest do
  use ExUnit.Case, async: true

  alias ElixirDB.Commands
  alias ElixirDB.Error
  alias ElixirDB.Runtime.{CommandContext, DatabaseCommandPolicy}

  test "public routing has no authority on a shadow database" do
    assert {:error, %Error{code: :shadow_command_forbidden}} =
             DatabaseCommandPolicy.authorize(
               :shadow,
               CommandContext.public(),
               %Commands.GetDocument{request: %{document_id: "doc"}}
             )
  end

  test "shadow read context allows only the narrow read set" do
    context = CommandContext.shadow_read()

    assert :ok =
             DatabaseCommandPolicy.authorize(
               :shadow,
               context,
               %Commands.GetRevision{request: %{document_id: "doc", revision_id: "1-rev"}}
             )

    assert {:error, %Error{code: :shadow_command_forbidden}} =
             DatabaseCommandPolicy.authorize(
               :shadow,
               context,
               %Commands.PutDocument{request: %{document_id: "doc"}}
             )
  end

  test "replication context allows revision transfer and checkpoint commands" do
    context = CommandContext.shadow_replication()

    assert :ok =
             DatabaseCommandPolicy.authorize(
               :shadow,
               context,
               %Commands.ImportRevisionChains{request: %{chains: []}}
             )

    assert :ok =
             DatabaseCommandPolicy.authorize(
               :shadow,
               context,
               %Commands.PutCheckpoint{request: %{namespace: "checkpoints", key: "source"}}
             )

    assert {:error, %Error{code: :shadow_command_forbidden}} =
             DatabaseCommandPolicy.authorize(
               :shadow,
               context,
               %Commands.DeleteDocument{request: %{document_id: "doc"}}
             )
  end

  test "unknown command structs are denied for every shadow context" do
    unknown = %URI{}

    for context <- [
          CommandContext.shadow_read(),
          CommandContext.shadow_replication(),
          CommandContext.shadow_control()
        ] do
      assert {:error, %Error{code: :shadow_command_forbidden}} =
               DatabaseCommandPolicy.authorize(:shadow, context, unknown)
    end
  end

  test "every command envelope has an explicit shadow policy decision" do
    policy = DatabaseCommandPolicy.shadow_policy()

    assert MapSet.new(Map.keys(policy)) == MapSet.new(Commands.command_types())
    assert Enum.all?(policy, fn {_command, classes} -> is_list(classes) end)
    assert policy[Commands.PutDocument] == []
    assert :shadow_replication in policy[Commands.ImportRevisionChains]
  end
end
