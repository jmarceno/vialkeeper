defmodule ElixirDB.Runtime.SubscriptionCommandsTest do
  @moduledoc "Covers database-owner subscription command behavior."

  use ExUnit.Case, async: false

  @moduletag :integration

  alias ElixirDB.Runtime.DatabaseCatalog

  setup do
    rel = "subscription-cmd-#{System.unique_integer([:positive])}.elixirdb"
    root = ElixirDB.Config.database_root()
    abs = Path.join(root, rel)
    ElixirDB.TempDatabase.cleanup(abs)

    assert {:ok, identity} = DatabaseCatalog.create(rel)
    uuid = identity.database_uuid

    on_exit(fn ->
      _ = DatabaseCatalog.close(uuid)
      _ = DatabaseCatalog.unregister(uuid)
      ElixirDB.TempDatabase.cleanup(abs)
    end)

    assert {:ok, _} =
             DatabaseCatalog.command(
               uuid,
               {:command, :put,
                %{
                  document_id: "doc",
                  body: %{"type" => "task"}
                }}
             )

    {:ok, uuid: uuid}
  end

  test "owner dispatches execute_subscription_snapshot", %{uuid: uuid} do
    assert {:ok, %{documents: documents, member_ids: ["doc"], sequence: sequence}} =
             DatabaseCatalog.command(
               uuid,
               {:command, :execute_subscription_snapshot,
                %{
                  selector: %{"/type" => "task"},
                  max_members: 10
                }}
             )

    assert is_integer(sequence)
    assert Enum.count(documents) == 1
  end

  test "owner rejects forbidden subscription snapshot fields", %{uuid: uuid} do
    assert {:error, %ElixirDB.Error{code: :invalid_request, message: message}} =
             DatabaseCatalog.command(
               uuid,
               {:command, :execute_subscription_snapshot,
                %{
                  selector: %{"/type" => "task"},
                  sort: [%{"/priority" => "desc"}]
                }}
             )

    assert message =~ "sort"
  end

  test "owner dispatches get_revisions_batch", %{uuid: uuid} do
    assert {:ok, %{revision: revision}} =
             DatabaseCatalog.command(uuid, {:command, :get_document, %{document_id: "doc"}})

    assert {:ok, [envelope]} =
             DatabaseCatalog.command(
               uuid,
               {:command, :get_revisions_batch,
                %{
                  requests: [%{document_id: "doc", revision_id: revision}]
                }}
             )

    assert envelope.id == "doc"
    assert envelope.revision == revision
  end

  test "owner rejects invalid revision batch requests", %{uuid: uuid} do
    assert {:error, %ElixirDB.Error{code: :invalid_request}} =
             DatabaseCatalog.command(
               uuid,
               {:command, :get_revisions_batch,
                %{
                  requests: [%{document_id: "doc"}]
                }}
             )
  end
end
