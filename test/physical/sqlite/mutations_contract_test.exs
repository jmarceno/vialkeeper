defmodule VialKeeper.Storage.SQLite.MutationsContractTest do
  use VialKeeper.Storage.Contracts.Mutations,
    adapter: VialKeeper.Storage.SQLite.Adapter,
    physical: true

  @moduletag :sqlite_physical

  alias VialKeeper.Storage.SQLite.Connection

  test "bulk-created documents remain queryable, indexed, and in changes", %{
    adapter: adapter
  } do
    assert {:ok, %{"index_id" => index_id}} =
             @adapter.create_index(adapter, %{
               "name" => "by-kind",
               "type" => "structured",
               "fields" => [%{"path" => "/kind", "type" => "string", "direction" => "asc"}]
             })

    assert {:ok, results} =
             @adapter.apply_bulk_mutation(adapter, %{
               operations: [
                 %{operation: :put, document_id: "task", body: %{"kind" => "task"}},
                 %{operation: :put, document_id: "note", body: %{"kind" => "note"}}
               ]
             })

    assert Enum.map(results, & &1.sequence) == [2, 1]

    assert {:ok, %{results: [%{id: "task"}], selected_index: ^index_id}} =
             @adapter.execute_query(adapter, %{
               selector: %{"/kind" => "task"},
               index: "by-kind",
               limit: 10
             })

    assert {:ok, %{body: %{"kind" => "task"}}} =
             @adapter.get_document(adapter, %{document_id: "task"})

    assert {:ok, %{results: [first], last_sequence: 1, has_more: true}} =
             @adapter.read_changes(adapter, %{since: 0, limit: 1})

    assert first.document_id == "note"

    assert {:ok, %{results: [second], last_sequence: 2, has_more: false}} =
             @adapter.read_changes(adapter, %{since: 1, limit: 1})

    assert second.document_id == "task"
  end

  test "integrity mismatch is detected after revision corruption", %{adapter: adapter} do
    assert {:ok, %{revision: revision}} =
             @adapter.apply_local_mutation(adapter, %{
               operation: :put,
               document_id: "doc",
               body: %{"ok" => true}
             })

    assert {:ok, %{ok: true}} = @adapter.integrity_check(adapter, %{})

    assert :ok =
             Connection.execute(
               adapter.conn,
               "UPDATE revisions SET body_json = ? WHERE revision_id = ?",
               [~s({"ok":false}), revision]
             )

    assert {:error, %VialKeeper.Error{code: :integrity_violation}} =
             @adapter.integrity_check(adapter, %{})
  end
end
