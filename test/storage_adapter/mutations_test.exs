defmodule ElixirDB.StorageAdapter.MutationsTest do
  use ElixirDB.Storage.AdapterCase, adapter: ElixirDB.Storage.SQLite.Adapter

  alias ElixirDB.Storage.SQLite.Connection

  test "bulk mutations are all-or-nothing", %{adapter: adapter} do
    assert {:ok, %{revision: first}} =
             @adapter.apply_local_mutation(adapter, %{
               operation: :put,
               document_id: "doc",
               body: %{"value" => 1}
             })

    assert {:error, %ElixirDB.Error{code: :revision_conflict}} =
             @adapter.apply_bulk_mutation(adapter, %{
               operations: [
                 %{
                   operation: :put,
                   document_id: "doc",
                   if_revision: first,
                   body: %{"value" => 2}
                 },
                 %{
                   operation: :put,
                   document_id: "other",
                   if_revision: "1-missing",
                   body: %{"value" => 9}
                 }
               ]
             })

    assert {:ok, %{body: %{"value" => 1}, revision: ^first}} =
             @adapter.get_document(adapter, %{document_id: "doc"})

    assert {:error, %ElixirDB.Error{code: :document_not_found}} =
             @adapter.get_document(adapter, %{document_id: "other"})

    assert {:ok, %{current_sequence: 1}} = @adapter.identity(adapter)

    assert {:ok, results} =
             @adapter.apply_bulk_mutation(adapter, %{
               operations: [
                 %{
                   operation: :put,
                   document_id: "doc",
                   if_revision: first,
                   body: %{"value" => 2}
                 },
                 %{operation: :put, document_id: "other", body: %{"value" => 9}}
               ]
             })

    assert length(results) == 2
    assert Enum.all?(results, &(&1.replayed == false))

    assert {:ok, %{body: %{"value" => 2}}} =
             @adapter.get_document(adapter, %{document_id: "doc"})

    assert {:ok, %{body: %{"value" => 9}}} =
             @adapter.get_document(adapter, %{document_id: "other"})
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

    assert {:error, %ElixirDB.Error{code: :integrity_violation}} =
             @adapter.integrity_check(adapter, %{})
  end

  test "put replay returns the same revision without advancing sequence", %{adapter: adapter} do
    assert {:ok, %{revision: revision, sequence: sequence, replayed: false}} =
             @adapter.apply_local_mutation(adapter, %{
               operation: :put,
               document_id: "doc",
               body: %{"value" => 1}
             })

    assert {:ok, %{revision: ^revision, sequence: ^sequence, replayed: true}} =
             @adapter.apply_local_mutation(adapter, %{
               operation: :put,
               document_id: "doc",
               body: %{"value" => 1}
             })

    assert {:ok, %{revision: next, replayed: false}} =
             @adapter.apply_local_mutation(adapter, %{
               operation: :put,
               document_id: "doc",
               if_revision: revision,
               body: %{"value" => 2}
             })

    assert next != revision

    assert {:ok, %{revision: ^next, replayed: true}} =
             @adapter.apply_local_mutation(adapter, %{
               operation: :put,
               document_id: "doc",
               if_revision: revision,
               body: %{"value" => 2}
             })

    assert {:ok, %{current_sequence: 2}} = @adapter.identity(adapter)
  end
end
