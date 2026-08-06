defmodule ElixirDB.StorageAdapter.LifecycleTest do
  use ElixirDB.Storage.AdapterCase, adapter: ElixirDB.Storage.SQLite.Adapter

  test "create, close, reopen preserves identity and documents", %{adapter: adapter, path: path} do
    assert {:ok, %{revision: revision}} =
             @adapter.apply_local_mutation(adapter, %{
               operation: :put,
               document_id: "lifecycle",
               body: %{"phase" => "create"}
             })

    assert {:ok, identity} = @adapter.identity(adapter)
    assert identity.current_sequence == 1
    assert :ok = @adapter.close(adapter)

    assert {:ok, reopened} = @adapter.open(path)

    assert {:ok, %{revision: ^revision, body: %{"phase" => "create"}}} =
             @adapter.get_document(reopened, %{document_id: "lifecycle"})

    assert {:ok, reopened_identity} = @adapter.identity(reopened)
    assert reopened_identity.database_uuid == identity.database_uuid
    assert reopened_identity.current_sequence == identity.current_sequence
    assert :ok = @adapter.close(reopened)
  end

  test "reopen after empty create still validates schema", %{adapter: adapter, path: path} do
    assert {:ok, identity} = @adapter.identity(adapter)
    assert :ok = @adapter.close(adapter)
    assert {:ok, reopened} = @adapter.open(path)
    assert {:ok, %{ok: true}} = @adapter.integrity_check(reopened, %{})
    assert {:ok, %{database_uuid: uuid}} = @adapter.identity(reopened)
    assert uuid == identity.database_uuid
    assert :ok = @adapter.close(reopened)
  end
end
