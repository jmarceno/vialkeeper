defmodule ElixirDB.StorageAdapter.PortabilityTest do
  use ElixirDB.Storage.AdapterCase, adapter: ElixirDB.Storage.SQLite.Adapter

  @moduletag :sqlite_physical

  test "closed-file OS copy without lease reopens with integrity", %{
    adapter: adapter,
    path: path
  } do
    assert {:ok, %{revision: revision}} =
             @adapter.apply_local_mutation(adapter, %{
               operation: :put,
               document_id: "portable",
               body: %{"copied" => true, "n" => 7}
             })

    assert {:ok, identity} = @adapter.identity(adapter)
    assert :ok = @adapter.close(adapter)

    refute File.exists?(path <> ".lease")
    refute File.exists?(path <> "-journal")
    refute File.exists?(path <> "-wal")
    refute File.exists?(path <> "-shm")

    {:ok, copy_bundle} = ElixirDB.TempDatabase.create(prefix: "elixirdb-portable-copy")
    copy_sqlite = ElixirDB.TempDatabase.sqlite_path(copy_bundle)
    File.cp!(path, copy_sqlite)
    refute File.exists?(copy_sqlite <> ".lease")

    on_exit(fn -> ElixirDB.TempDatabase.cleanup(copy_bundle) end)

    assert {:ok, reopened} = @adapter.open(copy_sqlite)

    assert {:ok, reopened_identity} = @adapter.identity(reopened)
    assert reopened_identity.database_uuid == identity.database_uuid
    assert reopened_identity.current_sequence == identity.current_sequence
    assert reopened_identity.config == identity.config

    assert {:ok, %{revision: ^revision, body: %{"copied" => true, "n" => 7}}} =
             @adapter.get_document(reopened, %{document_id: "portable"})

    assert {:ok, %{ok: true}} = @adapter.integrity_check(reopened, %{})
    assert :ok = @adapter.close(reopened)

    # Original path remains independently openable after copy.
    assert {:ok, original} = @adapter.open(path)

    assert {:ok, original_identity} = @adapter.identity(original)
    assert original_identity.database_uuid == identity.database_uuid
    assert original_identity.current_sequence == identity.current_sequence

    assert {:ok, %{revision: ^revision, body: %{"copied" => true, "n" => 7}}} =
             @adapter.get_document(original, %{document_id: "portable"})

    assert {:ok, %{ok: true}} = @adapter.integrity_check(original, %{})
    assert :ok = @adapter.close(original)
  end
end
