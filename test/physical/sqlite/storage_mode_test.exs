defmodule ElixirDB.StorageAdapter.MemoryModeTest do
  @moduledoc "Covers isolated in-memory SQLite adapter behavior and validation."

  use ExUnit.Case, async: true

  @moduletag :sqlite_physical

  alias ElixirDB.Storage.SQLite.{Adapter, Connection}

  test "creates an isolated in-memory database with the V1 schema" do
    assert {:ok, first} = Adapter.create(":memory:", %{storage_mode: :memory})
    assert {:ok, second} = Adapter.create(":memory:", %{storage_mode: :memory})

    on_exit(fn ->
      Adapter.close(first)
      Adapter.close(second)
    end)

    assert first.storage_mode == :memory
    assert {:ok, [["memory"]]} = Connection.pragma(first.conn, "journal_mode")
    assert {:ok, [[1]]} = Connection.pragma(first.conn, "synchronous")

    assert {:ok, %{revision: revision}} =
             Adapter.apply_local_mutation(first, %{
               operation: :put,
               document_id: "memory-only",
               body: %{"value" => 1}
             })

    assert {:ok, %{revision: ^revision}} =
             Adapter.get_document(first, %{document_id: "memory-only"})

    assert {:error, %ElixirDB.Error{code: :document_not_found}} =
             Adapter.get_document(second, %{document_id: "memory-only"})
  end

  test "rejects invalid SQLite storage mode/path combinations" do
    path = ElixirDB.TempDatabase.path(prefix: "elixirdb-invalid-storage-mode")

    assert {:error, %ElixirDB.Error{code: :invalid_request}} =
             Adapter.create(path, %{storage_mode: :memory})

    assert {:error, %ElixirDB.Error{code: :invalid_request}} =
             Adapter.create(path, %{storage_mode: :shared_memory})

    ElixirDB.TempDatabase.cleanup(path)
  end

  test "disk databases use WAL and drop sidecars on close" do
    {:ok, bundle} = ElixirDB.TempDatabase.create(prefix: "elixirdb-disk-wal")
    sqlite = ElixirDB.TempDatabase.sqlite_path(bundle)

    assert {:ok, adapter} = Adapter.create(sqlite, %{storage_mode: :disk})

    on_exit(fn ->
      _ = Adapter.close(adapter)
      ElixirDB.TempDatabase.cleanup(bundle)
    end)

    assert adapter.storage_mode == :disk
    assert {:ok, [["wal"]]} = Connection.pragma(adapter.conn, "journal_mode")
    assert {:ok, [[2]]} = Connection.pragma(adapter.conn, "synchronous")

    assert {:ok, _} =
             Adapter.apply_local_mutation(adapter, %{
               operation: :put,
               document_id: "wal-doc",
               body: %{"value" => 1}
             })

    assert :ok = Adapter.close(adapter)
    refute File.exists?(sqlite <> "-wal")
    refute File.exists?(sqlite <> "-shm")
    refute File.exists?(sqlite <> "-journal")
  end
end
