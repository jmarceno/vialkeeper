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
end
