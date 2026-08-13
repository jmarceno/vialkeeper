defmodule ElixirDB.StorageAdapter.ReadPoolConnectionTest do
  @moduledoc "Covers readonly SQLite snapshot connections. plan §8.1"
  use ExUnit.Case, async: true

  @moduletag :sqlite_physical

  alias ElixirDB.Storage.SQLite.{Adapter, Connection, Context, Lifecycle}

  test "disk reader is readonly and query_only" do
    {:ok, bundle} = ElixirDB.TempDatabase.create(prefix: "elixirdb-read-pool-conn")
    sqlite = ElixirDB.TempDatabase.sqlite_path(bundle)

    assert {:ok, writer} = Adapter.create(sqlite, %{storage_mode: :disk})

    on_exit(fn ->
      _ = Adapter.close(writer)
      ElixirDB.TempDatabase.cleanup(bundle)
    end)

    assert {:ok, _} =
             Adapter.apply_local_mutation(writer, %{
               operation: :put,
               document_id: "doc",
               body: %{"n" => 1}
             })

    assert {:ok, reader_ctx} = Lifecycle.open_reader(Adapter.to_context(writer))
    assert {:ok, reader} = Context.unwrap(reader_ctx)
    assert reader.reader?
    assert {:ok, [[1]]} = Connection.pragma(reader.conn, "query_only")

    assert {:ok, %{body: %{"n" => 1}}} =
             Adapter.get_document(reader, %{document_id: "doc"})

    assert {:error, _reason} =
             Connection.execute(reader.conn, "CREATE TABLE forbidden(id INTEGER)")

    assert :ok = Lifecycle.close_reader(reader_ctx)
    assert :ok = Adapter.close(writer)
    refute File.exists?(sqlite <> "-wal")
    refute File.exists?(sqlite <> "-shm")
  end
end
