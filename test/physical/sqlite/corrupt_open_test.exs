defmodule ElixirDB.StorageAdapter.CorruptOpenTest do
  @moduledoc "Covers typed failures when opening corrupt or foreign SQLite artifacts."

  use ExUnit.Case, async: true

  @moduletag :sqlite_physical

  alias ElixirDB.Storage.SQLite.Adapter

  test "open returns a typed error for random 4 KiB input" do
    path = corrupt_path("elixirdb-random-sqlite", :crypto.strong_rand_bytes(4_096))

    assert_typed_error(Adapter.open(path, %{}))
  end

  test "open returns a typed error for a real database truncated to 100 bytes" do
    {bundle, path} = database_path("elixirdb-truncated-sqlite")
    assert {:ok, adapter} = Adapter.create(path, %{})
    assert :ok = Adapter.close(adapter)

    contents = File.read!(path)
    assert byte_size(contents) > 100
    File.write!(path, binary_part(contents, 0, 100))

    on_exit(fn -> ElixirDB.TempDatabase.cleanup(bundle) end)

    assert_typed_error(Adapter.open(path, %{}))
  end

  test "open rejects a foreign SQLite schema with unsupported_format" do
    {bundle, path} = database_path("elixirdb-foreign-sqlite")
    {:ok, conn} = Exqlite.Sqlite3.open(path)

    try do
      assert :ok = Exqlite.Sqlite3.execute(conn, "CREATE TABLE foreign_data(id INTEGER)")
    after
      assert :ok = Exqlite.Sqlite3.close(conn)
    end

    on_exit(fn -> ElixirDB.TempDatabase.cleanup(bundle) end)

    assert {:error, %ElixirDB.Error{code: :unsupported_format}} = Adapter.open(path, %{})
  end

  test "open freezes the zero-byte artifact result as unsupported_format" do
    path = corrupt_path("elixirdb-empty-sqlite", <<>>)

    assert {:error, %ElixirDB.Error{code: :unsupported_format}} = Adapter.open(path, %{})
  end

  test "open returns a typed error for a directory path" do
    {:ok, bundle} = ElixirDB.TempDatabase.create(prefix: "elixirdb-directory-sqlite")
    on_exit(fn -> ElixirDB.TempDatabase.cleanup(bundle) end)

    assert_typed_error(Adapter.open(bundle, %{}))
  end

  test "open_reader returns a typed error for a corrupt artifact" do
    path = corrupt_path("elixirdb-corrupt-reader", :crypto.strong_rand_bytes(4_096))

    writer = %Adapter{
      path: path,
      storage_mode: :disk,
      identity: %{database_uuid: ElixirDB.UUID.v4()}
    }

    assert_typed_error(Adapter.open_reader(writer))
  end

  test "open_reader rejects a writer UUID mismatch" do
    {bundle, path} = database_path("elixirdb-reader-uuid-mismatch")
    assert {:ok, writer} = Adapter.create(path, %{})

    on_exit(fn ->
      _ = Adapter.close(writer)
      ElixirDB.TempDatabase.cleanup(bundle)
    end)

    mismatched_writer = %{
      writer
      | identity: Map.put(writer.identity, :database_uuid, ElixirDB.UUID.v4())
    }

    assert {:error,
            %ElixirDB.Error{
              code: :database_unavailable,
              details: %{reason: :uuid_mismatch}
            }} = Adapter.open_reader(mismatched_writer)
  end

  defp corrupt_path(prefix, contents) do
    {bundle, path} = database_path(prefix)
    File.write!(path, contents)
    on_exit(fn -> ElixirDB.TempDatabase.cleanup(bundle) end)
    path
  end

  defp database_path(prefix) do
    {:ok, bundle} = ElixirDB.TempDatabase.create(prefix: prefix)
    {bundle, ElixirDB.TempDatabase.sqlite_path(bundle)}
  end

  defp assert_typed_error(result) do
    assert {:error, %ElixirDB.Error{}} = result
  end
end
