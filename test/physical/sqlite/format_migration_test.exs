defmodule VialKeeper.StorageAdapter.FormatMigrationTest do
  @moduledoc "Covers LIFE-006 / MAINT-008 fail-closed format recognition on open."

  use ExUnit.Case, async: true

  @moduletag :sqlite_physical

  alias VialKeeper.Storage.SQLite.Adapter
  alias VialKeeper.Storage.SQLite.Connection

  test "current Version 1 generation remains openable after close" do
    {bundle, path} = database_path("vialkeeper-format-v1-roundtrip")
    assert {:ok, adapter} = Adapter.create(path, %{})

    assert {:ok, %{revision: revision}} =
             Adapter.apply_local_mutation(adapter, %{
               operation: :put,
               document_id: "kept",
               body: %{"n" => 1}
             })

    assert :ok = Adapter.close(adapter)
    assert {:ok, reopened} = Adapter.open(path)

    assert {:ok, %{revision: ^revision, body: %{"n" => 1}}} =
             Adapter.get_document(reopened, %{document_id: "kept"})

    assert :ok = Adapter.close(reopened)
    VialKeeper.TempDatabase.cleanup(bundle)
  end

  test "CHECK constraints refuse an in-place file_format_version bump" do
    {_bundle, path} = closed_v1("vialkeeper-format-check")
    {:ok, conn} = Exqlite.Sqlite3.open(path)

    try do
      assert {:error, _reason} =
               Exqlite.Sqlite3.execute(conn, "UPDATE db_meta SET file_format_version = 2")
    after
      assert :ok = Exqlite.Sqlite3.close(conn)
    end

    assert {:ok, adapter} = Adapter.open(path)
    assert {:ok, %{file_format_version: 1}} = Adapter.identity(adapter)
    assert :ok = Adapter.close(adapter)
  end

  test "open refuses user_version 2 without rewriting the header" do
    {_bundle, path} = closed_v1("vialkeeper-format-user-version")
    exec!(path, "PRAGMA user_version = 2")
    mutated = File.read!(path)

    assert {:error, %VialKeeper.Error{code: :unsupported_format}} = Adapter.open(path)
    assert File.read!(path) == mutated
    assert user_version(path) == 2
  end

  test "open refuses a foreign SQLite file without converting it to WAL" do
    {bundle, path} = database_path("vialkeeper-format-foreign")
    {:ok, conn} = Exqlite.Sqlite3.open(path)

    try do
      assert :ok = Exqlite.Sqlite3.execute(conn, "CREATE TABLE foreign_data(id INTEGER)")
    after
      assert :ok = Exqlite.Sqlite3.close(conn)
    end

    on_exit(fn -> VialKeeper.TempDatabase.cleanup(bundle) end)
    before = File.read!(path)

    assert {:error, %VialKeeper.Error{code: :unsupported_format}} = Adapter.open(path)
    assert File.read!(path) == before
    assert journal_mode(path) == "delete"
  end

  test "open refuses a zero-byte artifact without initializing SQLite" do
    {bundle, path} = database_path("vialkeeper-format-empty")
    File.write!(path, <<>>)
    on_exit(fn -> VialKeeper.TempDatabase.cleanup(bundle) end)

    assert {:error, %VialKeeper.Error{code: :unsupported_format}} = Adapter.open(path)
    assert File.read!(path) == <<>>
  end

  test "open refuses random bytes without rewriting them" do
    {bundle, path} = database_path("vialkeeper-format-random")
    contents = :crypto.strong_rand_bytes(4_096)
    File.write!(path, contents)
    on_exit(fn -> VialKeeper.TempDatabase.cleanup(bundle) end)

    assert {:error, %VialKeeper.Error{code: :unsupported_format}} = Adapter.open(path)
    assert File.read!(path) == contents
  end

  test "open refuses a partial schema missing required tables" do
    {_bundle, path} = closed_v1("vialkeeper-format-missing-table")
    exec!(path, "DROP TABLE documents")

    assert {:error, %VialKeeper.Error{code: :unsupported_format}} = Adapter.open(path)
    assert table_exists?(path, "db_meta")
    refute table_exists?(path, "documents")
  end

  test "open refuses a future db_meta version and does not downgrade it in place" do
    {_bundle, path} = closed_v1("vialkeeper-format-meta-v2")
    rewrite_file_format_version!(path, 2)

    assert {:error, %VialKeeper.Error{code: :unsupported_format}} = Adapter.open(path)
    assert file_format_version(path) == 2
  end

  defp closed_v1(prefix) do
    {bundle, path} = database_path(prefix)
    assert {:ok, adapter} = Adapter.create(path, %{})
    assert :ok = Adapter.close(adapter)
    on_exit(fn -> VialKeeper.TempDatabase.cleanup(bundle) end)
    {bundle, path}
  end

  defp database_path(prefix) do
    {:ok, bundle} = VialKeeper.TempDatabase.create(prefix: prefix)
    {bundle, VialKeeper.TempDatabase.sqlite_path(bundle)}
  end

  defp exec!(path, sql) do
    {:ok, conn} = Connection.open(path)

    try do
      assert :ok = Connection.execute(conn, sql)
    after
      assert :ok = Connection.close(conn)
    end
  end

  defp rewrite_file_format_version!(path, version) do
    {:ok, conn} = Connection.open(path)

    try do
      assert {:ok, [row]} =
               Connection.query(
                 conn,
                 """
                 SELECT database_uuid, database_kind, history_epoch, logical_schema_version,
                        revision_algorithm_version, canonicalization_version, replication_protocol_major,
                        current_sequence, retention_floor_sequence, compaction_epoch,
                        retention_boundary_digest, created_at, config_json
                 FROM db_meta WHERE id = 1
                 """
               )

      [
        uuid,
        kind,
        epoch,
        schema,
        revision,
        canonical,
        protocol,
        sequence,
        floor,
        compaction,
        digest,
        created_at,
        config
      ] = row

      assert :ok = Connection.execute(conn, "PRAGMA foreign_keys = OFF")
      assert :ok = Connection.execute(conn, "BEGIN")

      assert :ok =
               Connection.execute(conn, """
               CREATE TABLE db_meta_next (
                 id INTEGER PRIMARY KEY CHECK (id = 1),
                 database_uuid TEXT NOT NULL UNIQUE,
                 database_kind TEXT NOT NULL,
                 history_epoch TEXT NOT NULL,
                 file_format_version INTEGER NOT NULL,
                 logical_schema_version INTEGER NOT NULL,
                 revision_algorithm_version INTEGER NOT NULL,
                 canonicalization_version INTEGER NOT NULL,
                 replication_protocol_major INTEGER NOT NULL,
                 current_sequence INTEGER NOT NULL,
                 retention_floor_sequence INTEGER NOT NULL,
                 compaction_epoch INTEGER NOT NULL,
                 retention_boundary_digest TEXT,
                 created_at TEXT NOT NULL,
                 config_json TEXT NOT NULL
               )
               """)

      assert :ok =
               Connection.execute(
                 conn,
                 """
                 INSERT INTO db_meta_next (
                   id, database_uuid, database_kind, history_epoch, file_format_version,
                   logical_schema_version, revision_algorithm_version, canonicalization_version,
                   replication_protocol_major, current_sequence, retention_floor_sequence,
                   compaction_epoch, retention_boundary_digest, created_at, config_json
                 ) VALUES (1, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                 """,
                 [
                   uuid,
                   kind,
                   epoch,
                   version,
                   schema,
                   revision,
                   canonical,
                   protocol,
                   sequence,
                   floor,
                   compaction,
                   digest,
                   created_at,
                   config
                 ]
               )

      assert :ok = Connection.execute(conn, "DROP TABLE db_meta")
      assert :ok = Connection.execute(conn, "ALTER TABLE db_meta_next RENAME TO db_meta")
      assert :ok = Connection.execute(conn, "COMMIT")
    after
      assert :ok = Connection.close(conn)
    end
  end

  defp file_format_version(path) do
    {:ok, conn} = Connection.open(path)

    try do
      assert {:ok, [[version]]} =
               Connection.query(conn, "SELECT file_format_version FROM db_meta WHERE id = 1")

      version
    after
      assert :ok = Connection.close(conn)
    end
  end

  defp user_version(path) do
    {:ok, conn} = Connection.open(path)

    try do
      assert {:ok, [[version]]} = Connection.pragma(conn, "user_version")
      version
    after
      assert :ok = Connection.close(conn)
    end
  end

  defp journal_mode(path) do
    {:ok, conn} = Connection.open(path)

    try do
      assert {:ok, [[mode]]} = Connection.pragma(conn, "journal_mode")
      String.downcase(to_string(mode))
    after
      assert :ok = Connection.close(conn)
    end
  end

  defp table_exists?(path, name) do
    {:ok, conn} = Connection.open(path)

    try do
      assert {:ok, rows} =
               Connection.query(
                 conn,
                 "SELECT name FROM sqlite_master WHERE type = 'table' AND name = ?",
                 [name]
               )

      rows != []
    after
      assert :ok = Connection.close(conn)
    end
  end
end
