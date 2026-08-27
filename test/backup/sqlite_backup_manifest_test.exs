defmodule VialKeeper.Storage.SQLite.BackupManifestTest do
  use ExUnit.Case, async: true

  alias VialKeeper.Storage.SQLite.Adapter
  alias VialKeeper.Storage.SQLite.BackupManifest
  alias VialKeeper.UUID

  @moduletag :tmp_dir

  test "reads bundle identity without recreating SQLite sidecars", %{tmp_dir: tmp_dir} do
    bundle = Path.join(tmp_dir, "notes.vialkeeper")
    sqlite = Path.join(bundle, "database.sqlite3")
    File.mkdir_p!(Path.join(bundle, "blobs"))
    File.mkdir_p!(Path.join(bundle, "tmp"))
    uuid = UUID.v4()

    assert {:ok, adapter} =
             Adapter.create(sqlite, %{storage_mode: :disk, database_uuid: uuid})

    assert :ok = Adapter.close(adapter)
    refute File.exists?(sqlite <> "-wal")
    refute File.exists?(sqlite <> "-shm")

    assert {:ok, %{database_uuid: ^uuid, storage: storage}} =
             BackupManifest.read_bundle_identity(bundle)

    assert storage == %{
             "canonicalization_version" => 1,
             "file_format_version" => 1,
             "logical_schema_version" => 1,
             "replication_protocol_major" => 1,
             "revision_algorithm_version" => 1
           }

    refute File.exists?(sqlite <> "-wal")
    refute File.exists?(sqlite <> "-shm")
  end

  test "does not create a missing SQLite artifact", %{tmp_dir: tmp_dir} do
    bundle = Path.join(tmp_dir, "missing.vialkeeper")
    File.mkdir_p!(bundle)
    sqlite = Path.join(bundle, "database.sqlite3")

    assert {:error, %VialKeeper.Error{code: :database_unavailable}} =
             BackupManifest.read_bundle_identity(bundle)

    refute File.exists?(sqlite)
  end

  test "write/2 derives identity from the bundle instead of trusting overrides", %{tmp_dir: tmp_dir} do
    bundle = Path.join(tmp_dir, "notes.vialkeeper")
    sqlite = Path.join(bundle, "database.sqlite3")
    File.mkdir_p!(Path.join(bundle, "blobs"))
    File.mkdir_p!(Path.join(bundle, "tmp"))
    uuid = UUID.v4()
    false_uuid = UUID.v4()

    assert {:ok, adapter} =
             Adapter.create(sqlite, %{storage_mode: :disk, database_uuid: uuid})

    assert :ok = Adapter.close(adapter)

    assert {:ok, manifest} =
             BackupManifest.write(bundle, %{
               source_path: "notes.vialkeeper",
               integrity: %{"ok" => true},
               database_uuid: false_uuid,
               storage: %{"file_format_version" => 999}
             })

    assert manifest["database_uuid"] == uuid
    assert manifest["storage"]["file_format_version"] == 1
  end
end
