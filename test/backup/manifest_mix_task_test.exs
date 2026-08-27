defmodule Mix.Tasks.Vialkeeper.Backup.ManifestTest do
  use ExUnit.Case, async: false

  alias VialKeeper.Backup.Manifest
  alias VialKeeper.Storage.SQLite.Adapter
  alias VialKeeper.UUID

  @moduletag :tmp_dir

  test "runs a post-close integrity check and writes the manifest", %{tmp_dir: tmp_dir} do
    bundle = Path.join(tmp_dir, "notes.vialkeeper")
    sqlite = Path.join(bundle, "database.sqlite3")
    File.mkdir_p!(Path.join(bundle, "blobs"))
    File.mkdir_p!(Path.join(bundle, "tmp"))
    uuid = UUID.v4()

    assert {:ok, adapter} =
             Adapter.create(sqlite, %{storage_mode: :disk, database_uuid: uuid})

    assert :ok = Adapter.close(adapter)

    original_shell = Mix.shell()
    Mix.shell(Mix.Shell.Process)
    on_exit(fn -> Mix.shell(original_shell) end)
    Mix.Task.reenable("vialkeeper.backup.manifest")

    assert :ok =
             Mix.Tasks.Vialkeeper.Backup.Manifest.run([
               bundle,
               "--source-path",
               "notes.vialkeeper"
             ])

    manifest_path = Manifest.manifest_path_for_bundle(bundle)
    assert {:ok, manifest_json} = File.read(manifest_path)
    assert {:ok, manifest} = Manifest.decode_json(manifest_json)
    assert manifest["database_uuid"] == uuid
    assert manifest["integrity"]["ok"] == true
    assert :ok = Manifest.verify(bundle, manifest)
  end
end
