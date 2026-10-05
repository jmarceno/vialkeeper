defmodule VialKeeper.Storage.Turso.BackupManifestTest do
  @moduledoc "Offline backup manifests for closed Turso bundles."
  use ExUnit.Case, async: true

  @moduletag :turso_physical
  @moduletag :tmp_dir

  alias VialKeeper.Backup.Manifest
  alias VialKeeper.Storage.SQLite.BackupManifest
  alias VialKeeper.Storage.Turso.Adapter
  alias VialKeeper.UUID

  test "a closed Turso bundle yields a verified manifest and keeps no sidecars", %{
    tmp_dir: tmp_dir
  } do
    bundle = Path.join(tmp_dir, "notes.vialkeeper")
    artifact = Adapter.artifact_path(bundle)
    File.mkdir_p!(Path.join(bundle, "blobs"))
    File.mkdir_p!(Path.join(bundle, "tmp"))
    uuid = UUID.v4()

    assert {:ok, adapter} = Adapter.create(artifact, %{database_uuid: uuid})

    assert {:ok, _} =
             Adapter.apply_local_mutation(adapter, %{
               operation: :put,
               document_id: "doc",
               body: %{"n" => 1}
             })

    assert :ok = Adapter.close(adapter)
    assert sidecars(artifact) == []

    assert {:ok, %{database_uuid: ^uuid}} = BackupManifest.read_bundle_identity(bundle)
    assert {:ok, %{ok: true}} = BackupManifest.integrity_check(bundle)
    assert sidecars(artifact) == []

    assert {:ok, manifest} = BackupManifest.write(bundle, %{source_path: "notes.vialkeeper"})
    assert manifest["database_uuid"] == uuid
    assert manifest["artifacts"]["sqlite"]["relative_path"] == Path.basename(artifact)
    assert :ok = Manifest.verify(bundle, manifest)
    assert sidecars(artifact) == []
  end

  test "an open Turso bundle is refused", %{tmp_dir: tmp_dir} do
    bundle = Path.join(tmp_dir, "open.vialkeeper")
    File.mkdir_p!(bundle)
    assert {:ok, adapter} = Adapter.create(Adapter.artifact_path(bundle), %{})
    on_exit(fn -> Adapter.close(adapter) end)

    assert {:error, %VialKeeper.Error{code: :invalid_request}} =
             BackupManifest.read_bundle_identity(bundle)
  end

  defp sidecars(artifact),
    do: Enum.filter(["-wal", "-log", "-shm"], &File.exists?(artifact <> &1))
end
