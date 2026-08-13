defmodule ElixirDB.Attachments.StoreRefTest do
  use ExUnit.Case, async: true

  alias ElixirDB.Attachments.{FilesystemStore, StoreRef}
  alias ElixirDB.DatabaseBundle
  alias ElixirDB.TempDatabase

  test "external read-only refs read the shared CAS and reject every write entry point" do
    {:ok, bundle_path} = TempDatabase.create(prefix: "elixirdb-external-cas")
    {:ok, bundle} = DatabaseBundle.create(bundle_path)

    on_exit(fn -> TempDatabase.cleanup(bundle_path) end)

    {:ok, writer} = FilesystemStore.begin_put(StoreRef.bundle_local(bundle), 100, %{})
    assert :ok = FilesystemStore.write_chunk(writer, "shared")
    assert {:ok, %{digest: digest, logical_size: 6}} = FilesystemStore.finish_put(writer)

    external = StoreRef.external_read_only(bundle.blobs_path, [bundle.root])
    assert {:ok, %{physical_size: _}} = FilesystemStore.stat(external, digest)
    assert FilesystemStore.exists?(external, digest)
    assert :ok = FilesystemStore.verify(external, digest, 6)

    assert {:error, %{code: :shadow_attachment_store_read_only}} =
             FilesystemStore.begin_put(external, 100, %{})

    assert {:error, %{code: :shadow_attachment_store_read_only}} =
             FilesystemStore.begin_put_representation(external, %{}, 100)

    assert {:error, %{code: :shadow_attachment_store_read_only}} =
             FilesystemStore.delete(external, digest)

    assert {:error, %{code: :shadow_attachment_store_read_only}} =
             FilesystemStore.cleanup_tmp(external, DateTime.utc_now())
  end

  test "external refs cannot escape an allowed root or use a non-CAS directory" do
    assert {:error, %{code: :invalid_request}} =
             StoreRef.normalize(StoreRef.external_read_only("/tmp/not-blobs", ["/tmp"]))

    ref = StoreRef.external_read_only("/tmp/source/blobs", ["/tmp/other"])
    assert {:error, %{code: :invalid_request}} = StoreRef.normalize(ref)
  end
end
