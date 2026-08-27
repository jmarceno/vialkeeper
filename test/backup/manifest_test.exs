defmodule VialKeeper.Backup.ManifestTest do
  use ExUnit.Case, async: true

  alias VialKeeper.Backup.Manifest
  alias VialKeeper.Diagnostics
  alias VialKeeper.UUID

  @moduletag :tmp_dir

  defp temp_bundle do
    base = System.tmp_dir!()

    bundle =
      Path.join(base, "vialkeeper-backup-test-#{System.unique_integer([:positive])}.vialkeeper")

    File.mkdir_p!(Path.join(bundle, "blobs"))
    File.mkdir_p!(Path.join(bundle, "tmp"))
    sqlite_path = Path.join(bundle, "database.sqlite3")
    File.write!(sqlite_path, "sqlite-content-#{System.unique_integer([:positive])}")
    # create a dummy blob
    digest = :crypto.hash(:sha256, "hello") |> Base.encode16(case: :lower)
    prefix = String.slice(digest, 0, 2)
    dir = Path.join([bundle, "blobs", prefix])
    File.mkdir_p!(dir)
    File.write!(Path.join(dir, "#{digest}.blob"), "hello-blob-content")
    {bundle, digest}
  end

  defp valid_attrs(bundle_path, opts \\ []) do
    generation_id = Keyword.get(opts, :generation_id, UUID.v4())
    database_uuid = Keyword.get(opts, :database_uuid, UUID.v4())
    source_path = Keyword.get(opts, :source_path, Path.basename(bundle_path))

    %{
      generation_id: generation_id,
      database_uuid: database_uuid,
      source_path: source_path,
      bundle_path: bundle_path,
      created_at: DateTime.utc_now(),
      diagnostics: Diagnostics.runtime(),
      storage: %{
        "file_format_version" => 1,
        "logical_schema_version" => 1,
        "revision_algorithm_version" => 1,
        "canonicalization_version" => 1,
        "replication_protocol_major" => 1
      },
      integrity: %{"ok" => true, "checked_at" => DateTime.utc_now() |> DateTime.to_iso8601()}
    }
  end

  test "build/1 computes hashes and validates" do
    {bundle, _digest} = temp_bundle()
    on_exit(fn -> File.rm_rf(bundle) end)

    attrs = valid_attrs(bundle)
    assert {:ok, manifest} = Manifest.build(attrs)
    assert manifest["manifest_version"] == 1
    assert manifest["generation_id"] == attrs.generation_id
    assert manifest["database_uuid"] == attrs.database_uuid
    assert manifest["source_path"] == attrs.source_path
    assert is_integer(manifest["bundle_bytes"])
    assert manifest["artifacts"]["sqlite"]["bytes"] > 0
    assert manifest["artifacts"]["sqlite"]["sha256"] =~ ~r/^[0-9a-f]{64}$/
    assert manifest["artifacts"]["blobs"]["count"] == 1
    assert manifest["artifacts"]["blobs"]["sha256"] =~ ~r/^[0-9a-f]{64}$/
    assert :ok = Manifest.validate(manifest)
  end

  test "write/2 and verify/2 round-trip" do
    {bundle, _digest} = temp_bundle()
    on_exit(fn -> File.rm_rf(bundle) end)

    attrs = valid_attrs(bundle)
    assert {:ok, manifest} = Manifest.write(bundle, Map.put(attrs, :integrity, %{"ok" => true}))
    manifest_path = Manifest.manifest_path_for_bundle(bundle)
    assert File.exists?(manifest_path)
    assert :ok = Manifest.verify(bundle, manifest_path)
    assert :ok = Manifest.verify(bundle, manifest)

    # tamper with sqlite
    sqlite_path = Path.join(bundle, "database.sqlite3")
    File.write!(sqlite_path, "tampered")

    assert {:error, %VialKeeper.Error{code: :integrity_violation}} =
             Manifest.verify(bundle, manifest)
  end

  test "write/2 rejects hot journal and lease" do
    {bundle, _} = temp_bundle()
    on_exit(fn -> File.rm_rf(bundle) end)

    wal = Path.join(bundle, "database.sqlite3-wal")
    File.write!(wal, "wal")
    attrs = valid_attrs(bundle)
    assert {:error, %VialKeeper.Error{code: :invalid_request}} = Manifest.write(bundle, attrs)
    File.rm!(wal)

    lease = bundle <> ".lease"
    File.write!(lease, "lease")
    assert {:error, %VialKeeper.Error{code: :invalid_request}} = Manifest.write(bundle, attrs)
    File.rm!(lease)
  end

  test "write/2 rejects a manifest path inside the bundle" do
    {bundle, _} = temp_bundle()
    on_exit(fn -> File.rm_rf(bundle) end)
    manifest_path = Path.join(bundle, "manifest.json")

    assert {:error, %VialKeeper.Error{code: :invalid_request}} =
             Manifest.write(bundle, Map.put(valid_attrs(bundle), :manifest_path, manifest_path))

    refute File.exists?(manifest_path)
  end

  test "write/2 rejects a manifest path that is not beside the bundle" do
    {bundle, _} = temp_bundle()
    on_exit(fn -> File.rm_rf(bundle) end)
    other_dir = Path.join(System.tmp_dir!(), "other-#{System.unique_integer([:positive])}")
    on_exit(fn -> File.rm_rf(other_dir) end)
    manifest_path = Path.join(other_dir, "manifest.json")

    assert {:error, %VialKeeper.Error{code: :invalid_request}} =
             Manifest.write(bundle, Map.put(valid_attrs(bundle), :manifest_path, manifest_path))

    refute File.exists?(manifest_path)
  end

  test "validate/1 rejects missing integrity.ok" do
    {bundle, _} = temp_bundle()
    on_exit(fn -> File.rm_rf(bundle) end)

    attrs = valid_attrs(bundle) |> Map.put(:integrity, %{"ok" => false})
    assert {:error, %VialKeeper.Error{code: :invalid_request}} = Manifest.build(attrs)

    attrs2 = valid_attrs(bundle) |> Map.delete(:integrity)
    # build will fail because integrity required
    assert {:error, _} = Manifest.build(Map.delete(attrs2, :integrity))
  end

  test "build/1 rejects unsafe source paths and non-v4 generation ids" do
    {bundle, _} = temp_bundle()
    on_exit(fn -> File.rm_rf(bundle) end)

    assert {:error, %VialKeeper.Error{code: :invalid_request}} =
             Manifest.build(valid_attrs(bundle, source_path: "../notes.vialkeeper"))

    assert {:error, %VialKeeper.Error{code: :invalid_request}} =
             Manifest.build(
               valid_attrs(bundle,
                 generation_id: "550e8400-e29b-11d4-a716-446655440000"
               )
             )
  end

  test "validate/1 requires every storage version and per-blob inventory" do
    {bundle, _} = temp_bundle()
    on_exit(fn -> File.rm_rf(bundle) end)
    assert {:ok, manifest} = Manifest.build(valid_attrs(bundle))

    incomplete_storage =
      update_in(manifest["storage"], &Map.delete(&1, "canonicalization_version"))

    assert {:error, %VialKeeper.Error{code: :invalid_request}} =
             Manifest.validate(incomplete_storage)

    missing_entries = update_in(manifest["artifacts"]["blobs"], &Map.delete(&1, "entries"))

    assert {:error, %VialKeeper.Error{code: :invalid_request}} =
             Manifest.validate(missing_entries)

    incomplete_diagnostics =
      update_in(manifest["diagnostics"], &Map.delete(&1, "storage_backend"))

    assert {:error, %VialKeeper.Error{code: :invalid_request}} =
             Manifest.validate(incomplete_diagnostics)
  end

  test "build/1 rejects nested atom and string key collisions" do
    {bundle, _} = temp_bundle()
    on_exit(fn -> File.rm_rf(bundle) end)

    attrs =
      valid_attrs(bundle)
      |> Map.put(:integrity, %{"ok" => true, ok: true})

    assert {:error, %VialKeeper.Error{code: :invalid_request}} = Manifest.build(attrs)
  end

  test "manifest_path_for_bundle/1" do
    assert Manifest.manifest_path_for_bundle("notes.vialkeeper") == "notes.vialkeeper.manifest.json"

    assert Manifest.manifest_path_for_bundle("/tmp/a/b.vialkeeper") ==
             "/tmp/a/b.vialkeeper.manifest.json"
  end

  test "encode_json/1 and decode_json/1 round-trip" do
    {bundle, _} = temp_bundle()
    on_exit(fn -> File.rm_rf(bundle) end)

    attrs = valid_attrs(bundle)
    assert {:ok, manifest} = Manifest.build(attrs)
    assert {:ok, json} = Manifest.encode_json(manifest)
    assert {:ok, decoded} = Manifest.decode_json(json)
    assert decoded == manifest
  end

  test "blobs tree hash is empty for no blobs" do
    base = System.tmp_dir!()

    bundle =
      Path.join(base, "vialkeeper-backup-empty-#{System.unique_integer([:positive])}.vialkeeper")

    File.mkdir_p!(Path.join(bundle, "blobs"))
    File.mkdir_p!(Path.join(bundle, "tmp"))
    File.write!(Path.join(bundle, "database.sqlite3"), "empty")
    on_exit(fn -> File.rm_rf(bundle) end)

    attrs = valid_attrs(bundle)
    assert {:ok, manifest} = Manifest.build(attrs)
    assert manifest["artifacts"]["blobs"]["count"] == 0
    assert manifest["artifacts"]["blobs"]["bytes"] == 0
    # hash of empty string
    assert manifest["artifacts"]["blobs"]["sha256"] ==
             :crypto.hash(:sha256, "") |> Base.encode16(case: :lower)
  end

  test "verify detects blob count mismatch" do
    {bundle, digest} = temp_bundle()
    on_exit(fn -> File.rm_rf(bundle) end)

    attrs = valid_attrs(bundle)
    assert {:ok, manifest} = Manifest.build(attrs)
    # remove blob
    prefix = String.slice(digest, 0, 2)
    File.rm!(Path.join([bundle, "blobs", prefix, "#{digest}.blob"]))

    assert {:error, %VialKeeper.Error{code: :integrity_violation}} =
             Manifest.verify(bundle, manifest)
  end
end
