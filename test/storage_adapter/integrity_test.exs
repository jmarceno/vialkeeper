defmodule ElixirDB.StorageAdapter.IntegrityTest do
  use ElixirDB.Storage.AdapterCase, adapter: ElixirDB.Storage.SQLite.Adapter

  alias ElixirDB.Attachments.FilesystemStore
  alias ElixirDB.Attachments.Manifest
  alias ElixirDB.JSON.Canonical
  alias ElixirDB.Storage.SQLite.Connection

  test "integrity passes on a fresh database", %{adapter: adapter} do
    assert {:ok, %{ok: true}} = @adapter.integrity_check(adapter, %{})
  end

  test "integrity detects corrupted revision digest", %{adapter: adapter} do
    assert {:ok, %{revision: revision}} =
             @adapter.apply_local_mutation(adapter, %{
               operation: :put,
               document_id: "doc",
               body: %{"n" => 1}
             })

    assert :ok =
             Connection.execute(
               adapter.conn,
               "UPDATE revisions SET digest = 'bad' WHERE revision_id = ?",
               [revision]
             )

    assert {:error, %ElixirDB.Error{code: :integrity_violation}} =
             @adapter.integrity_check(adapter, %{})
  end

  test "integrity detects change rows at or below the retention floor", %{adapter: adapter} do
    assert {:ok, %{revision: revision, sequence: sequence}} =
             @adapter.apply_local_mutation(adapter, %{
               operation: :put,
               document_id: "doc",
               body: %{"n" => 1}
             })

    assert :ok =
             Connection.execute(
               adapter.conn,
               "UPDATE db_meta SET retention_floor_sequence = ? WHERE id = 1",
               [sequence]
             )

    assert {:error, %ElixirDB.Error{code: :integrity_violation, message: message}} =
             @adapter.integrity_check(adapter, %{})

    assert message =~ "retention floor"
    assert revision != nil
  end

  test "integrity detects checkpoint installed compaction ahead of source", %{adapter: adapter} do
    value = %{
      "version" => 1,
      "replication_id" => "rep-integrity",
      "checkpoint_version" => 1,
      "session_id" => "sess",
      "source_sequence" => 3,
      "source_compaction_epoch" => 2,
      "installed_source_compaction_epoch" => 5,
      "safe_source_sequence" => 3,
      "source_history_epoch" => identity_epoch(adapter),
      "history" => []
    }

    {:ok, json} = Canonical.encode(value)

    assert :ok =
             Connection.execute(
               adapter.conn,
               "INSERT INTO local_records(namespace, record_key, record_version, value_json) VALUES ('checkpoints', 'rep-integrity', 1, ?)",
               [json]
             )

    assert {:error, %ElixirDB.Error{code: :integrity_violation, message: message}} =
             @adapter.integrity_check(adapter, %{})

    assert message =~ "installed compaction"
  end

  test "integrity detects non-descending checkpoint history", %{adapter: adapter} do
    value = %{
      "version" => 1,
      "replication_id" => "rep-history",
      "checkpoint_version" => 1,
      "session_id" => "sess",
      "source_sequence" => 5,
      "source_history_epoch" => identity_epoch(adapter),
      "source_compaction_epoch" => 0,
      "safe_source_sequence" => 5,
      "installed_source_compaction_epoch" => 0,
      "history" => [
        %{"session_id" => "a", "source_sequence" => 3},
        %{"session_id" => "b", "source_sequence" => 5}
      ]
    }

    {:ok, json} = Canonical.encode(value)

    assert :ok =
             Connection.execute(
               adapter.conn,
               "INSERT INTO local_records(namespace, record_key, record_version, value_json) VALUES ('checkpoints', 'rep-history', 1, ?)",
               [json]
             )

    assert {:error, %ElixirDB.Error{code: :integrity_violation, message: message}} =
             @adapter.integrity_check(adapter, %{})

    assert message =~ "checkpoint history"
  end

  test "integrity verifies referenced blobs and reports reclaimable orphans", %{
    adapter: adapter,
    path: path
  } do
    bundle = Path.dirname(path)
    payload = "integrity-blob-#{System.unique_integer([:positive])}"

    assert {:ok, writer} = FilesystemStore.begin_put(bundle, 1024, %{})
    assert :ok = FilesystemStore.write_chunk(writer, payload)

    assert {:ok, %{digest: digest, logical_size: size}} =
             FilesystemStore.finish_put(writer)

    assert {:ok, _} =
             @adapter.protect_pending_blob(adapter, %{digest: digest, logical_size: size})

    assert {:ok, %{revision: _}} =
             @adapter.apply_local_mutation(adapter, %{
               operation: :put,
               document_id: "att-doc",
               body: %{"n" => 1},
               attachments: %{
                 "a.bin" => Manifest.entry(digest, size, "application/octet-stream")
               }
             })

    assert {:ok, %{ok: true, reclaimable_blobs: 0}} = @adapter.integrity_check(adapter, %{})

    orphan = "orphan-only-#{System.unique_integer([:positive])}"
    assert {:ok, writer} = FilesystemStore.begin_put(bundle, 1024, %{})
    assert :ok = FilesystemStore.write_chunk(writer, orphan)

    assert {:ok, %{digest: orphan_digest}} =
             FilesystemStore.finish_put(writer)

    assert FilesystemStore.exists?(bundle, orphan_digest)

    assert {:ok, %{ok: true, reclaimable_blobs: reclaimable}} =
             @adapter.integrity_check(adapter, %{})

    assert reclaimable >= 1

    # Dual representation is corruption, not reclaimable garbage.
    prefix = String.slice(digest, 0, 2)
    dual = Path.join([bundle, "blobs", prefix, digest <> ".zst"])
    File.write!(dual, "bogus")

    assert {:error, %ElixirDB.Error{code: :integrity_violation, message: message}} =
             @adapter.integrity_check(adapter, %{})

    assert String.contains?(message, "multiple physical") or
             String.contains?(message, "physical verification")
  end

  test "integrity rejects malformed blob filenames", %{adapter: adapter, path: path} do
    bundle = Path.dirname(path)
    bad_dir = Path.join([bundle, "blobs", "ab"])
    File.mkdir_p!(bad_dir)
    File.write!(Path.join(bad_dir, "not-a-digest.raw"), "x")

    assert {:error, %ElixirDB.Error{code: :integrity_violation, message: message}} =
             @adapter.integrity_check(adapter, %{})

    assert message =~ "malformed"
  end

  test "integrity detects a missing physical blob still referenced by a revision", %{
    adapter: adapter,
    path: path
  } do
    bundle = Path.dirname(path)
    payload = "missing-referenced-blob"

    assert {:ok, writer} = FilesystemStore.begin_put(bundle, 1024, %{})
    assert :ok = FilesystemStore.write_chunk(writer, payload)
    assert {:ok, %{digest: digest, logical_size: size}} = FilesystemStore.finish_put(writer)
    assert {:ok, _} = @adapter.protect_pending_blob(adapter, %{digest: digest, logical_size: size})

    assert {:ok, _} =
             @adapter.apply_local_mutation(adapter, %{
               operation: :put,
               document_id: "missing-blob-doc",
               body: %{},
               attachments: %{
                 "missing.bin" => Manifest.entry(digest, size, "application/octet-stream")
               }
             })

    assert :ok = FilesystemStore.delete(bundle, digest)

    assert {:error, %ElixirDB.Error{code: :integrity_violation, message: message}} =
             @adapter.integrity_check(adapter, %{})

    assert message =~ "physical verification"
  end

  test "integrity rejects inconsistent logical sizes for the same digest", %{
    adapter: adapter,
    path: path
  } do
    alias ElixirDB.Revisions.Id

    bundle = Path.dirname(path)
    payload = "size-conflict-#{System.unique_integer([:positive])}"

    assert {:ok, writer} = FilesystemStore.begin_put(bundle, 1024, %{})
    assert :ok = FilesystemStore.write_chunk(writer, payload)

    assert {:ok, %{digest: digest, logical_size: size}} =
             FilesystemStore.finish_put(writer)

    assert {:ok, _} =
             @adapter.protect_pending_blob(adapter, %{digest: digest, logical_size: size})

    assert {:ok, %{revision: _}} =
             @adapter.apply_local_mutation(adapter, %{
               operation: :put,
               document_id: "size-a",
               body: %{"n" => 1},
               attachments: %{
                 "a.bin" => Manifest.entry(digest, size, "application/octet-stream")
               }
             })

    assert {:ok, %{revision: rev_b}} =
             @adapter.apply_local_mutation(adapter, %{
               operation: :put,
               document_id: "size-b",
               body: %{"n" => 1},
               attachments: %{
                 "b.bin" => Manifest.entry(digest, size, "application/octet-stream")
               }
             })

    wrong_size = max(size - 1, 0)

    attachments = %{
      "b.bin" => Manifest.entry(digest, wrong_size, "application/octet-stream")
    }

    assert {:ok, [[history_id]]} =
             Connection.query(
               adapter.conn,
               """
               SELECT r.history_id
               FROM revisions AS r
               JOIN documents AS d ON d.doc_key = r.doc_key
               WHERE d.document_id = 'size-b' AND r.revision_id = ?
               """,
               [rev_b]
             )

    assert {:ok, new_revision_id} =
             Id.calculate("size-b", history_id, nil, false, %{"n" => 1}, attachments)

    assert {:ok, expected_generation} = Id.generation(new_revision_id)
    digest_part = new_revision_id |> String.split("-", parts: 2) |> List.last()

    assert :ok = Connection.execute(adapter.conn, "PRAGMA foreign_keys = OFF")

    assert :ok =
             Connection.execute(
               adapter.conn,
               """
               UPDATE revision_attachments
               SET logical_size = ?, revision_id = ?
               WHERE doc_key = (SELECT doc_key FROM documents WHERE document_id = 'size-b')
               """,
               [wrong_size, new_revision_id]
             )

    assert :ok =
             Connection.execute(
               adapter.conn,
               """
               UPDATE revisions
               SET revision_id = ?, generation = ?, digest = ?
               WHERE doc_key = (SELECT doc_key FROM documents WHERE document_id = 'size-b')
               """,
               [new_revision_id, expected_generation, digest_part]
             )

    assert :ok = Connection.execute(adapter.conn, "PRAGMA foreign_keys = ON")

    assert {:error, %ElixirDB.Error{code: :integrity_violation, message: message}} =
             @adapter.integrity_check(adapter, %{})

    assert message =~ "inconsistent logical sizes" or
             message =~ "physical verification"
  end

  defp identity_epoch(adapter) do
    {:ok, identity} = @adapter.identity(adapter)
    identity.history_epoch
  end
end
