defmodule ElixirDB.EndToEnd.OfflineCopyTest do
  @moduledoc "Covers offline bundle copy, registration, and integrity validation."

  use ExUnit.Case, async: false

  @moduletag :sqlite_physical
  @moduletag :integration

  alias ElixirDB.Attachments
  alias ElixirDB.Attachments.FilesystemStore
  alias ElixirDB.Documents
  alias ElixirDB.Runtime.DatabaseCatalog

  @tag :slow
  test "offline copy, registration, derived index rebuild, and integrity" do
    source_rel = "e2e-offline-src-#{System.unique_integer([:positive])}.elixirdb"
    dest_rel = "e2e-offline-dst-#{System.unique_integer([:positive])}.elixirdb"
    root = ElixirDB.Config.database_root()
    source_abs = Path.join(root, source_rel)
    dest_abs = Path.join(root, dest_rel)

    for path <- [source_abs, dest_abs] do
      ElixirDB.TempDatabase.cleanup(path)
    end

    assert {:ok, source} = DatabaseCatalog.create(source_rel)
    source_uuid = source.database_uuid

    assert {:ok, %{revision: revision}} =
             Documents.put(source_uuid, %{
               id: "portable",
               body: %{"copied" => true, "kind" => "task"}
             })

    assert {:ok, _} =
             Documents.put(source_uuid, %{
               id: "other",
               body: %{"copied" => true, "kind" => "note"}
             })

    assert {:ok, %{"index_id" => structured_id}} =
             ElixirDB.Query.create_index(source_uuid, %{
               "name" => "by-kind",
               "type" => "structured",
               "fields" => [%{"path" => "/kind", "type" => "string", "direction" => "asc"}]
             })

    assert {:ok, %{"index_id" => fts_id}} =
             ElixirDB.Query.create_index(source_uuid, %{
               "name" => "kind-text",
               "type" => "full_text",
               "fields" => ["/kind"],
               "tokenization" => %{"strategy" => "unicode_words_v1", "diacritics" => "preserve"}
             })

    assert {:ok, %{documents: pre_docs, selected_index: ^structured_id}} =
             ElixirDB.Query.execute(source_uuid, %{
               "selector" => %{"/kind" => "task"},
               "index" => "by-kind",
               "limit" => 10
             })

    assert Enum.any?(pre_docs, &(&1.id == "portable"))

    # Closed copy — LIFE-009: database must be closed before offline portability.
    assert :ok = DatabaseCatalog.close(source_uuid)
    source_sqlite = ElixirDB.TempDatabase.sqlite_path(source_abs)
    refute File.exists?(source_sqlite <> "-journal")
    refute File.exists?(source_sqlite <> "-wal")
    File.cp_r!(source_abs, dest_abs)

    # Same UUID cannot register twice on this host; unregister the source route first.
    assert :ok = DatabaseCatalog.unregister(source_uuid)
    assert {:ok, restored} = DatabaseCatalog.register(dest_rel)
    assert restored.database_uuid == source_uuid

    on_exit(fn ->
      _ = DatabaseCatalog.close(source_uuid)
      _ = DatabaseCatalog.unregister(source_uuid)
      ElixirDB.TempDatabase.cleanup(source_abs)
      ElixirDB.TempDatabase.cleanup(dest_abs)
    end)

    assert {:ok, %{revision: ^revision, body: %{"copied" => true, "kind" => "task"}}} =
             Documents.get(source_uuid, %{id: "portable"})

    assert {:ok, indexes} = ElixirDB.Query.list_indexes(source_uuid)
    index_ids = MapSet.new(Enum.map(indexes, & &1["index_id"]))
    assert MapSet.member?(index_ids, structured_id)
    assert MapSet.member?(index_ids, fts_id)

    # Rebuild every derived index after registration.
    for %{"index_id" => index_id} <- indexes do
      assert {:ok, %{rebuilt: true}} = ElixirDB.Query.rebuild_index(source_uuid, index_id)
    end

    assert {:ok, %{documents: docs, selected_index: ^structured_id}} =
             ElixirDB.Query.execute(source_uuid, %{
               "selector" => %{"/kind" => "task"},
               "index" => "by-kind",
               "limit" => 10
             })

    assert [_] = docs
    assert hd(docs).id == "portable"

    assert {:ok, %{documents: fts_docs, selected_index: ^fts_id}} =
             ElixirDB.Query.execute(source_uuid, %{
               "search" => %{"index" => "kind-text", "text" => "task", "mode" => "all"},
               "limit" => 10
             })

    assert Enum.any?(fts_docs, &(&1.id == "portable"))

    assert {:ok, %{ok: true}} =
             DatabaseCatalog.command(source_uuid, {:command, :integrity_check, %{}})
  end

  @tag :slow
  test "copied bundle preserves attachments, encodings, and ignores lease files" do
    source_rel = "e2e-offline-att-src-#{System.unique_integer([:positive])}.elixirdb"
    dest_rel = "e2e-offline-att-dst-#{System.unique_integer([:positive])}.elixirdb"
    root = ElixirDB.Config.database_root()
    source_abs = Path.join(root, source_rel)
    dest_abs = Path.join(root, dest_rel)

    for path <- [source_abs, dest_abs], do: ElixirDB.TempDatabase.cleanup(path)

    assert {:ok, source} = DatabaseCatalog.create(source_rel)
    uuid = source.database_uuid

    on_exit(fn ->
      _ = DatabaseCatalog.close(uuid)
      _ = DatabaseCatalog.unregister(uuid)
      ElixirDB.TempDatabase.cleanup(source_abs)
      ElixirDB.TempDatabase.cleanup(dest_abs)
    end)

    raw_payload = "raw-portable-#{System.unique_integer([:positive])}"
    zst_payload = :binary.copy(<<0>>, 300 * 1024)

    assert {:ok, %{blob: raw_digest, length: raw_len}} =
             Attachments.upload_stream(uuid, [raw_payload])

    assert {:ok, %{blob: zst_digest, length: zst_len}} =
             Attachments.upload_stream(uuid, [zst_payload])

    assert {:ok, %{encoding: :raw}} = FilesystemStore.stat(source_abs, raw_digest)
    assert {:ok, %{encoding: :compressed}} = FilesystemStore.stat(source_abs, zst_digest)

    assert {:ok, %{revision: revision}} =
             Documents.put(uuid, %{
               "id" => "with-attachments",
               "body" => %{"kind" => "portable"},
               "attachments" => %{
                 "note.txt" => %{
                   "blob" => raw_digest,
                   "content_type" => "text/plain"
                 },
                 "zeros.bin" => %{
                   "blob" => zst_digest,
                   "content_type" => "application/octet-stream"
                 }
               }
             })

    # Active close waits for attachment activity (none left) then becomes portable.
    assert :ok = DatabaseCatalog.close(uuid)
    source_sqlite = ElixirDB.TempDatabase.sqlite_path(source_abs)
    refute File.exists?(source_sqlite <> "-journal")

    # Lease is non-authoritative: copying a closed bundle (lease file may remain
    # unlocked on disk) must still register and open on another path.
    File.cp_r!(source_abs, dest_abs)

    assert :ok = DatabaseCatalog.unregister(uuid)
    assert {:ok, restored} = DatabaseCatalog.register(dest_rel)
    assert restored.database_uuid == uuid

    assert {:ok, %{revision: ^revision, attachments: attachments}} =
             Documents.get(uuid, %{id: "with-attachments"})

    assert map_size(attachments) == 2

    assert {:ok, stream} =
             Attachments.open_stream(uuid, %{
               "id" => "with-attachments",
               "name" => "note.txt"
             })

    assert IO.iodata_to_binary(Enum.to_list(stream.body)) == raw_payload
    assert stream.content_length == raw_len
    stream.close.()

    assert {:ok, zst_stream} =
             Attachments.open_stream(uuid, %{
               "id" => "with-attachments",
               "name" => "zeros.bin"
             })

    assert IO.iodata_to_binary(Enum.to_list(zst_stream.body)) == zst_payload
    assert zst_stream.content_length == zst_len
    zst_stream.close.()

    assert {:ok, %{encoding: :raw}} = FilesystemStore.stat(dest_abs, raw_digest)
    assert {:ok, %{encoding: :compressed}} = FilesystemStore.stat(dest_abs, zst_digest)

    assert {:ok, %{ok: true, reclaimable_blobs: 0}} =
             DatabaseCatalog.command(uuid, {:command, :integrity_check, %{}})
  end

  @tag :slow
  test "crash after install before commit leaves orphan; after commit never missing blob" do
    rel = "e2e-offline-crash-#{System.unique_integer([:positive])}.elixirdb"
    abs = Path.join(ElixirDB.Config.database_root(), rel)
    ElixirDB.TempDatabase.cleanup(abs)

    assert {:ok, identity} = DatabaseCatalog.create(rel)
    uuid = identity.database_uuid

    on_exit(fn ->
      _ = DatabaseCatalog.close(uuid)
      _ = DatabaseCatalog.unregister(uuid)
      ElixirDB.TempDatabase.cleanup(abs)
    end)

    # Incomplete tmp is non-authoritative and cleaned on prepare_for_open.
    tmp = Path.join([abs, "tmp", "incomplete-upload"])
    File.write!(tmp, "partial-bytes")
    File.touch!(tmp, {{2000, 1, 1}, {0, 0, 0}})
    assert :ok = DatabaseCatalog.close(uuid)
    assert {:ok, _} = DatabaseCatalog.open(uuid)
    refute File.exists?(tmp)

    # Install durable blob then "crash" before revision commit: orphan only.
    # Direct store install (no pending protection) models post-install / pre-protect failure.
    orphan_payload = "orphan-#{System.unique_integer([:positive])}"

    assert {:ok, writer} = FilesystemStore.begin_put(abs, byte_size(orphan_payload) + 1, %{})
    assert :ok = FilesystemStore.write_chunk(writer, orphan_payload)

    assert {:ok, %{digest: orphan_digest, logical_size: orphan_len}} =
             FilesystemStore.finish_put(writer)

    assert FilesystemStore.exists?(abs, orphan_digest)
    assert :ok = FilesystemStore.verify(abs, orphan_digest, orphan_len)

    assert {:ok, %{ok: true, reclaimable_blobs: reclaimable}} =
             DatabaseCatalog.command(uuid, {:command, :integrity_check, %{}})

    assert reclaimable >= 1

    assert {:error, %ElixirDB.Error{code: :document_not_found}} =
             Documents.get(uuid, %{id: "never-committed"})

    # After revision commit the blob remains reachable and integrity-clean.
    committed = "committed-#{System.unique_integer([:positive])}"

    assert {:ok, %{blob: digest, length: len}} =
             Attachments.upload_stream(uuid, [committed])

    assert {:ok, _} =
             Documents.put(uuid, %{
               "id" => "committed-att",
               "body" => %{"ok" => true},
               "attachments" => %{
                 "file.bin" => %{"blob" => digest, "content_type" => "application/octet-stream"}
               }
             })

    assert :ok = FilesystemStore.verify(abs, digest, len)

    assert {:ok, stream} =
             Attachments.open_stream(uuid, %{"id" => "committed-att", "name" => "file.bin"})

    assert IO.iodata_to_binary(Enum.to_list(stream.body)) == committed
    stream.close.()

    assert {:ok, %{ok: true}} =
             DatabaseCatalog.command(uuid, {:command, :integrity_check, %{}})
  end
end
