defmodule ElixirDB.StorageAdapter.RevisionTransferTest do
  use ElixirDB.Storage.AdapterCase, adapter: ElixirDB.Storage.SQLite.Adapter

  alias ElixirDB.Attachments.FilesystemStore
  alias ElixirDB.Attachments.Manifest
  alias ElixirDB.Storage.SQLite.Connection
  alias ElixirDB.TestRevisionId, as: Id

  test "diff and import transfer a root-to-leaf chain", %{adapter: source} do
    {:ok, dest_bundle} = ElixirDB.TempDatabase.create(prefix: "elixirdb-rev-dest")
    dest_path = ElixirDB.TempDatabase.sqlite_path(dest_bundle)
    assert {:ok, dest} = @adapter.create(dest_path, %{})

    on_exit(fn ->
      _ = @adapter.close(dest)
      ElixirDB.TempDatabase.cleanup(dest_bundle)
    end)

    assert {:ok, %{revision: root}} =
             @adapter.apply_local_mutation(source, %{
               operation: :put,
               document_id: "doc",
               body: %{"n" => 0}
             })

    assert {:ok, %{revision: leaf}} =
             @adapter.apply_local_mutation(source, %{
               operation: :put,
               document_id: "doc",
               if_revision: root,
               body: %{"n" => 1}
             })

    assert {:ok, %{documents: [%{document_id: "doc", missing_revisions: [^leaf]}]}} =
             @adapter.diff_revisions(dest, %{
               documents: [%{document_id: "doc", leaf_revisions: [leaf]}]
             })

    assert {:ok, %{chains: [chain]}} =
             @adapter.get_revision_chains(source, %{
               documents: [%{document_id: "doc", leaf_revisions: [leaf]}]
             })

    assert chain.leaf_revision == leaf
    assert [_, _] = chain.revisions

    assert {:ok, %{revisions_inserted: 2, documents_changed: 1}} =
             @adapter.import_revision_chains(dest, %{chains: [chain]})

    assert {:ok, %{body: %{"n" => 1}, revision: ^leaf}} =
             @adapter.get_document(dest, %{document_id: "doc"})

    assert {:ok, %{documents: [%{document_id: "doc", missing_revisions: []}]}} =
             @adapter.diff_revisions(dest, %{
               documents: [%{document_id: "doc", leaf_revisions: [leaf]}]
             })
  end

  test "import refuses missing physical attachment dependency", %{
    adapter: source,
    bundle_path: source_bundle
  } do
    payload = "attachment-bytes"
    {:ok, digest, length} = install_blob(source_bundle, payload)

    attachments = %{
      "file.bin" => Manifest.entry(digest, length, "application/octet-stream")
    }

    assert {:ok, _} =
             @adapter.protect_pending_blob(source, %{digest: digest, logical_size: length})

    assert {:ok, %{revision: revision}} =
             @adapter.apply_local_mutation(source, %{
               operation: :put,
               document_id: "attached",
               body: %{"n" => 1},
               attachments: attachments
             })

    assert {:ok, %{chains: [chain]}} =
             @adapter.get_revision_chains(source, %{
               documents: [%{document_id: "attached", leaf_revisions: [revision]}]
             })

    {:ok, dest_bundle} = ElixirDB.TempDatabase.create(prefix: "elixirdb-rev-missing-blob")
    dest_path = ElixirDB.TempDatabase.sqlite_path(dest_bundle)
    assert {:ok, dest} = @adapter.create(dest_path, %{})

    on_exit(fn ->
      _ = @adapter.close(dest)
      ElixirDB.TempDatabase.cleanup(dest_bundle)
    end)

    assert {:error, %ElixirDB.Error{code: :attachment_blob_not_found}} =
             @adapter.import_revision_chains(dest, %{chains: [chain]})

    assert {:ok, ^digest, ^length} = install_blob(dest_bundle, payload)

    assert {:ok, %{revisions_inserted: 1, documents_changed: 1}} =
             @adapter.import_revision_chains(dest, %{chains: [chain]})

    assert {:ok, %{revision: ^revision, attachments: imported}} =
             @adapter.get_document(dest, %{document_id: "attached"})

    assert Map.fetch!(imported, "file.bin").digest == digest
  end

  test "dangling parent chains are rejected atomically", %{adapter: adapter} do
    {:ok, root} = Id.calculate("doc", nil, false, %{"n" => 0})
    {:ok, mid} = Id.calculate("doc", root, false, %{"n" => "mid"})
    {:ok, leaf} = Id.calculate("doc", mid, false, %{"n" => "leaf"})

    assert {:ok, %{revisions_inserted: 1}} =
             @adapter.import_revision_chains(adapter, %{
               chains: [
                 %{
                   document_id: "doc",
                   leaf_revision: root,
                   revisions: [wire("doc", root, nil, false, %{"n" => 0})]
                 }
               ]
             })

    assert {:error, %ElixirDB.Error{code: :integrity_violation, message: message}} =
             @adapter.import_revision_chains(adapter, %{
               chains: [
                 %{
                   document_id: "doc",
                   leaf_revision: leaf,
                   revisions: [
                     wire("doc", mid, root, false, %{"n" => "mid"}),
                     wire("doc", leaf, mid, false, %{"n" => "leaf"})
                   ]
                 }
               ]
             })

    assert message =~ "dangling" or message =~ "parent"

    assert {:ok, %{revision: ^root, body: %{"n" => 0}}} =
             @adapter.get_document(adapter, %{document_id: "doc"})

    assert {:ok, %{current_sequence: 1}} = @adapter.identity(adapter)
  end

  test "identical imports are idempotent no-ops", %{adapter: adapter} do
    {:ok, root} = Id.calculate("doc", nil, false, %{"v" => 1})
    {:ok, leaf} = Id.calculate("doc", root, false, %{"v" => 2})

    chain = %{
      document_id: "doc",
      leaf_revision: leaf,
      revisions: [
        wire("doc", root, nil, false, %{"v" => 1}),
        wire("doc", leaf, root, false, %{"v" => 2})
      ]
    }

    assert {:ok, %{revisions_inserted: 2, documents_changed: 1, last_sequence: seq}} =
             @adapter.import_revision_chains(adapter, %{chains: [chain]})

    assert seq >= 1

    assert {:ok, %{revisions_inserted: 0, documents_changed: 0}} =
             @adapter.import_revision_chains(adapter, %{chains: [chain]})

    assert {:ok, %{revision: ^leaf, body: %{"v" => 2}}} =
             @adapter.get_document(adapter, %{document_id: "doc"})

    assert {:ok, %{current_sequence: ^seq}} = @adapter.identity(adapter)
  end

  test "different content under one revision id is rejected", %{adapter: adapter} do
    {:ok, root} = Id.calculate("doc", nil, false, %{"n" => 0})
    {:ok, leaf} = Id.calculate("doc", root, false, %{"n" => 1})

    assert {:ok, _} =
             @adapter.import_revision_chains(adapter, %{
               chains: [
                 %{
                   document_id: "doc",
                   leaf_revision: leaf,
                   revisions: [
                     wire("doc", root, nil, false, %{"n" => 0}),
                     wire("doc", leaf, root, false, %{"n" => 1})
                   ]
                 }
               ]
             })

    {:ok, [[doc_key]]} =
      Connection.query(
        adapter.conn,
        "SELECT doc_key FROM documents WHERE document_id = ?",
        ["doc"]
      )

    assert :ok =
             Connection.execute(
               adapter.conn,
               "UPDATE revisions SET body_json = ? WHERE doc_key = ? AND revision_id = ?",
               [~s({"n":99}), doc_key, leaf]
             )

    assert {:error, %ElixirDB.Error{code: :integrity_violation, message: message}} =
             @adapter.import_revision_chains(adapter, %{
               chains: [
                 %{
                   document_id: "doc",
                   leaf_revision: leaf,
                   revisions: [
                     wire("doc", root, nil, false, %{"n" => 0}),
                     wire("doc", leaf, root, false, %{"n" => 1})
                   ]
                 }
               ]
             })

    assert message =~ "differs" or message =~ "content"
  end

  defp install_blob(bundle_path, payload) when is_binary(bundle_path) and is_binary(payload) do
    assert {:ok, writer} = FilesystemStore.begin_put(bundle_path, byte_size(payload) + 1, %{})
    assert :ok = FilesystemStore.write_chunk(writer, payload)
    assert {:ok, %{digest: digest, logical_size: length}} = FilesystemStore.finish_put(writer)
    {:ok, digest, length}
  end
end
