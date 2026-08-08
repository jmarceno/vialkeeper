defmodule ElixirDB.StorageAdapter.AttachmentsTest do
  use ElixirDB.Storage.AdapterCase, adapter: ElixirDB.Storage.SQLite.Adapter

  alias ElixirDB.Storage.SQLite.Connection
  alias ElixirDB.Storage.SQLite.Revisions

  @digest String.duplicate("a", 64)
  @other String.duplicate("b", 64)
  @third String.duplicate("c", 64)

  test "pending protection insert, renew, resolve, and remove", %{adapter: adapter} do
    assert {:ok, %{digest: @digest, logical_size: 12, expires_at: expires}} =
             @adapter.protect_pending_blob(adapter, %{digest: @digest, logical_size: 12})

    assert {:ok, _, _} = DateTime.from_iso8601(expires)

    assert {:ok, %{logical_size: 12}} =
             @adapter.resolve_blob_metadata(adapter, %{digest: @digest})

    assert {:ok, %{expires_at: renewed}} =
             @adapter.protect_pending_blob(adapter, %{blob: @digest, length: 12})

    assert renewed >= expires

    assert {:ok, %{removed: 1}} =
             @adapter.remove_pending_blob_protection(adapter, %{digest: @digest})

    assert {:error, %ElixirDB.Error{code: :attachment_blob_not_found}} =
             @adapter.resolve_blob_metadata(adapter, %{digest: @digest})
  end

  test "cleanup removes expired pending blobs only", %{adapter: adapter} do
    past = DateTime.utc_now() |> DateTime.add(-3_600, :second) |> DateTime.to_iso8601()
    future = DateTime.utc_now() |> DateTime.add(3_600, :second) |> DateTime.to_iso8601()
    now = DateTime.utc_now() |> DateTime.to_iso8601()

    assert :ok =
             Connection.execute(
               adapter.conn,
               "INSERT INTO pending_blobs(blob_digest, logical_size, expires_at, updated_at) VALUES (?, ?, ?, ?)",
               [@digest, 1, past, past]
             )

    assert :ok =
             Connection.execute(
               adapter.conn,
               "INSERT INTO pending_blobs(blob_digest, logical_size, expires_at, updated_at) VALUES (?, ?, ?, ?)",
               [@other, 2, future, now]
             )

    assert {:ok, %{removed: 1}} =
             @adapter.cleanup_expired_pending_blobs(adapter, %{now: now})

    assert {:error, %ElixirDB.Error{code: :attachment_blob_not_found}} =
             @adapter.resolve_blob_metadata(adapter, %{digest: @digest})

    assert {:ok, %{logical_size: 2}} =
             @adapter.resolve_blob_metadata(adapter, %{digest: @other})
  end

  test "list_live_attachment_digests unions retained and pending with paging", %{adapter: adapter} do
    assert {:ok, _} =
             @adapter.protect_pending_blob(adapter, %{digest: @digest, logical_size: 1})

    assert {:ok, _} =
             @adapter.protect_pending_blob(adapter, %{digest: @other, logical_size: 2})

    assert {:ok, _} =
             @adapter.protect_pending_blob(adapter, %{digest: @third, logical_size: 3})

    assert {:ok, %{revision: rev}} =
             @adapter.apply_local_mutation(adapter, %{
               operation: :put,
               document_id: "doc",
               body: %{"n" => 1},
               attachments: %{
                 "a.bin" => %{
                   digest: @digest,
                   length: 1,
                   content_type: "application/octet-stream"
                 }
               }
             })

    assert is_binary(rev)

    assert {:ok, %{digests: first_page, next_after_digest: cursor}} =
             @adapter.list_live_attachment_digests(adapter, %{limit: 2})

    assert match?([_, _], first_page)
    assert cursor == List.last(first_page)

    assert {:ok, %{digests: rest, next_after_digest: nil}} =
             @adapter.list_live_attachment_digests(adapter, %{
               after_digest: cursor,
               limit: 2
             })

    assert rest == [@third]
    assert Enum.sort([@digest, @other, @third]) == Enum.sort(first_page ++ rest)
  end

  test "mutation inherits, clears, replaces, and rejects missing blob metadata", %{
    adapter: adapter
  } do
    assert {:ok, _} =
             @adapter.protect_pending_blob(adapter, %{digest: @digest, logical_size: 10})

    assert {:ok, _} =
             @adapter.protect_pending_blob(adapter, %{digest: @other, logical_size: 20})

    assert {:ok, %{revision: first}} =
             @adapter.apply_local_mutation(adapter, %{
               operation: :put,
               document_id: "doc",
               body: %{"v" => 1},
               attachments: %{
                 "diagram.svg" => %{
                   "digest" => @digest,
                   "length" => 10,
                   "content_type" => "image/svg+xml"
                 }
               }
             })

    assert {:ok, %{attachments: attachments}} =
             load_revision(adapter, "doc", first)

    assert attachments["diagram.svg"].digest == @digest

    # Pending row is cleared once the digest is retained by a revision; metadata
    # remains resolvable through revision_attachments.
    assert {:ok, [[0]]} =
             Connection.query(
               adapter.conn,
               "SELECT COUNT(*) FROM pending_blobs WHERE blob_digest = ?",
               [@digest]
             )

    assert {:ok, %{logical_size: 10}} =
             @adapter.resolve_blob_metadata(adapter, %{digest: @digest})

    assert {:ok, %{revision: second}} =
             @adapter.apply_local_mutation(adapter, %{
               operation: :put,
               document_id: "doc",
               if_revision: first,
               body: %{"v" => 2}
             })

    assert {:ok, %{attachments: inherited}} = load_revision(adapter, "doc", second)
    assert inherited == attachments

    assert {:ok, %{revision: cleared}} =
             @adapter.apply_local_mutation(adapter, %{
               operation: :put,
               document_id: "doc",
               if_revision: second,
               body: %{"v" => 3},
               attachments: %{}
             })

    assert {:ok, %{attachments: %{}}} = load_revision(adapter, "doc", cleared)

    assert {:error, %ElixirDB.Error{code: :attachment_blob_not_found}} =
             @adapter.apply_local_mutation(adapter, %{
               operation: :put,
               document_id: "doc",
               if_revision: cleared,
               body: %{"v" => 4},
               attachments: %{
                 "missing.bin" => %{
                   digest: String.duplicate("f", 64),
                   length: 1,
                   content_type: "application/octet-stream"
                 }
               }
             })

    assert {:ok, %{revision: replaced}} =
             @adapter.apply_local_mutation(adapter, %{
               operation: :put,
               document_id: "doc",
               if_revision: cleared,
               body: %{"v" => 5},
               attachments: %{
                 "other.bin" => %{
                   blob: @other,
                   length: 20,
                   content_type: "application/octet-stream"
                 }
               }
             })

    assert {:ok, %{attachments: %{"other.bin" => entry}}} =
             load_revision(adapter, "doc", replaced)

    assert entry.digest == @other
    assert entry.length == 20
  end

  test "ticket resolves winner or specific revision attachment", %{adapter: adapter} do
    assert {:ok, _} =
             @adapter.protect_pending_blob(adapter, %{digest: @digest, logical_size: 4})

    assert {:ok, %{revision: first}} =
             @adapter.apply_local_mutation(adapter, %{
               operation: :put,
               document_id: "doc",
               body: %{},
               attachments: %{
                 "file.txt" => %{
                   digest: @digest,
                   length: 4,
                   content_type: "text/plain"
                 }
               }
             })

    assert {:ok, ticket} =
             @adapter.resolve_attachment_ticket(adapter, %{
               id: "doc",
               revision: nil,
               name: "file.txt"
             })

    assert ticket.blob_digest == @digest
    assert ticket.revision_id == first
    assert ticket.logical_size == 4
    assert ticket.content_type == "text/plain"
    assert ticket.document_id == "doc"
    assert ticket.attachment_name == "file.txt"
    assert ticket.database_uuid == adapter.identity.database_uuid
    assert ticket.bundle_path == Path.dirname(adapter.path)

    assert {:ok, ^ticket} =
             @adapter.resolve_attachment_ticket(adapter, %{
               document_id: "doc",
               revision_id: first,
               attachment_name: "file.txt"
             })

    assert {:error, %ElixirDB.Error{code: :attachment_not_found}} =
             @adapter.resolve_attachment_ticket(adapter, %{
               id: "doc",
               revision: first,
               name: "missing"
             })
  end

  test "tombstone persists empty attachment manifest", %{adapter: adapter} do
    assert {:ok, _} =
             @adapter.protect_pending_blob(adapter, %{digest: @digest, logical_size: 1})

    assert {:ok, %{revision: first}} =
             @adapter.apply_local_mutation(adapter, %{
               operation: :put,
               document_id: "doc",
               body: %{},
               attachments: %{
                 "a" => %{digest: @digest, length: 1, content_type: "text/plain"}
               }
             })

    assert {:ok, %{revision: tombstone}} =
             @adapter.apply_local_mutation(adapter, %{
               operation: :delete,
               document_id: "doc",
               if_revision: first
             })

    assert {:ok, %{deleted: true, attachments: %{}}} =
             load_revision(adapter, "doc", tombstone)
  end

  test "revision_attachments rows cascade when revision is deleted", %{adapter: adapter} do
    assert {:ok, _} =
             @adapter.protect_pending_blob(adapter, %{digest: @digest, logical_size: 1})

    assert {:ok, %{revision: first}} =
             @adapter.apply_local_mutation(adapter, %{
               operation: :put,
               document_id: "doc",
               body: %{},
               attachments: %{
                 "a" => %{digest: @digest, length: 1, content_type: "text/plain"}
               }
             })

    assert {:ok, [[1]]} =
             Connection.query(
               adapter.conn,
               "SELECT COUNT(*) FROM revision_attachments WHERE revision_id = ?",
               [first]
             )

    assert {:ok, [[doc_key]]} =
             Connection.query(adapter.conn, "SELECT doc_key FROM documents WHERE document_id = ?", [
               "doc"
             ])

    assert :ok =
             Connection.execute(adapter.conn, "DELETE FROM revisions WHERE doc_key = ?", [doc_key])

    assert {:ok, [[0]]} =
             Connection.query(
               adapter.conn,
               "SELECT COUNT(*) FROM revision_attachments WHERE revision_id = ?",
               [first]
             )
  end

  defp load_revision(adapter, document_id, revision_id) do
    case Connection.query(adapter.conn, "SELECT doc_key FROM documents WHERE document_id = ?", [
           document_id
         ]) do
      {:ok, [[doc_key]]} -> Revisions.find(adapter.conn, doc_key, revision_id)
      {:ok, []} -> {:error, ElixirDB.Error.document_not_found("document not found")}
      {:error, reason} -> {:error, reason}
    end
  end
end
