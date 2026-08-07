defmodule ElixirDB.StorageAdapter.V1ConformanceTest do
  use ElixirDB.Storage.AdapterCase, adapter: ElixirDB.Storage.SQLite.Adapter

  alias ElixirDB.Storage.SQLite.Connection
  alias ElixirDB.TestRevisionId, as: Id

  test "bulk writes are atomic and allocate one change per affected document", %{adapter: adapter} do
    assert {:ok, %{revision: first}} =
             @adapter.apply_local_mutation(adapter, %{
               operation: :put,
               document_id: "doc",
               body: %{"value" => 1}
             })

    assert {:error, %ElixirDB.Error{code: :revision_conflict}} =
             @adapter.apply_bulk_mutation(adapter, %{
               operations: [
                 %{operation: :put, document_id: "doc", if_revision: first, body: %{"value" => 2}},
                 %{operation: :put, document_id: "other", if_revision: "stale", body: %{}}
               ]
             })

    assert {:ok, %{body: %{"value" => 1}}} =
             @adapter.get_document(adapter, %{document_id: "doc"})

    assert {:error, %ElixirDB.Error{code: :document_not_found}} =
             @adapter.get_document(adapter, %{document_id: "other"})

    assert {:ok, %{current_sequence: 1}} = @adapter.identity(adapter)
  end

  test "imported sibling branches preserve conflicts and resolve atomically", %{adapter: adapter} do
    {:ok, root} = Id.calculate("doc", nil, false, %{"value" => 0})
    {:ok, left} = Id.calculate("doc", root, false, %{"value" => 1})
    {:ok, right} = Id.calculate("doc", root, false, %{"value" => 2})

    assert {:ok, %{revisions_inserted: 2}} =
             @adapter.import_revision_chains(adapter, %{
               chains: [
                 %{
                   document_id: "doc",
                   leaf_revision: left,
                   revisions: [
                     wire("doc", root, nil, false, %{"value" => 0}),
                     wire("doc", left, root, false, %{"value" => 1})
                   ]
                 }
               ]
             })

    assert {:ok, %{revisions_inserted: 1}} =
             @adapter.import_revision_chains(adapter, %{
               chains: [
                 %{
                   document_id: "doc",
                   leaf_revision: right,
                   revisions: [
                     wire("doc", root, nil, false, %{"value" => 0}),
                     wire("doc", right, root, false, %{"value" => 2})
                   ]
                 }
               ]
             })

    assert {:ok, %{conflicts: conflicts}} =
             @adapter.get_document(adapter, %{document_id: "doc", include_conflicts: true})

    assert [_] = conflicts
    assert hd(conflicts) in [left, right]

    assert {:error, %ElixirDB.Error{code: :revision_conflict}} =
             @adapter.resolve_conflict(adapter, %{
               document_id: "doc",
               expected_live_revisions: [left],
               chosen_parent_revision: left,
               body: %{"value" => 1}
             })

    assert {:ok, %{replayed: false}} =
             @adapter.resolve_conflict(adapter, %{
               document_id: "doc",
               expected_live_revisions: [left, right],
               chosen_parent_revision: left,
               body: %{"value" => 1}
             })
  end

  test "structured and full-text indexes are physical and integrity checked", %{adapter: adapter} do
    assert {:ok, _} =
             @adapter.apply_local_mutation(adapter, %{
               operation: :put,
               document_id: "a",
               body: %{"type" => "task", "title" => "Hello world"}
             })

    assert {:ok, _} =
             @adapter.apply_local_mutation(adapter, %{
               operation: :put,
               document_id: "b",
               body: %{"type" => "note", "title" => "Other"}
             })

    assert {:ok, %{"index_id" => structured_id}} =
             @adapter.create_index(adapter, %{
               "name" => "by-type",
               "type" => "structured",
               "fields" => [%{"path" => "/type", "type" => "string", "direction" => "asc"}]
             })

    assert {:ok, %{"index_id" => full_text_id}} =
             @adapter.create_index(adapter, %{
               "name" => "titles",
               "type" => "full_text",
               "fields" => ["/title"],
               "tokenization" => %{"strategy" => "unicode_words_v1", "diacritics" => "preserve"}
             })

    assert {:ok, %{results: [%{id: "a"}], selected_index: ^structured_id}} =
             @adapter.execute_query(adapter, %{
               selector: %{"/type" => "task"},
               index: "by-type",
               limit: 10
             })

    assert {:ok, %{results: [%{id: "a"}], selected_index: ^full_text_id}} =
             @adapter.execute_query(adapter, %{
               search: %{index: "titles", text: "hello", mode: "all"},
               limit: 10
             })

    assert {:ok, %{ok: true, indexes: 2}} = @adapter.integrity_check(adapter, %{})

    {:ok, indexes} = @adapter.list_indexes(adapter)
    full_text = Enum.find(indexes, &(&1["type"] == "full_text"))
    physical = full_text["_metadata"]["physical_name"]
    assert physical == "fts_" <> binary_part(String.trim_leading(full_text_id, "idx_"), 0, 24)
    assert full_text["_metadata"]["fts_table_kind"] == "contentless_delete"
    assert :ok = Connection.execute(adapter.conn, ~s(DELETE FROM "#{physical}"))

    assert {:error, %ElixirDB.Error{code: :integrity_violation}} =
             @adapter.integrity_check(adapter, %{})

    assert {:ok, %{rebuilt: true}} = @adapter.rebuild_index(adapter, full_text_id)
    assert {:ok, %{ok: true}} = @adapter.integrity_check(adapter, %{})
  end

  test "full-text search post-filters with unicode_words_v1 matcher (QUERY-015/017)", %{
    adapter: adapter
  } do
    assert {:ok, _} =
             @adapter.apply_local_mutation(adapter, %{
               operation: :put,
               document_id: "doc",
               body: %{"title" => "hello world"}
             })

    assert {:ok, %{"index_id" => _full_text_id}} =
             @adapter.create_index(adapter, %{
               "name" => "titles",
               "type" => "full_text",
               "fields" => ["/title"],
               "tokenization" => %{"strategy" => "unicode_words_v1", "diacritics" => "preserve"}
             })

    {:ok, indexes} = @adapter.list_indexes(adapter)
    full_text = Enum.find(indexes, &(&1["type"] == "full_text"))
    physical = full_text["_metadata"]["physical_name"]

    # Poison the FTS row so MATCH would over-match relative to the document body.
    {:ok, [[doc_key]]} =
      Connection.query(
        adapter.conn,
        "SELECT doc_key FROM documents WHERE document_id = ?",
        ["doc"]
      )

    assert :ok =
             Connection.execute(adapter.conn, ~s(DELETE FROM "#{physical}" WHERE rowid = ?), [
               doc_key
             ])

    assert :ok =
             Connection.execute(
               adapter.conn,
               "INSERT INTO \"#{physical}\"(rowid, content) VALUES (?, 'secret token')",
               [doc_key]
             )

    # FTS5 MATCH finds "secret"; project-owned matcher rejects because body has no such token.
    assert {:ok, %{results: []}} =
             @adapter.execute_query(adapter, %{
               search: %{index: "titles", text: "secret", mode: "all"},
               limit: 10
             })
  end

  test "closed databases reopen with identity, sequence, and configuration intact", %{
    adapter: adapter,
    path: path
  } do
    assert {:ok, %{revision: _}} =
             @adapter.apply_local_mutation(adapter, %{
               operation: :put,
               document_id: "portable",
               body: %{"ok" => true}
             })

    assert {:ok, identity} = @adapter.identity(adapter)
    assert :ok = @adapter.close(adapter)
    assert {:ok, reopened} = @adapter.open(path)
    assert {:ok, reopened_identity} = @adapter.identity(reopened)
    assert reopened_identity.database_uuid == identity.database_uuid
    assert reopened_identity.current_sequence == identity.current_sequence
    assert reopened_identity.config == identity.config

    assert {:ok, %{body: %{"ok" => true}}} =
             @adapter.get_document(reopened, %{document_id: "portable"})

    assert {:ok, %{ok: true}} = @adapter.integrity_check(reopened, %{})
    assert :ok = @adapter.close(reopened)
  end
end
