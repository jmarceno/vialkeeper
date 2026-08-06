defmodule ElixirDB.StorageAdapter.FullTextIndexesTest do
  use ElixirDB.Storage.AdapterCase, adapter: ElixirDB.Storage.SQLite.Adapter

  alias ElixirDB.Storage.SQLite.Connection

  @fts_definition %{
    "name" => "titles",
    "type" => "full_text",
    "fields" => ["/title"],
    "tokenization" => %{"strategy" => "unicode_words_v1", "diacritics" => "preserve"}
  }

  test "create, delete, and rebuild full-text indexes", %{adapter: adapter} do
    assert {:ok, _} =
             @adapter.apply_local_mutation(adapter, %{
               operation: :put,
               document_id: "a",
               body: %{"title" => "alpha hello"}
             })

    assert {:ok, %{"index_id" => index_id, "name" => "titles", "type" => "full_text"}} =
             @adapter.create_index(adapter, @fts_definition)

    assert {:ok, %{results: [%{id: "a"}], selected_index: ^index_id}} =
             @adapter.execute_query(adapter, %{
               search: %{index: "titles", text: "hello", mode: "all"},
               limit: 10
             })

    assert {:ok, %{rebuilt: true}} = @adapter.rebuild_index(adapter, index_id)

    assert {:ok, %{results: [%{id: "a"}], selected_index: ^index_id}} =
             @adapter.execute_query(adapter, %{
               search: %{index: "titles", text: "alpha", mode: "all"},
               limit: 10
             })

    assert {:ok, %{index_id: ^index_id, deleted: true}} = @adapter.delete_index(adapter, index_id)
    assert {:ok, indexes} = @adapter.list_indexes(adapter)
    refute Enum.any?(indexes, &(&1["index_id"] == index_id))

    assert {:error, %ElixirDB.Error{code: code}} =
             @adapter.execute_query(adapter, %{
               search: %{index: "titles", text: "hello", mode: "all"},
               limit: 10
             })

    assert code in [:index_not_found, :invalid_index_hint]
  end

  test "mode any vs all with multi-token documents", %{adapter: adapter} do
    assert {:ok, _} =
             @adapter.apply_local_mutation(adapter, %{
               operation: :put,
               document_id: "both",
               body: %{"title" => "alpha beta"}
             })

    assert {:ok, _} =
             @adapter.apply_local_mutation(adapter, %{
               operation: :put,
               document_id: "one",
               body: %{"title" => "alpha only"}
             })

    assert {:ok, %{"index_id" => _index_id}} = @adapter.create_index(adapter, @fts_definition)

    assert {:ok, %{results: results_all}} =
             @adapter.execute_query(adapter, %{
               search: %{index: "titles", text: "alpha beta", mode: "all"},
               limit: 10
             })

    assert Enum.map(results_all, & &1.id) == ["both"]

    assert {:ok, %{results: results_any}} =
             @adapter.execute_query(adapter, %{
               search: %{index: "titles", text: "alpha beta", mode: "any"},
               limit: 10
             })

    ids = results_any |> Enum.map(& &1.id) |> Enum.sort()
    assert ids == ["both", "one"]
  end

  test "winner and tombstone refresh update full-text search results", %{adapter: adapter} do
    assert {:ok, %{revision: first}} =
             @adapter.apply_local_mutation(adapter, %{
               operation: :put,
               document_id: "doc",
               body: %{"title" => "ancient oak"}
             })

    assert {:ok, %{"index_id" => index_id}} = @adapter.create_index(adapter, @fts_definition)

    assert {:ok, %{results: [%{id: "doc"}]}} =
             @adapter.execute_query(adapter, %{
               search: %{index: "titles", text: "oak", mode: "all"},
               limit: 10
             })

    assert {:ok, %{revision: second}} =
             @adapter.apply_local_mutation(adapter, %{
               operation: :put,
               document_id: "doc",
               if_revision: first,
               body: %{"title" => "modern pine"}
             })

    assert {:ok, %{results: []}} =
             @adapter.execute_query(adapter, %{
               search: %{index: "titles", text: "oak", mode: "all"},
               limit: 10
             })

    assert {:ok, %{results: [%{id: "doc"}], selected_index: ^index_id}} =
             @adapter.execute_query(adapter, %{
               search: %{index: "titles", text: "pine", mode: "all"},
               limit: 10
             })

    assert {:ok, %{revision: _tombstone, deleted: true}} =
             @adapter.apply_local_mutation(adapter, %{
               operation: :delete,
               document_id: "doc",
               if_revision: second
             })

    assert {:ok, %{results: []}} =
             @adapter.execute_query(adapter, %{
               search: %{index: "titles", text: "pine", mode: "all"},
               limit: 10
             })
  end

  test "unicode_words_v1 matcher post-filters FTS5 over-matches", %{adapter: adapter} do
    assert {:ok, _} =
             @adapter.apply_local_mutation(adapter, %{
               operation: :put,
               document_id: "doc",
               body: %{"title" => "hello world"}
             })

    assert {:ok, %{"index_id" => _index_id}} = @adapter.create_index(adapter, @fts_definition)

    {:ok, indexes} = @adapter.list_indexes(adapter)
    full_text = Enum.find(indexes, &(&1["type"] == "full_text"))
    physical = full_text["_metadata"]["physical_name"]

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
end
