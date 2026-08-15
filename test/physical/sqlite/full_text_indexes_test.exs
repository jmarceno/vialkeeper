defmodule VialKeeper.StorageAdapter.FullTextIndexesTest do
  alias VialKeeper.Query.Normalizer
  alias VialKeeper.Storage.SQLite.QueryRunner
  use VialKeeper.Storage.AdapterCase, adapter: VialKeeper.Storage.SQLite.Adapter

  @moduletag :sqlite_physical

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

    assert {:error, %VialKeeper.Error{code: code}} =
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

  test "prefix mode matches token prefixes, not middle substrings", %{adapter: adapter} do
    assert {:ok, _} =
             @adapter.apply_local_mutation(adapter, %{
               operation: :put,
               document_id: "replication",
               body: %{"title" => "replication checkpoint"}
             })

    assert {:ok, _} =
             @adapter.apply_local_mutation(adapter, %{
               operation: :put,
               document_id: "unrelated",
               body: %{"title" => "application checkpoint"}
             })

    assert {:ok, %{"index_id" => _index_id}} = @adapter.create_index(adapter, @fts_definition)

    assert {:ok, %{results: [%{id: "replication"}]}} =
             @adapter.execute_query(adapter, %{
               search: %{index: "titles", text: "checkp replic", mode: "prefix"},
               limit: 10
             })

    assert {:ok, %{results: []}} =
             @adapter.execute_query(adapter, %{
               search: %{index: "titles", text: "plica", mode: "prefix"},
               limit: 10
             })
  end

  test "prefix search quotes tokenized client text before FTS compilation", %{adapter: adapter} do
    assert {:ok, _} =
             @adapter.apply_local_mutation(adapter, %{
               operation: :put,
               document_id: "replication",
               body: %{"title" => "replication"}
             })

    assert {:ok, %{"index_id" => _index_id}} = @adapter.create_index(adapter, @fts_definition)

    # Without project tokenization and trusted quoting, `* OR` could turn this
    # into an FTS expression matching the otherwise unrelated document.
    assert {:ok, %{results: []}} =
             @adapter.execute_query(adapter, %{
               search: %{index: "titles", text: "replic* OR", mode: "prefix"},
               limit: 10
             })
  end

  test "prefix search remains the candidate source while structured predicates post-filter", %{
    adapter: adapter
  } do
    for {document_id, title, status} <- [
          {"open", "replication checkpoint", "open"},
          {"closed", "replication checkpoint", "closed"},
          {"other", "replication guide", "open"}
        ] do
      assert {:ok, _} =
               @adapter.apply_local_mutation(adapter, %{
                 operation: :put,
                 document_id: document_id,
                 body: %{"title" => title, "status" => status}
               })
    end

    assert {:ok, _} = @adapter.create_index(adapter, @fts_definition)

    assert {:ok, %{plan_kind: :full_text, results: [%{id: "open"}]}} =
             @adapter.execute_query(adapter, %{
               search: %{index: "titles", text: "replic checkp", mode: "prefix"},
               selector: %{"/status" => "open"},
               limit: 10
             })
  end

  test "full-text candidate processing enforces the query deadline", %{adapter: adapter} do
    for n <- 1..128 do
      assert {:ok, _} =
               @adapter.apply_local_mutation(adapter, %{
                 operation: :put,
                 document_id: "deadline-#{n}",
                 body: %{"title" => String.duplicate("replication checkpoint ", 256)}
               })
    end

    assert {:ok, %{"index_id" => _index_id}} = @adapter.create_index(adapter, @fts_definition)
    assert {:ok, _} = @adapter.update_config(adapter, %{"queries" => %{"max_execution_ms" => 1}})

    assert {:ok, request} =
             Normalizer.normalize(%{
               search: %{index: "titles", text: "replication", mode: "all"},
               limit: 128
             })

    assert {:error, %VialKeeper.Error{code: :resource_limit}} =
             QueryRunner.execute(adapter, request)
  end

  test "full-text pagination continues by rank before document id", %{adapter: adapter} do
    for {document_id, title} <- [
          {"a", "replication"},
          {"b", "replication replication"},
          {"c", "replication checkpoint"}
        ] do
      assert {:ok, _} =
               @adapter.apply_local_mutation(adapter, %{
                 operation: :put,
                 document_id: document_id,
                 body: %{"title" => title}
               })
    end

    assert {:ok, %{"index_id" => _index_id}} = @adapter.create_index(adapter, @fts_definition)

    request = %{search: %{index: "titles", text: "replic", mode: "prefix"}, limit: 1}

    assert {:ok, %{results: [%{id: first_id}], has_more: true, last_ordering_key: first_key}} =
             @adapter.execute_query(adapter, request)

    assert is_number(first_key["rank"])

    assert {:ok, %{results: remaining}} =
             @adapter.execute_query(
               adapter,
               request |> Map.put(:after_ordering, first_key) |> Map.put(:limit, 10)
             )

    remaining_ids = Enum.map(remaining, & &1.id)
    assert first_id not in remaining_ids
    assert MapSet.new([first_id | remaining_ids]) == MapSet.new(["a", "b", "c"])
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

  test "missing search cache rebuilds from winning documents", %{adapter: adapter} do
    assert {:ok, _} =
             @adapter.apply_local_mutation(adapter, %{
               operation: :put,
               document_id: "doc",
               body: %{"title" => "hello world"}
             })

    assert {:ok, %{"index_id" => _index_id}} = @adapter.create_index(adapter, @fts_definition)

    context = @adapter.to_context(adapter)
    assert :ok = VialKeeper.Search.stop(context)

    persist = Path.join(context.bundle_root, "tmp/search-index.etf")
    _ = File.rm(persist)

    assert {:ok, %{results: [%{id: "doc"}]}} =
             @adapter.execute_query(adapter, %{
               search: %{index: "titles", text: "hello", mode: "all"},
               limit: 10
             })

    assert {:ok, %{results: []}} =
             @adapter.execute_query(adapter, %{
               search: %{index: "titles", text: "secret", mode: "all"},
               limit: 10
             })
  end
end
