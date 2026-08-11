defmodule ElixirDB.Storage.SQLite.ViewsTest do
  @moduledoc "Behavioral tests for SQLite-backed declarative view storage."
  use ElixirDB.Storage.AdapterCase, adapter: ElixirDB.Storage.SQLite.Adapter

  @moduletag :sqlite_physical

  alias ElixirDB.Storage.PortFault

  @view %{
    "name" => "scores",
    "key" => [%{"path" => "/kind"}],
    "value" => %{"path" => "/score"},
    "reducer" => "_sum"
  }

  test "schema create/open includes view tables", %{adapter: adapter} do
    assert {:ok, _} = @adapter.integrity_check(adapter, %{})
  end

  test "view CRUD and name conflict", %{adapter: adapter} do
    assert {:ok, %{"view_id" => view_id, "name" => "scores"}} =
             @adapter.create_view(adapter, @view)

    assert {:ok, views} = @adapter.list_views(adapter)
    assert Enum.any?(views, &(&1["view_id"] == view_id))

    assert {:error, %ElixirDB.Error{code: :view_name_conflict}} =
             @adapter.create_view(adapter, Map.put(@view, "key", [%{"literal" => "other"}]))

    assert {:ok, %{deleted: true}} = @adapter.delete_view(adapter, view_id)
    assert {:error, %ElixirDB.Error{code: :view_not_found}} = @adapter.view_state(adapter, view_id)
  end

  test "query rejects missing identifiers and invalid options", %{adapter: adapter} do
    assert {:error, %ElixirDB.Error{code: :invalid_request}} =
             @adapter.query_view(adapter, %{})

    assert {:ok, %{"view_id" => view_id}} = @adapter.create_view(adapter, @view)

    assert {:error, %ElixirDB.Error{code: :invalid_request}} =
             @adapter.query_view(adapter, %{"view_id" => view_id, "limit" => 0})

    assert {:error, %ElixirDB.Error{code: :invalid_request}} =
             @adapter.query_view(adapter, %{"view_id" => view_id, "inclusive_end" => "false"})
  end

  test "create_view enforces max_definitions", %{adapter: adapter} do
    {:ok, identity} = @adapter.identity(adapter)
    config = put_in(identity.config, ["views", "max_definitions"], 1)
    assert {:ok, _} = @adapter.update_config(adapter, config)

    assert {:ok, _} = @adapter.create_view(adapter, @view)

    assert {:error, %ElixirDB.Error{code: :resource_limit}} =
             @adapter.create_view(adapter, Map.put(@view, "name", "other"))
  end

  test "apply batch rolls back indexed_through when row upsert fails", %{adapter: adapter} do
    assert {:ok, %{"view_id" => view_id}} = @adapter.create_view(adapter, @view)

    fault_adapter =
      PortFault.inject(
        adapter,
        :view_upsert_row,
        {:once, ElixirDB.Error.internal_error("injected view upsert fault")}
      )

    assert {:error, %ElixirDB.Error{code: :internal_error}} =
             PortFault.apply_view_batch(fault_adapter, %{
               "view_id" => view_id,
               "expected_indexed_through" => 0,
               "through_sequence" => 1,
               "rows" => [
                 %{
                   "document_id" => "a",
                   "revision_id" => "1-a",
                   "key" => ["alpha"],
                   "value" => 3
                 }
               ],
               "removals" => []
             })

    assert {:ok, %{indexed_through: 0}} = @adapter.view_state(adapter, view_id)
  end

  test "apply batch CAS and idempotency", %{adapter: adapter} do
    assert {:ok, %{"view_id" => view_id}} = @adapter.create_view(adapter, @view)

    batch = %{
      "view_id" => view_id,
      "expected_indexed_through" => 0,
      "through_sequence" => 5,
      "rows" => [
        %{
          "document_id" => "a",
          "revision_id" => "1-a",
          "key" => ["alpha"],
          "value" => 3
        }
      ],
      "removals" => []
    }

    assert {:ok, %{applied: true, indexed_through: 5}} =
             @adapter.apply_view_batch(adapter, batch)

    assert {:ok, %{applied: false, indexed_through: 5}} =
             @adapter.apply_view_batch(adapter, batch)

    assert {:error, %ElixirDB.Error{code: :revision_conflict}} =
             @adapter.apply_view_batch(adapter, %{
               batch
               | "expected_indexed_through" => 0,
                 "through_sequence" => 6
             })
  end

  test "generation rebuild switch hides old generation", %{adapter: adapter} do
    assert {:ok, %{"view_id" => view_id}} = @adapter.create_view(adapter, @view)

    assert {:ok, %{building_generation: 2}} =
             @adapter.begin_view_rebuild(adapter, %{
               "view_id" => view_id,
               "start_sequence" => 0
             })

    assert {:ok, _} =
             @adapter.append_view_rebuild_page(adapter, %{
               "view_id" => view_id,
               "generation" => 2,
               "rows" => [
                 %{
                   "document_id" => "a",
                   "revision_id" => "1-a",
                   "key" => ["beta"],
                   "value" => 9
                 }
               ]
             })

    assert {:ok, _} =
             @adapter.apply_view_batch(adapter, %{
               "view_id" => view_id,
               "expected_indexed_through" => 0,
               "through_sequence" => 1,
               "rows" => [
                 %{
                   "document_id" => "a",
                   "revision_id" => "1-a",
                   "key" => ["alpha"],
                   "value" => 1
                 }
               ],
               "removals" => []
             })

    assert {:ok, %{results: [%{"key" => ["alpha"], "value" => 1.0}]}} =
             @adapter.query_view(adapter, %{"view_id" => view_id, "limit" => 10})

    assert {:ok, %{active_generation: 2}} =
             @adapter.finish_view_rebuild(adapter, %{
               "view_id" => view_id,
               "generation" => 2,
               "indexed_through" => 2
             })

    assert {:ok, %{results: [%{"key" => ["beta"], "value" => 9.0}]}} =
             @adapter.query_view(adapter, %{"view_id" => view_id, "limit" => 10})
  end

  test "view range, group level, pagination, and stale bookmark", %{adapter: adapter} do
    assert {:ok, %{"view_id" => view_id}} =
             @adapter.create_view(adapter, %{
               "name" => "rows",
               "key" => [%{"path" => "/k"}, %{"literal" => 1}],
               "value" => %{"path" => "/v"}
             })

    rows = [
      {"a", ["a", 1], 1},
      {"b", ["b", 1], 2},
      {"c", ["c", 1], 3}
    ]

    Enum.reduce(rows, 0, fn {id, key, value}, cursor ->
      assert {:ok, _} =
               @adapter.apply_view_batch(adapter, %{
                 "view_id" => view_id,
                 "expected_indexed_through" => cursor,
                 "through_sequence" => cursor + 1,
                 "rows" => [
                   %{
                     "document_id" => id,
                     "revision_id" => "1-#{id}",
                     "key" => key,
                     "value" => value
                   }
                 ],
                 "removals" => []
               })

      cursor + 1
    end)

    assert {:ok, %{results: results, bookmark: bookmark}} =
             @adapter.query_view(adapter, %{
               "view_id" => view_id,
               "start_key" => ["b", 1],
               "limit" => 1
             })

    assert [result] = results
    assert result["key"] == ["b", 1]
    assert is_binary(bookmark)

    assert {:ok, %{results: [more]}} =
             @adapter.query_view(adapter, %{
               "view_id" => view_id,
               "bookmark" => bookmark,
               "limit" => 10
             })

    assert more["key"] == ["c", 1]

    assert {:ok, _} =
             @adapter.apply_view_batch(adapter, %{
               "view_id" => view_id,
               "expected_indexed_through" => 3,
               "through_sequence" => 4,
               "rows" => [],
               "removals" => []
             })

    assert {:error, %ElixirDB.Error{code: :bookmark_stale}} =
             @adapter.query_view(adapter, %{
               "view_id" => view_id,
               "bookmark" => bookmark,
               "limit" => 10
             })
  end

  test "view query supports an exact key bound", %{adapter: adapter} do
    assert {:ok, %{"view_id" => view_id}} =
             @adapter.create_view(adapter, %{
               "name" => "exact-key",
               "key" => [%{"path" => "/k"}],
               "value" => %{"path" => "/v"}
             })

    assert {:ok, _} =
             @adapter.apply_view_batch(adapter, %{
               "view_id" => view_id,
               "expected_indexed_through" => 0,
               "through_sequence" => 1,
               "rows" => [
                 %{
                   "document_id" => "a",
                   "revision_id" => "1-a",
                   "key" => ["match"],
                   "value" => 1
                 },
                 %{
                   "document_id" => "b",
                   "revision_id" => "1-b",
                   "key" => ["other"],
                   "value" => 2
                 }
               ],
               "removals" => []
             })

    assert {:ok, %{results: [%{"id" => "a", "key" => ["match"], "value" => 1}]}} =
             @adapter.query_view(adapter, %{"view_id" => view_id, "key" => ["match"]})

    assert {:error, %ElixirDB.Error{code: :invalid_request}} =
             @adapter.query_view(adapter, %{
               "view_id" => view_id,
               "key" => ["match"],
               "start_key" => ["match"]
             })

    assert {:error, %ElixirDB.Error{code: :invalid_request}} =
             @adapter.query_view(adapter, %{
               "view_id" => view_id,
               "key" => ["match"],
               "inclusive_end" => false
             })
  end

  test "reduced query with limit aggregates all rows in each group", %{adapter: adapter} do
    assert {:ok, %{"view_id" => view_id}} = @adapter.create_view(adapter, @view)

    Enum.each(1..20, fn index ->
      assert {:ok, _} =
               @adapter.apply_view_batch(adapter, %{
                 "view_id" => view_id,
                 "expected_indexed_through" => index - 1,
                 "through_sequence" => index,
                 "rows" => [
                   %{
                     "document_id" => "doc-#{index}",
                     "revision_id" => "1-#{index}",
                     "key" => ["shared"],
                     "value" => 1
                   }
                 ],
                 "removals" => []
               })
    end)

    assert {:ok, %{results: [%{"key" => ["shared"], "value" => 20.0}]}} =
             @adapter.query_view(adapter, %{"view_id" => view_id, "limit" => 1})
  end

  test "reduced query bookmarks advance past every row in a group", %{adapter: adapter} do
    assert {:ok, %{"view_id" => view_id}} = @adapter.create_view(adapter, @view)

    rows = [
      {"a1", "alpha", 1, 0, 1},
      {"a2", "alpha", 2, 1, 2},
      {"b1", "beta", 3, 2, 3}
    ]

    Enum.each(rows, fn {id, kind, score, expected, through} ->
      assert {:ok, _} =
               @adapter.apply_view_batch(adapter, %{
                 "view_id" => view_id,
                 "expected_indexed_through" => expected,
                 "through_sequence" => through,
                 "rows" => [
                   %{
                     "document_id" => id,
                     "revision_id" => "1-#{id}",
                     "key" => [kind],
                     "value" => score
                   }
                 ],
                 "removals" => []
               })
    end)

    assert {:ok, %{results: [%{"key" => ["alpha"], "value" => 3.0}], bookmark: bookmark}} =
             @adapter.query_view(adapter, %{"view_id" => view_id, "limit" => 1})

    assert {:ok, %{results: [%{"key" => ["beta"], "value" => 3.0}]}} =
             @adapter.query_view(adapter, %{
               "view_id" => view_id,
               "bookmark" => bookmark,
               "limit" => 1
             })
  end

  test "read_winning_documents_page returns only winning non-deleted documents", %{adapter: adapter} do
    assert {:ok, _} =
             @adapter.apply_local_mutation(adapter, %{
               operation: :put,
               document_id: "live",
               body: %{"k" => 1}
             })

    assert {:ok, %{revision: gone_revision}} =
             @adapter.apply_local_mutation(adapter, %{
               operation: :put,
               document_id: "gone",
               body: %{"k" => 2}
             })

    assert {:ok, _} =
             @adapter.apply_local_mutation(adapter, %{
               operation: :delete,
               document_id: "gone",
               if_revision: gone_revision
             })

    assert {:ok, %{documents: [%{"document_id" => "live"}], next_after: nil}} =
             @adapter.read_winning_documents_page(adapter, %{
               "after_document_id" => nil,
               "limit" => 10
             })
  end
end
