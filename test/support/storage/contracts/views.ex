defmodule VialKeeper.Storage.Contracts.Views do
  @moduledoc """
  Shared local-view contract tests for storage adapters.
  """

  defmacro __using__(opts) do
    # The contract tests must be injected into each adapter module.
    # credo:disable-for-next-line Credo.Check.Refactor.LongQuoteBlocks
    quote do
      use VialKeeper.Storage.AdapterCase, unquote(opts)

      alias VialKeeper.Storage.AdapterCase

      @view %{
        "name" => "scores",
        "key" => [%{"path" => "/kind"}],
        "value" => %{"path" => "/score"},
        "reducer" => "_sum"
      }

      test "map-only views support range, bookmarks, and stale bookmarks", %{adapter: adapter} do
        assert {:ok, %{"view_id" => view_id}} =
                 @adapter.create_view(adapter, %{
                   "name" => "rows",
                   "key" => [%{"path" => "/k"}, %{"literal" => 1}],
                   "value" => %{"path" => "/v"}
                 })

        _ =
          Enum.reduce(
            [{"a", ["a", 1], 1}, {"b", ["b", 1], 2}, {"c", ["c", 1], 3}],
            0,
            fn {id, key, value}, cursor ->
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
            end
          )

        assert {:ok, %{results: [result], bookmark: bookmark}} =
                 @adapter.query_view(adapter, %{
                   "view_id" => view_id,
                   "start_key" => ["b", 1],
                   "limit" => 1
                 })

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

        assert {:error, %VialKeeper.Error{code: :bookmark_stale}} =
                 @adapter.query_view(adapter, %{
                   "view_id" => view_id,
                   "bookmark" => bookmark,
                   "limit" => 10
                 })
      end

      test "grouped reducers aggregate every row before pagination", %{adapter: adapter} do
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

      test "batch CAS, rebuild transitions, and reload persistence", %{
        adapter: adapter,
        path: path
      } do
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

        assert {:ok, %{results: [%{"key" => ["alpha"], "value" => 3.0}]}} =
                 @adapter.query_view(adapter, %{"view_id" => view_id, "limit" => 10})

        assert {:ok, _} =
                 @adapter.finish_view_rebuild(adapter, %{
                   "view_id" => view_id,
                   "generation" => 2,
                   "indexed_through" => 2
                 })

        assert {:ok, %{results: [%{"key" => ["beta"], "value" => 9.0}]}} =
                 @adapter.query_view(adapter, %{"view_id" => view_id, "limit" => 10})

        reopened = AdapterCase.reopen!(@adapter, adapter, path)

        assert {:ok, %{results: [%{"key" => ["beta"], "value" => 9.0}]}} =
                 @adapter.query_view(reopened, %{"view_id" => view_id, "limit" => 10})

        assert :ok = @adapter.close(reopened)
      end

      test "winning document pages omit deleted documents", %{adapter: adapter} do
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
  end
end
