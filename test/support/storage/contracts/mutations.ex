defmodule VialKeeper.Storage.Contracts.Mutations do
  @moduledoc """
  Shared mutation and replay contract tests for storage adapters.
  """

  defmacro __using__(opts) do
    physical? = Keyword.get(opts, :physical, false)

    # The contract tests must be injected into each adapter module.
    # credo:disable-for-next-line Credo.Check.Refactor.LongQuoteBlocks
    quote do
      use VialKeeper.Storage.AdapterCase, unquote(opts)

      alias VialKeeper.Storage.AdapterCase

      test "bulk mutations are all-or-nothing", %{adapter: adapter} do
        assert {:ok, %{revision: first}} =
                 @adapter.apply_local_mutation(adapter, %{
                   operation: :put,
                   document_id: "doc",
                   body: %{"value" => 1}
                 })

        assert {:error, %VialKeeper.Error{code: :revision_conflict}} =
                 @adapter.apply_bulk_mutation(adapter, %{
                   operations: [
                     %{
                       operation: :put,
                       document_id: "doc",
                       if_revision: first,
                       body: %{"value" => 2}
                     },
                     %{
                       operation: :put,
                       document_id: "other",
                       if_revision: "1-missing",
                       body: %{"value" => 9}
                     }
                   ]
                 })

        assert {:ok, %{body: %{"value" => 1}, revision: ^first}} =
                 @adapter.get_document(adapter, %{document_id: "doc"})

        assert {:error, %VialKeeper.Error{code: :document_not_found}} =
                 @adapter.get_document(adapter, %{document_id: "other"})

        assert {:ok, %{current_sequence: 1}} = @adapter.identity(adapter)

        assert {:ok, results} =
                 @adapter.apply_bulk_mutation(adapter, %{
                   operations: [
                     %{
                       operation: :put,
                       document_id: "doc",
                       if_revision: first,
                       body: %{"value" => 2}
                     },
                     %{operation: :put, document_id: "other", body: %{"value" => 9}}
                   ]
                 })

        assert [_, _] = results
        assert Enum.all?(results, &(&1.replayed == false))

        assert {:ok, %{body: %{"value" => 2}}} =
                 @adapter.get_document(adapter, %{document_id: "doc"})

        assert {:ok, %{body: %{"value" => 9}}} =
                 @adapter.get_document(adapter, %{document_id: "other"})

        assert {:ok, %{current_sequence: 3}} = @adapter.identity(adapter)
      end

      test "duplicate document operations observe each prior mutation", %{adapter: adapter} do
        assert {:ok, %{revision: base}} =
                 @adapter.apply_local_mutation(adapter, %{
                   operation: :put,
                   document_id: "doc",
                   body: %{"value" => 0}
                 })

        assert {:error, %VialKeeper.Error{code: :revision_conflict}} =
                 @adapter.apply_bulk_mutation(adapter, %{
                   operations: [
                     %{
                       operation: :put,
                       document_id: "doc",
                       if_revision: base,
                       body: %{"value" => 1}
                     },
                     %{
                       operation: :put,
                       document_id: "doc",
                       if_revision: base,
                       body: %{"value" => 2}
                     }
                   ]
                 })

        assert {:ok, %{body: %{"value" => 0}, revision: ^base}} =
                 @adapter.get_document(adapter, %{document_id: "doc"})
      end

      unless unquote(physical?) do
        test "bulk-created documents remain queryable, indexed, and in changes", %{
          adapter: adapter
        } do
          assert {:ok, results} =
                   @adapter.apply_bulk_mutation(adapter, %{
                     operations: [
                       %{operation: :put, document_id: "task", body: %{"kind" => "task"}},
                       %{operation: :put, document_id: "note", body: %{"kind" => "note"}}
                     ]
                   })

          assert Enum.map(results, & &1.sequence) == [2, 1]

          assert {:ok, %{body: %{"kind" => "task"}}} =
                   @adapter.get_document(adapter, %{document_id: "task"})

          assert {:ok, %{results: [%{id: "task"}], plan_kind: :bounded_scan}} =
                   @adapter.execute_query(adapter, %{
                     selector: %{"/kind" => "task"},
                     limit: 10
                   })

          assert {:ok, %{results: [first], last_sequence: 1, has_more: true}} =
                   @adapter.read_changes(adapter, %{since: 0, limit: 1})

          assert first.document_id == "note"

          assert {:ok, %{results: [second], last_sequence: 2, has_more: false}} =
                   @adapter.read_changes(adapter, %{since: 1, limit: 1})

          assert second.document_id == "task"
        end
      end

      test "bulk mutations reject unknown operation types", %{adapter: adapter} do
        assert {:error, %VialKeeper.Error{code: :invalid_request}} =
                 @adapter.apply_bulk_mutation(adapter, %{
                   operations: [
                     %{operation: :upsert, document_id: "doc", body: %{"value" => 1}}
                   ]
                 })

        assert {:error, %VialKeeper.Error{code: :document_not_found}} =
                 @adapter.get_document(adapter, %{document_id: "doc"})
      end

      test "put replay returns the same revision without advancing sequence", %{adapter: adapter} do
        assert {:ok, %{revision: revision, sequence: sequence, replayed: false}} =
                 @adapter.apply_local_mutation(adapter, %{
                   operation: :put,
                   document_id: "doc",
                   body: %{"value" => 1}
                 })

        assert {:ok, %{revision: ^revision, sequence: ^sequence, replayed: true}} =
                 @adapter.apply_local_mutation(adapter, %{
                   operation: :put,
                   document_id: "doc",
                   body: %{"value" => 1}
                 })

        assert {:ok, %{revision: next, replayed: false}} =
                 @adapter.apply_local_mutation(adapter, %{
                   operation: :put,
                   document_id: "doc",
                   if_revision: revision,
                   body: %{"value" => 2}
                 })

        assert next != revision

        assert {:ok, %{revision: ^next, replayed: true}} =
                 @adapter.apply_local_mutation(adapter, %{
                   operation: :put,
                   document_id: "doc",
                   if_revision: revision,
                   body: %{"value" => 2}
                 })

        assert {:ok, %{current_sequence: 2}} = @adapter.identity(adapter)
      end

      test "replay, stale fencing, sequence, and causal markers survive close/reopen", %{
        adapter: adapter,
        path: path
      } do
        assert {:ok, %{revision: rev1, sequence: seq1, replayed: false}} =
                 @adapter.apply_local_mutation(adapter, %{
                   operation: :put,
                   document_id: "doc",
                   body: %{"value" => 1}
                 })

        assert {:ok, true} = @adapter.has_local_origin_changes?(adapter)

        assert {:ok, %{revision: rev2, sequence: seq2, replayed: false}} =
                 @adapter.apply_local_mutation(adapter, %{
                   operation: :put,
                   document_id: "doc",
                   if_revision: rev1,
                   body: %{"value" => 2}
                 })

        assert seq2 == seq1 + 1

        reopened = AdapterCase.reopen!(@adapter, adapter, path)

        assert {:ok, %{current_sequence: ^seq2}} = @adapter.identity(reopened)
        assert {:ok, true} = @adapter.has_local_origin_changes?(reopened)

        assert {:ok, %{revision: ^rev2, sequence: ^seq2, replayed: true}} =
                 @adapter.apply_local_mutation(reopened, %{
                   operation: :put,
                   document_id: "doc",
                   if_revision: rev1,
                   body: %{"value" => 2}
                 })

        assert {:ok, %{revision: rev3, replayed: false}} =
                 @adapter.apply_local_mutation(reopened, %{
                   operation: :put,
                   document_id: "doc",
                   if_revision: rev2,
                   body: %{"value" => 3}
                 })

        assert {:error,
                %VialKeeper.Error{
                  code: :revision_conflict,
                  details: %{operation_already_committed: true}
                }} =
                 @adapter.apply_local_mutation(reopened, %{
                   operation: :put,
                   document_id: "doc",
                   if_revision: rev1,
                   body: %{"value" => 2}
                 })

        assert {:ok, %{revision: ^rev3, body: %{"value" => 3}}} =
                 @adapter.get_document(reopened, %{document_id: "doc"})

        assert {:ok, :cleared} = @adapter.clear_pending_local_causal(reopened)
        assert {:ok, false} = @adapter.has_local_origin_changes?(reopened)

        reopened2 = AdapterCase.reopen!(@adapter, reopened, path)

        assert {:ok, false} = @adapter.has_local_origin_changes?(reopened2)

        assert {:ok, %{revision: ^rev3, body: %{"value" => 3}}} =
                 @adapter.get_document(reopened2, %{document_id: "doc"})
      end

      test "stale-parent retry of a superseded revision returns revision_conflict with operation_already_committed",
           %{adapter: adapter} do
        assert {:ok, %{revision: rev1}} =
                 @adapter.apply_local_mutation(adapter, %{
                   operation: :put,
                   document_id: "doc",
                   body: %{"value" => 1}
                 })

        assert {:ok, %{revision: rev2, replayed: false}} =
                 @adapter.apply_local_mutation(adapter, %{
                   operation: :put,
                   document_id: "doc",
                   if_revision: rev1,
                   body: %{"value" => 2}
                 })

        assert rev2 != rev1

        assert {:ok, %{revision: rev3, replayed: false}} =
                 @adapter.apply_local_mutation(adapter, %{
                   operation: :put,
                   document_id: "doc",
                   if_revision: rev2,
                   body: %{"value" => 3}
                 })

        assert rev3 != rev2

        assert {:error,
                %VialKeeper.Error{
                  code: :revision_conflict,
                  details: %{operation_already_committed: true}
                }} =
                 @adapter.apply_local_mutation(adapter, %{
                   operation: :put,
                   document_id: "doc",
                   if_revision: rev1,
                   body: %{"value" => 2}
                 })

        assert {:ok, %{body: %{"value" => 3}, revision: ^rev3}} =
                 @adapter.get_document(adapter, %{document_id: "doc"})
      end

      test "stale-parent retry that is still the winner replays successfully", %{adapter: adapter} do
        assert {:ok, %{revision: rev1, replayed: false}} =
                 @adapter.apply_local_mutation(adapter, %{
                   operation: :put,
                   document_id: "doc",
                   body: %{"value" => 1}
                 })

        assert {:ok, %{revision: ^rev1, replayed: true}} =
                 @adapter.apply_local_mutation(adapter, %{
                   operation: :put,
                   document_id: "doc",
                   body: %{"value" => 1}
                 })
      end

      test "bulk write with a stale-parent superseded retry fails the batch atomically",
           %{adapter: adapter} do
        assert {:ok, %{revision: rev1}} =
                 @adapter.apply_local_mutation(adapter, %{
                   operation: :put,
                   document_id: "doc",
                   body: %{"value" => 1}
                 })

        assert {:ok, %{revision: rev2}} =
                 @adapter.apply_local_mutation(adapter, %{
                   operation: :put,
                   document_id: "doc",
                   if_revision: rev1,
                   body: %{"value" => 2}
                 })

        assert {:ok, %{revision: rev3}} =
                 @adapter.apply_local_mutation(adapter, %{
                   operation: :put,
                   document_id: "doc",
                   if_revision: rev2,
                   body: %{"value" => 3}
                 })

        assert {:error,
                %VialKeeper.Error{
                  code: :revision_conflict,
                  details: %{operation_already_committed: true}
                }} =
                 @adapter.apply_bulk_mutation(adapter, %{
                   operations: [
                     %{
                       operation: :put,
                       document_id: "doc",
                       if_revision: rev1,
                       body: %{"value" => 2}
                     },
                     %{operation: :put, document_id: "fresh", body: %{"value" => 9}}
                   ]
                 })

        assert {:ok, %{body: %{"value" => 3}, revision: ^rev3}} =
                 @adapter.get_document(adapter, %{document_id: "doc"})

        assert {:error, %VialKeeper.Error{code: :document_not_found}} =
                 @adapter.get_document(adapter, %{document_id: "fresh"})
      end
    end
  end
end
