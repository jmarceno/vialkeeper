defmodule ElixirDB.Storage.Contracts.Conflicts do
  @moduledoc """
  Shared conflict-resolution contract tests for storage adapters.
  """

  defmacro __using__(opts) do
    # The contract tests must be injected into each adapter module.
    # credo:disable-for-next-line Credo.Check.Refactor.LongQuoteBlocks
    quote do
      use ElixirDB.Storage.AdapterCase, unquote(opts)

      alias ElixirDB.Storage.AdapterCase
      alias ElixirDB.TestRevisionId, as: Id

      test "sibling imports surface conflicts and resolve with live leaf CAS", %{adapter: adapter} do
        {:ok, root} = Id.calculate("doc", nil, false, %{"n" => 0})
        {:ok, left} = Id.calculate("doc", root, false, %{"n" => 1})
        {:ok, right} = Id.calculate("doc", root, false, %{"n" => 2})

        assert {:ok, _} =
                 @adapter.import_revision_chains(adapter, %{
                   chains: [
                     %{
                       document_id: "doc",
                       leaf_revision: left,
                       revisions: [
                         wire("doc", root, nil, false, %{"n" => 0}),
                         wire("doc", left, root, false, %{"n" => 1})
                       ]
                     }
                   ]
                 })

        assert {:ok, _} =
                 @adapter.import_revision_chains(adapter, %{
                   chains: [
                     %{
                       document_id: "doc",
                       leaf_revision: right,
                       revisions: [
                         wire("doc", root, nil, false, %{"n" => 0}),
                         wire("doc", right, root, false, %{"n" => 2})
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
                   body: %{"n" => 3}
                 })

        assert {:ok, %{revision: resolved, replayed: false}} =
                 @adapter.resolve_conflict(adapter, %{
                   document_id: "doc",
                   expected_live_revisions: [left, right],
                   chosen_parent_revision: left,
                   body: %{"n" => 3}
                 })

        assert {:ok, %{revision: ^resolved, body: %{"n" => 3}, conflicts: []}} =
                 @adapter.get_document(adapter, %{document_id: "doc", include_conflicts: true})
      end

      test "conflict leaves and resolution survive close/reopen", %{
        adapter: adapter,
        path: path
      } do
        {:ok, root} = Id.calculate("doc", nil, false, %{"n" => 0})
        {:ok, left} = Id.calculate("doc", root, false, %{"n" => 1})
        {:ok, right} = Id.calculate("doc", root, false, %{"n" => 2})

        for leaf <- [left, right] do
          body = if leaf == left, do: %{"n" => 1}, else: %{"n" => 2}

          assert {:ok, _} =
                   @adapter.import_revision_chains(adapter, %{
                     chains: [
                       %{
                         document_id: "doc",
                         leaf_revision: leaf,
                         revisions: [
                           wire("doc", root, nil, false, %{"n" => 0}),
                           wire("doc", leaf, root, false, body)
                         ]
                       }
                     ]
                   })
        end

        assert {:ok, %{conflicts: conflicts}} =
                 @adapter.get_document(adapter, %{document_id: "doc", include_conflicts: true})

        assert [_] = conflicts

        reopened = AdapterCase.reopen!(@adapter, adapter, path)

        assert {:ok, %{conflicts: reopened_conflicts}} =
                 @adapter.get_document(reopened, %{document_id: "doc", include_conflicts: true})

        assert MapSet.new(reopened_conflicts) == MapSet.new(conflicts)

        assert {:ok, %{revision: resolved, replayed: false}} =
                 @adapter.resolve_conflict(reopened, %{
                   document_id: "doc",
                   expected_live_revisions: [left, right],
                   chosen_parent_revision: left,
                   body: %{"n" => 3}
                 })

        reopened2 = AdapterCase.reopen!(@adapter, reopened, path)

        assert {:ok, %{revision: ^resolved, body: %{"n" => 3}, conflicts: []}} =
                 @adapter.get_document(reopened2, %{document_id: "doc", include_conflicts: true})

        assert {:error, %ElixirDB.Error{code: :revision_conflict}} =
                 @adapter.resolve_conflict(reopened2, %{
                   document_id: "doc",
                   expected_live_revisions: [left, right],
                   chosen_parent_revision: left,
                   body: %{"n" => 9}
                 })
      end

      test "conflict resolution CAS rejects stale leaf sets after success", %{adapter: adapter} do
        {:ok, root} = Id.calculate("doc", nil, false, %{"n" => 0})
        {:ok, left} = Id.calculate("doc", root, false, %{"n" => 1})
        {:ok, right} = Id.calculate("doc", root, false, %{"n" => 2})

        for leaf <- [left, right] do
          body = if leaf == left, do: %{"n" => 1}, else: %{"n" => 2}

          assert {:ok, _} =
                   @adapter.import_revision_chains(adapter, %{
                     chains: [
                       %{
                         document_id: "doc",
                         leaf_revision: leaf,
                         revisions: [
                           wire("doc", root, nil, false, %{"n" => 0}),
                           wire("doc", leaf, root, false, body)
                         ]
                       }
                     ]
                   })
        end

        request = %{
          document_id: "doc",
          expected_live_revisions: [left, right],
          chosen_parent_revision: left,
          body: %{"merged" => true}
        }

        assert {:ok, %{revision: first, replayed: false}} =
                 @adapter.resolve_conflict(adapter, request)

        assert {:error, %ElixirDB.Error{code: :revision_conflict}} =
                 @adapter.resolve_conflict(adapter, request)

        assert {:ok, %{revision: ^first, body: %{"merged" => true}, conflicts: []}} =
                 @adapter.get_document(adapter, %{document_id: "doc", include_conflicts: true})
      end
    end
  end
end
