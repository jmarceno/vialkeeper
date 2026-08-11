for {name, adapter_module} <- [
      {"SQLite", ElixirDB.Storage.SQLite.Adapter},
      {"Memory", ElixirDB.Storage.Memory.Adapter}
    ] do
  defmodule Module.concat([ElixirDB.StorageAdapter, "#{name}ConflictsTest"]) do
    use ElixirDB.Storage.AdapterCase, adapter: adapter_module

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

      # Live leaf set CAS: the original expected set is stale after resolution.
      assert {:error, %ElixirDB.Error{code: :revision_conflict}} =
               @adapter.resolve_conflict(adapter, request)

      assert {:ok, %{revision: ^first, body: %{"merged" => true}, conflicts: []}} =
               @adapter.get_document(adapter, %{document_id: "doc", include_conflicts: true})
    end
  end
end
