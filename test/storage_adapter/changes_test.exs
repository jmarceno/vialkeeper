for {name, adapter_module} <- [
      {"SQLite", ElixirDB.Storage.SQLite.Adapter},
      {"Memory", ElixirDB.Storage.Memory.Adapter}
    ] do
  defmodule Module.concat([ElixirDB.StorageAdapter, "#{name}ChangesTest"]) do
    use ElixirDB.Storage.AdapterCase, adapter: adapter_module

    alias ElixirDB.MapAccess
    alias ElixirDB.Storage.SQLite.{Connection, TermBlob}

    test "changes are ordered by sequence and advance last_sequence", %{adapter: adapter} do
      assert {:ok, %{revision: _a}} =
               @adapter.apply_local_mutation(adapter, %{
                 operation: :put,
                 document_id: "a",
                 body: %{"i" => 1}
               })

      assert {:ok, %{revision: _b}} =
               @adapter.apply_local_mutation(adapter, %{
                 operation: :put,
                 document_id: "b",
                 body: %{"i" => 2}
               })

      assert {:ok, %{revision: _c}} =
               @adapter.apply_local_mutation(adapter, %{
                 operation: :put,
                 document_id: "c",
                 body: %{"i" => 3}
               })

      assert {:ok, %{results: results, last_sequence: last, has_more: false}} =
               @adapter.read_changes(adapter, %{since: 0, limit: 10})

      assert Enum.map(results, & &1.sequence) == [1, 2, 3]
      assert Enum.map(results, & &1.document_id) == ["a", "b", "c"]
      assert last == 3

      assert {:ok, %{results: [%{sequence: 2, document_id: "b"}], last_sequence: 2}} =
               @adapter.read_changes(adapter, %{since: 1, limit: 1})

      assert {:ok, %{results: rest, has_more: false}} =
               @adapter.read_changes(adapter, %{since: 2, limit: 10})

      assert Enum.map(rest, & &1.document_id) == ["c"]
    end

    test "reject invalid since and oversized limit", %{adapter: adapter} do
      assert {:error, %ElixirDB.Error{code: :invalid_request}} =
               @adapter.read_changes(adapter, %{since: -1, limit: 10})

      assert {:error, %ElixirDB.Error{code: :resource_limit}} =
               @adapter.read_changes(adapter, %{since: 0, limit: 10_000})
    end

    test "updates to the same document produce ordered change rows with final leaves", %{
      adapter: adapter
    } do
      assert {:ok, %{revision: v1}} =
               @adapter.apply_local_mutation(adapter, %{
                 operation: :put,
                 document_id: "doc",
                 body: %{"n" => 1}
               })

      assert {:ok, %{revision: v2}} =
               @adapter.apply_local_mutation(adapter, %{
                 operation: :put,
                 document_id: "doc",
                 if_revision: v1,
                 body: %{"n" => 2}
               })

      assert {:ok, %{results: [_, _] = results, last_sequence: 2, has_more: false}} =
               @adapter.read_changes(adapter, %{since: 0, limit: 10})

      assert Enum.map(results, & &1.sequence) == [1, 2]
      assert Enum.map(results, & &1.document_id) == ["doc", "doc"]

      seq2 = Enum.at(results, 1)
      assert seq2.sequence == 2
      assert seq2.winning_revision == v2
      assert seq2.deleted == false

      assert [leaf] = seq2.leaf_revisions
      assert MapAccess.get(leaf, :revision) == v2
      assert MapAccess.get(leaf, :deleted) == false

      assert {:ok, %{results: [since_one], last_sequence: 2, has_more: false}} =
               @adapter.read_changes(adapter, %{since: 1, limit: 10})

      assert since_one.sequence == 2
      assert since_one.winning_revision == v2
    end

    test "changes since the current last_sequence are empty and do not duplicate prior rows", %{
      adapter: adapter
    } do
      assert {:ok, %{revision: _}} =
               @adapter.apply_local_mutation(adapter, %{
                 operation: :put,
                 document_id: "a",
                 body: %{"n" => 1}
               })

      assert {:ok, %{results: [], last_sequence: 1, has_more: false}} =
               @adapter.read_changes(adapter, %{since: 1, limit: 10})

      assert {:ok, %{revision: _}} =
               @adapter.apply_local_mutation(adapter, %{
                 operation: :put,
                 document_id: "b",
                 body: %{"n" => 2}
               })

      assert {:ok, %{results: [change], last_sequence: 2, has_more: false}} =
               @adapter.read_changes(adapter, %{since: 1, limit: 10})

      assert change.document_id == "b"
      assert change.sequence == 2
    end

    test "ordered changes and sequences survive close/reopen", %{adapter: adapter, path: path} do
      assert {:ok, %{revision: v1}} =
               @adapter.apply_local_mutation(adapter, %{
                 operation: :put,
                 document_id: "doc",
                 body: %{"n" => 1}
               })

      assert {:ok, %{revision: v2}} =
               @adapter.apply_local_mutation(adapter, %{
                 operation: :put,
                 document_id: "doc",
                 if_revision: v1,
                 body: %{"n" => 2}
               })

      assert {:ok, %{results: before, last_sequence: 2}} =
               @adapter.read_changes(adapter, %{since: 0, limit: 10})

      assert Enum.map(before, & &1.winning_revision) == [v1, v2]

      reopened = AdapterCaseModule.reopen!(@adapter, adapter, path)

      assert {:ok, %{current_sequence: 2}} = @adapter.identity(reopened)

      assert {:ok, %{results: after_reload, last_sequence: 2, has_more: false}} =
               @adapter.read_changes(reopened, %{since: 0, limit: 10})

      assert Enum.map(after_reload, &{&1.sequence, &1.winning_revision, &1.document_id}) ==
               Enum.map(before, &{&1.sequence, &1.winning_revision, &1.document_id})
    end

    test "deleted document change carries a tombstone leaf", %{adapter: adapter} do
      assert {:ok, %{revision: base}} =
               @adapter.apply_local_mutation(adapter, %{
                 operation: :put,
                 document_id: "doc",
                 body: %{"n" => 1}
               })

      assert {:ok, %{revision: tombstone}} =
               @adapter.apply_local_mutation(adapter, %{
                 operation: :delete,
                 document_id: "doc",
                 if_revision: base
               })

      assert {:ok, %{results: results}} = @adapter.read_changes(adapter, %{since: 0, limit: 10})

      deleted_change =
        results
        |> Enum.filter(&(&1.document_id == "doc"))
        |> List.last()

      assert deleted_change.sequence == 2
      assert deleted_change.deleted == true
      assert deleted_change.winning_revision == tombstone

      assert Enum.any?(deleted_change.leaf_revisions, fn leaf ->
               MapAccess.get(leaf, :revision) == tombstone and
                 MapAccess.get(leaf, :deleted) == true
             end)
    end

    if adapter_module == ElixirDB.Storage.SQLite.Adapter do
      test "malformed trusted leaf terms surface as integrity errors", %{adapter: adapter} do
        assert {:ok, _} =
                 @adapter.apply_local_mutation(adapter, %{
                   operation: :put,
                   document_id: "doc",
                   body: %{"n" => 1}
                 })

        assert :ok =
                 Connection.execute(
                   adapter.conn,
                   "UPDATE changes SET leaf_set_term = ? WHERE sequence = 1",
                   [TermBlob.bind(<<0, 1, 2>>)]
                 )

        assert {:error, %ElixirDB.Error{code: :integrity_violation}} =
                 @adapter.read_changes(adapter, %{since: 0, limit: 10})
      end
    end

    test "read below retention floor returns history_truncated", %{adapter: adapter} do
      assert {:ok, _} =
               @adapter.update_config(adapter, %{
                 "retention" => %{
                   "mode" => "stable_frontier",
                   "history_depth" => 0,
                   "peer_expiry_ms" => 86_400_000,
                   "schedule" => "disabled"
                 }
               })

      assert {:ok, %{revision: _}} =
               @adapter.apply_local_mutation(adapter, %{
                 operation: :put,
                 document_id: "doc",
                 body: %{"n" => 1}
               })

      assert {:ok, %{database_uuid: uuid}} = @adapter.identity(adapter)
      assert {:ok, _} = @adapter.compact_retention(adapter, %{})

      assert {:error, %ElixirDB.Error{code: :history_truncated, details: details}} =
               @adapter.read_changes(adapter, %{since: 0, limit: 10})

      assert details.database_uuid == uuid
      assert is_binary(details.history_epoch)
      assert details.retention_floor == 1
      assert details.compaction_epoch == 1
    end
  end
end
