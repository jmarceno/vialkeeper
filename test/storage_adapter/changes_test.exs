defmodule ElixirDB.StorageAdapter.ChangesTest do
  use ElixirDB.Storage.AdapterCase, adapter: ElixirDB.Storage.SQLite.Adapter

  alias ElixirDB.MapAccess

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
end
