defmodule ElixirDB.StorageAdapter.ChangesTest do
  use ElixirDB.Storage.AdapterCase, adapter: ElixirDB.Storage.SQLite.Adapter

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
end
