defmodule ElixirDB.StorageAdapter.StructuredIndexesTest do
  use ElixirDB.Storage.AdapterCase, adapter: ElixirDB.Storage.SQLite.Adapter

  test "structured index creation, query selection, and delete", %{adapter: adapter} do
    assert {:ok, _} =
             @adapter.apply_local_mutation(adapter, %{
               operation: :put,
               document_id: "t1",
               body: %{"type" => "task", "priority" => 1}
             })

    assert {:ok, _} =
             @adapter.apply_local_mutation(adapter, %{
               operation: :put,
               document_id: "n1",
               body: %{"type" => "note", "priority" => 9}
             })

    assert {:ok, %{"index_id" => index_id, "name" => "by-type"}} =
             @adapter.create_index(adapter, %{
               "name" => "by-type",
               "type" => "structured",
               "fields" => [%{"path" => "/type", "type" => "string", "direction" => "asc"}]
             })

    assert {:ok, %{results: [%{id: "t1"}], selected_index: ^index_id}} =
             @adapter.execute_query(adapter, %{
               selector: %{"/type" => "task"},
               index: "by-type",
               limit: 10
             })

    assert {:ok, indexes} = @adapter.list_indexes(adapter)
    assert Enum.any?(indexes, &(&1["index_id"] == index_id))

    assert {:ok, _} = @adapter.delete_index(adapter, index_id)
    assert {:ok, remaining} = @adapter.list_indexes(adapter)
    refute Enum.any?(remaining, &(&1["index_id"] == index_id))
  end

  test "duplicate structured index name with same definition replays", %{adapter: adapter} do
    definition = %{
      "name" => "by-kind",
      "type" => "structured",
      "fields" => [%{"path" => "/kind", "type" => "string", "direction" => "asc"}]
    }

    assert {:ok, first} = @adapter.create_index(adapter, definition)
    assert {:ok, second} = @adapter.create_index(adapter, definition)
    assert first["index_id"] == second["index_id"]
  end
end
