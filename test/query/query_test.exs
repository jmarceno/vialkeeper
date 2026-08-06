defmodule ElixirDB.Query.QueryTest do
  use ExUnit.Case, async: false

  setup do
    path = Path.join(System.tmp_dir!(), "elixirdb-query-#{System.unique_integer([:positive])}.db")
    {:ok, adapter} = ElixirDB.Storage.SQLite.Adapter.create(path, %{})

    on_exit(fn ->
      ElixirDB.Storage.SQLite.Adapter.close(adapter)
      File.rm(path)
    end)

    {:ok, adapter: adapter}
  end

  test "selector and pointer projection operate on materialized documents", %{adapter: adapter} do
    assert {:ok, _} =
             ElixirDB.Storage.SQLite.Adapter.apply_local_mutation(adapter, %{
               operation: :put,
               document_id: "a",
               body: %{"type" => "task", "priority" => 3, "title" => "A"}
             })

    assert {:ok, _} =
             ElixirDB.Storage.SQLite.Adapter.apply_local_mutation(adapter, %{
               operation: :put,
               document_id: "b",
               body: %{"type" => "note", "priority" => 1}
             })

    assert {:ok, %{results: [%{id: "a", fields: fields}]}} =
             ElixirDB.Storage.SQLite.Adapter.execute_query(adapter, %{
               selector: %{"/type" => "task"},
               fields: ["/title"],
               limit: 10
             })

    assert fields == %{"/title" => "A"}
  end
end
