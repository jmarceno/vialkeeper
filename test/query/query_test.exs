defmodule ElixirDB.Query.QueryTest do
  alias ElixirDB.Storage.SQLite.Adapter
  use ExUnit.Case, async: false

  setup do
    {:ok, bundle_path} = ElixirDB.TempDatabase.create(prefix: "elixirdb-query")
    path = ElixirDB.TempDatabase.sqlite_path(bundle_path)
    {:ok, adapter} = Adapter.create(path, %{})

    on_exit(fn ->
      Adapter.close(adapter)
      ElixirDB.TempDatabase.cleanup(bundle_path)
    end)

    {:ok, adapter: adapter}
  end

  test "selector and pointer projection operate on materialized documents", %{adapter: adapter} do
    assert {:ok, _} =
             Adapter.apply_local_mutation(adapter, %{
               operation: :put,
               document_id: "a",
               body: %{"type" => "task", "priority" => 3, "title" => "A"}
             })

    assert {:ok, _} =
             Adapter.apply_local_mutation(adapter, %{
               operation: :put,
               document_id: "b",
               body: %{"type" => "note", "priority" => 1}
             })

    assert {:ok, %{results: [%{id: "a", fields: fields}]}} =
             Adapter.execute_query(adapter, %{
               selector: %{"/type" => "task"},
               fields: ["/title"],
               limit: 10
             })

    assert fields == %{"/title" => "A"}
  end

  test "full scan is permitted only below scan_threshold", %{adapter: adapter} do
    # QUERY-011: "only when the number of candidate documents is BELOW the configured scan
    # threshold." With scan_threshold = 1000, a database with exactly 1000 candidate docs
    # must require an index; 999 must still scan.
    threshold = 1_000

    for n <- 1..(threshold - 1) do
      assert {:ok, _} =
               Adapter.apply_local_mutation(adapter, %{
                 operation: :put,
                 document_id: :erlang.integer_to_binary(n),
                 body: %{"type" => "note", "priority" => n}
               })
    end

    # below the threshold: selector with no matching index still scans successfully.
    assert {:ok, %{results: results}} =
             Adapter.execute_query(adapter, %{
               selector: %{"/type" => "note"},
               limit: 5
             })

    assert [_, _, _, _, _] = results

    # seed exactly one more to reach exactly the threshold (1000 candidate docs).
    assert {:ok, _} =
             Adapter.apply_local_mutation(adapter, %{
               operation: :put,
               document_id: :erlang.integer_to_binary(threshold),
               body: %{"type" => "note", "priority" => threshold}
             })

    assert {:error, %ElixirDB.Error{code: :index_required}} =
             Adapter.execute_query(adapter, %{
               selector: %{"/type" => "note"},
               limit: 5
             })
  end
end
