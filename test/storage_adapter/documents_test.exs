defmodule ElixirDB.StorageAdapter.DocumentsTest do
  use ExUnit.Case, async: false

  alias ElixirDB.Storage.SQLite.Adapter

  setup do
    path = Path.join(System.tmp_dir!(), "elixirdb-test-#{System.unique_integer([:positive])}.db")

    on_exit(fn ->
      File.rm(path)
      File.rm(path <> ".lease")
    end)

    {:ok, adapter} = Adapter.create(path, %{})
    on_exit(fn -> Adapter.close(adapter) end)
    {:ok, adapter: adapter}
  end

  test "put, update, delete, specific revision and changes", %{adapter: adapter} do
    assert {:ok, %{revision: first}} =
             Adapter.apply_local_mutation(adapter, %{
               operation: :put,
               document_id: "doc",
               body: %{"value" => 1}
             })

    assert {:ok, %{revision: ^first, replayed: true}} =
             Adapter.apply_local_mutation(adapter, %{
               operation: :put,
               document_id: "doc",
               body: %{"value" => 1}
             })

    assert {:ok, %{revision: second}} =
             Adapter.apply_local_mutation(adapter, %{
               operation: :put,
               document_id: "doc",
               if_revision: first,
               body: %{"value" => 2}
             })

    assert second != first
    assert {:ok, %{body: %{"value" => 2}}} = Adapter.get_document(adapter, %{document_id: "doc"})

    assert {:ok, %{revision: ^second, replayed: true}} =
             Adapter.apply_local_mutation(adapter, %{
               operation: :put,
               document_id: "doc",
               if_revision: first,
               body: %{"value" => 2}
             })

    assert {:ok, %{body: %{"value" => 1}}} =
             Adapter.get_revision(adapter, %{document_id: "doc", revision_id: first})

    assert {:ok, %{revision: tombstone, deleted: true}} =
             Adapter.apply_local_mutation(adapter, %{
               operation: :delete,
               document_id: "doc",
               if_revision: second
             })

    assert {:error, %ElixirDB.Error{code: :document_not_found}} =
             Adapter.get_document(adapter, %{document_id: "doc"})

    assert {:ok, %{deleted: true, revision: ^tombstone}} =
             Adapter.get_revision(adapter, %{document_id: "doc", revision_id: tombstone})

    assert {:ok, %{results: results}} = Adapter.read_changes(adapter, %{since: 0, limit: 10})
    assert length(results) == 3
  end

  test "stale local writes are rejected", %{adapter: adapter} do
    assert {:ok, %{revision: first}} =
             Adapter.apply_local_mutation(adapter, %{operation: :put, document_id: "doc", body: %{}})

    assert {:ok, _} =
             Adapter.apply_local_mutation(adapter, %{
               operation: :put,
               document_id: "doc",
               if_revision: first,
               body: %{"x" => true}
             })

    assert {:error, %ElixirDB.Error{code: :revision_conflict}} =
             Adapter.apply_local_mutation(adapter, %{
               operation: :put,
               document_id: "doc",
               if_revision: first,
               body: %{"x" => false}
             })
  end
end
