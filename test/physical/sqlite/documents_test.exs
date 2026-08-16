defmodule VialKeeper.StorageAdapter.DocumentsTest do
  @moduledoc "Covers SQLite document adapter persistence semantics."

  use ExUnit.Case, async: true

  @moduletag :sqlite_physical

  alias VialKeeper.Storage.SQLite.Adapter

  setup do
    {:ok, bundle_path} = VialKeeper.TempDatabase.create(prefix: "vialkeeper-test")

    on_exit(fn ->
      VialKeeper.TempDatabase.cleanup(bundle_path)
    end)

    path = VialKeeper.TempDatabase.sqlite_path(bundle_path)
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

    assert {:error, %VialKeeper.Error{code: :document_not_found}} =
             Adapter.get_document(adapter, %{document_id: "doc"})

    assert {:ok, %{deleted: true, revision: ^tombstone}} =
             Adapter.get_revision(adapter, %{document_id: "doc", revision_id: tombstone})

    assert {:ok, %{results: results}} = Adapter.read_changes(adapter, %{since: 0, limit: 10})
    assert [_, _, _] = results
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

    assert {:error, %VialKeeper.Error{code: :revision_conflict}} =
             Adapter.apply_local_mutation(adapter, %{
               operation: :put,
               document_id: "doc",
               if_revision: first,
               body: %{"x" => false}
             })
  end

  test "trusted canonical body bytes preserve revision and persistence semantics", %{
    adapter: adapter
  } do
    {:ok, other_bundle_path} = VialKeeper.TempDatabase.create(prefix: "vialkeeper-canonical")
    other_path = VialKeeper.TempDatabase.sqlite_path(other_bundle_path)
    {:ok, other_adapter} = Adapter.create(other_path, %{})

    on_exit(fn ->
      _ = Adapter.close(other_adapter)
      VialKeeper.TempDatabase.cleanup(other_bundle_path)
    end)

    body = %{"z" => [3, 2, 1], "a" => %{"value" => true}}
    body_json = VialKeeper.JSON.Canonical.encode!(body)
    history_id = VialKeeper.UUID.v4()

    request = %{
      operation: :put,
      document_id: "canonical-body",
      history_id: history_id,
      body: body,
      attachments: %{}
    }

    assert {:ok, %{revision: revision_without_cache}} =
             Adapter.apply_local_mutation(adapter, request)

    assert {:ok, %{revision: revision_with_cache}} =
             Adapter.apply_local_mutation(other_adapter, Map.put(request, :body_json, body_json))

    assert revision_with_cache == revision_without_cache
    assert {:ok, %{body: ^body}} = Adapter.get_document(adapter, %{document_id: "canonical-body"})

    assert {:ok, %{body: ^body}} =
             Adapter.get_document(other_adapter, %{document_id: "canonical-body"})
  end
end
