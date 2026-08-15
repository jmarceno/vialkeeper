defmodule VialKeeper.StorageAdapter.RevisionsBatchTest do
  use VialKeeper.Storage.AdapterCase, adapter: VialKeeper.Storage.SQLite.Adapter

  @moduletag :sqlite_physical

  test "rejects oversized revision batches", %{adapter: adapter} do
    requests =
      for index <- 1..501 do
        %{document_id: "doc-#{index}", revision_id: "rev-#{index}"}
      end

    assert {:error, %VialKeeper.Error{code: :resource_limit}} =
             @adapter.get_revisions_batch(adapter, requests)
  end

  test "rejects invalid revision batch items before lookup", %{adapter: adapter} do
    assert {:error, %VialKeeper.Error{code: :invalid_request}} =
             @adapter.get_revisions_batch(adapter, [%{document_id: "doc"}])
  end

  test "empty revision batch returns empty list", %{adapter: adapter} do
    assert {:ok, []} = @adapter.get_revisions_batch(adapter, [])
  end

  test "returns revision envelopes in request order", %{adapter: adapter} do
    assert {:ok, %{revision: rev_a}} =
             @adapter.apply_local_mutation(adapter, %{
               operation: :put,
               document_id: "a",
               body: %{"value" => 1}
             })

    assert {:ok, %{revision: rev_b}} =
             @adapter.apply_local_mutation(adapter, %{
               operation: :put,
               document_id: "b",
               body: %{"value" => 2}
             })

    assert {:ok, [second, first]} =
             @adapter.get_revisions_batch(adapter, [
               %{document_id: "b", revision_id: rev_b},
               %{document_id: "a", revision_id: rev_a}
             ])

    assert second.id == "b"
    assert second.revision == rev_b
    assert second.body == %{"value" => 2}
    assert first.id == "a"
    assert first.revision == rev_a
    assert first.body == %{"value" => 1}
  end

  test "deleted revisions return usable deleted envelopes", %{adapter: adapter} do
    assert {:ok, %{revision: revision}} =
             @adapter.apply_local_mutation(adapter, %{
               operation: :put,
               document_id: "doc",
               body: %{"value" => 1}
             })

    assert {:ok, %{revision: tombstone}} =
             @adapter.apply_local_mutation(adapter, %{
               operation: :delete,
               document_id: "doc",
               if_revision: revision
             })

    assert {:ok, [envelope]} =
             @adapter.get_revisions_batch(adapter, [
               %{document_id: "doc", revision_id: tombstone}
             ])

    assert envelope.deleted == true
    assert envelope.id == "doc"
    assert envelope.revision == tombstone
  end

  test "missing revision returns integrity_violation", %{adapter: adapter} do
    assert {:ok, %{revision: revision}} =
             @adapter.apply_local_mutation(adapter, %{
               operation: :put,
               document_id: "doc",
               body: %{"value" => 1}
             })

    assert {:error, %VialKeeper.Error{code: :integrity_violation}} =
             @adapter.get_revisions_batch(adapter, [
               %{document_id: "doc", revision_id: "missing-revision"}
             ])

    assert {:error, %VialKeeper.Error{code: :integrity_violation}} =
             @adapter.get_revisions_batch(adapter, [
               %{document_id: "missing-doc", revision_id: revision}
             ])
  end

  test "does not expose adapter-private values", %{adapter: adapter} do
    assert {:ok, %{revision: revision}} =
             @adapter.apply_local_mutation(adapter, %{
               operation: :put,
               document_id: "doc",
               body: %{"value" => 1}
             })

    assert {:ok, [envelope]} =
             @adapter.get_revisions_batch(adapter, [
               %{document_id: "doc", revision_id: revision}
             ])

    assert Map.keys(envelope) |> Enum.sort() == [:body, :deleted, :id, :revision]
  end
end
