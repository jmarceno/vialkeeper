defmodule VialKeeper.Storage.Contracts.Physical.Documents do
  @moduledoc """
  Shared document persistence tests for the SQLite-dialect storage engines.

  Injected into one test module per engine (`test/physical/sqlite/` and
  `test/physical/turso/`).
  """

  defmacro __using__(opts) do
    # quality:reason contract tests are injected via quote into each adapter module
    # credo:disable-for-next-line Credo.Check.Refactor.LongQuoteBlocks
    quote do
      use VialKeeper.Storage.AdapterCase, unquote(opts)

      alias VialKeeper.JSON.Canonical
      alias VialKeeper.Storage.AdapterCase
      alias VialKeeper.UUID

      test "put, update, delete, specific revision and changes", %{adapter: adapter} do
        assert {:ok, %{revision: first}} =
                 @adapter.apply_local_mutation(adapter, %{
                   operation: :put,
                   document_id: "doc",
                   body: %{"value" => 1}
                 })

        assert {:ok, %{revision: ^first, replayed: true}} =
                 @adapter.apply_local_mutation(adapter, %{
                   operation: :put,
                   document_id: "doc",
                   body: %{"value" => 1}
                 })

        assert {:ok, %{revision: second}} =
                 @adapter.apply_local_mutation(adapter, %{
                   operation: :put,
                   document_id: "doc",
                   if_revision: first,
                   body: %{"value" => 2}
                 })

        assert second != first

        assert {:ok, %{body: %{"value" => 2}}} =
                 @adapter.get_document(adapter, %{document_id: "doc"})

        assert {:ok, %{revision: ^second, replayed: true}} =
                 @adapter.apply_local_mutation(adapter, %{
                   operation: :put,
                   document_id: "doc",
                   if_revision: first,
                   body: %{"value" => 2}
                 })

        assert {:ok, %{body: %{"value" => 1}}} =
                 @adapter.get_revision(adapter, %{document_id: "doc", revision_id: first})

        assert {:ok, %{revision: tombstone, deleted: true}} =
                 @adapter.apply_local_mutation(adapter, %{
                   operation: :delete,
                   document_id: "doc",
                   if_revision: second
                 })

        assert {:error, %VialKeeper.Error{code: :document_not_found}} =
                 @adapter.get_document(adapter, %{document_id: "doc"})

        assert {:ok, %{deleted: true, revision: ^tombstone}} =
                 @adapter.get_revision(adapter, %{document_id: "doc", revision_id: tombstone})

        assert {:ok, %{results: results}} = @adapter.read_changes(adapter, %{since: 0, limit: 10})
        assert [_, _, _] = results
      end

      test "stale local writes are rejected", %{adapter: adapter} do
        assert {:ok, %{revision: first}} =
                 @adapter.apply_local_mutation(adapter, %{
                   operation: :put,
                   document_id: "doc",
                   body: %{}
                 })

        assert {:ok, _} =
                 @adapter.apply_local_mutation(adapter, %{
                   operation: :put,
                   document_id: "doc",
                   if_revision: first,
                   body: %{"x" => true}
                 })

        assert {:error, %VialKeeper.Error{code: :revision_conflict}} =
                 @adapter.apply_local_mutation(adapter, %{
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
        other_path = AdapterCase.adapter_path(@adapter, other_bundle_path)
        {:ok, other_adapter} = @adapter.create(other_path, %{})

        on_exit(fn ->
          _ = @adapter.close(other_adapter)
          VialKeeper.TempDatabase.cleanup(other_bundle_path)
        end)

        body = %{"z" => [3, 2, 1], "a" => %{"value" => true}}
        body_json = Canonical.encode!(body)
        history_id = UUID.v4()

        request = %{
          operation: :put,
          document_id: "canonical-body",
          history_id: history_id,
          body: body,
          attachments: %{}
        }

        assert {:ok, %{revision: revision_without_cache}} =
                 @adapter.apply_local_mutation(adapter, request)

        assert {:ok, %{revision: revision_with_cache}} =
                 @adapter.apply_local_mutation(
                   other_adapter,
                   Map.put(request, :body_json, body_json)
                 )

        assert revision_with_cache == revision_without_cache

        assert {:ok, %{body: ^body}} =
                 @adapter.get_document(adapter, %{document_id: "canonical-body"})

        assert {:ok, %{body: ^body}} =
                 @adapter.get_document(other_adapter, %{document_id: "canonical-body"})
      end
    end
  end
end
