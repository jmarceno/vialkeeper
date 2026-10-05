defmodule VialKeeper.Storage.Contracts.Physical.Lifecycle do
  @moduledoc """
  Shared lifecycle tests for the SQLite-dialect storage engines.

  Injected into one test module per engine (`test/physical/sqlite/` and
  `test/physical/turso/`).
  """

  defmacro __using__(opts) do
    # quality:reason contract tests are injected via quote into each adapter module
    # credo:disable-for-next-line Credo.Check.Refactor.LongQuoteBlocks
    quote do
      use VialKeeper.Storage.AdapterCase, unquote(opts)

      test "create, close, reopen preserves identity and documents", %{adapter: adapter, path: path} do
        assert {:ok, %{revision: revision}} =
                 @adapter.apply_local_mutation(adapter, %{
                   operation: :put,
                   document_id: "lifecycle",
                   body: %{"phase" => "create"}
                 })

        assert {:ok, identity} = @adapter.identity(adapter)
        assert identity.current_sequence == 1
        assert :ok = @adapter.close(adapter)

        assert {:ok, reopened} = @adapter.open(path)

        assert {:ok, %{revision: ^revision, body: %{"phase" => "create"}}} =
                 @adapter.get_document(reopened, %{document_id: "lifecycle"})

        assert {:ok, reopened_identity} = @adapter.identity(reopened)
        assert reopened_identity.database_uuid == identity.database_uuid
        assert reopened_identity.current_sequence == identity.current_sequence
        assert :ok = @adapter.close(reopened)
      end

      test "reopen after empty create still validates schema", %{adapter: adapter, path: path} do
        assert {:ok, identity} = @adapter.identity(adapter)
        assert :ok = @adapter.close(adapter)
        assert {:ok, reopened} = @adapter.open(path)
        assert {:ok, %{ok: true}} = @adapter.integrity_check(reopened, %{})
        assert {:ok, %{database_uuid: uuid}} = @adapter.identity(reopened)
        assert uuid == identity.database_uuid
        assert :ok = @adapter.close(reopened)
      end
    end
  end
end
