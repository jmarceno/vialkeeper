defmodule VialKeeper.Storage.Contracts.Physical.ReadPoolConnection do
  @moduledoc """
  Shared readonly snapshot-connection tests for the SQLite-dialect storage
  engines.

  Injected into one test module per engine (`test/physical/sqlite/` and
  `test/physical/turso/`).
  """

  defmacro __using__(opts) do
    quote do
      use ExUnit.Case, async: true

      alias VialKeeper.Storage.AdapterCase
      alias VialKeeper.Storage.SQLite.{Connection, Context, Lifecycle}

      @adapter Keyword.fetch!(unquote(opts), :adapter)

      test "disk reader is readonly and query_only" do
        {:ok, bundle} = VialKeeper.TempDatabase.create(prefix: "vialkeeper-read-pool-conn")
        path = AdapterCase.adapter_path(@adapter, bundle)

        assert {:ok, writer} = @adapter.create(path, %{storage_mode: :disk})

        on_exit(fn ->
          _ = @adapter.close(writer)
          VialKeeper.TempDatabase.cleanup(bundle)
        end)

        assert {:ok, _} =
                 @adapter.apply_local_mutation(writer, %{
                   operation: :put,
                   document_id: "doc",
                   body: %{"n" => 1}
                 })

        assert {:ok, reader_ctx} = Lifecycle.open_reader(@adapter.to_context(writer))
        assert :ok = Lifecycle.interrupt_reader(reader_ctx)
        assert {:ok, reader} = Context.unwrap(reader_ctx)
        assert reader.reader?
        assert {:ok, [[1]]} = Connection.pragma(reader.conn, "query_only")

        assert {:ok, %{body: %{"n" => 1}}} =
                 @adapter.get_document(reader, %{document_id: "doc"})

        assert {:error, _reason} =
                 Connection.execute(reader.conn, "CREATE TABLE forbidden(id INTEGER)")

        assert {:error, _reason} =
                 Connection.execute(
                   reader.conn,
                   "INSERT INTO local_records(namespace, record_key, record_version, value_json) VALUES ('reader', 'forbidden', 1, '{}')"
                 )

        assert :ok = Lifecycle.close_reader(reader_ctx)
        assert :ok = @adapter.close(writer)

        for suffix <- ["-wal", "-shm", "-log"] do
          refute File.exists?(path <> suffix)
        end
      end
    end
  end
end
