defmodule VialKeeper.Storage.Contracts.Physical.CorruptOpen do
  @moduledoc """
  Shared typed-failure tests for opening corrupt or foreign artifacts on the
  SQLite-dialect storage engines.

  Injected into one test module per engine (`test/physical/sqlite/` and
  `test/physical/turso/`).
  """

  defmacro __using__(opts) do
    # quality:reason contract tests are injected via quote into each adapter module
    # credo:disable-for-next-line Credo.Check.Refactor.LongQuoteBlocks
    quote do
      use ExUnit.Case, async: true

      alias VialKeeper.Storage.AdapterCase
      alias VialKeeper.Storage.SQLite.Connection

      @adapter Keyword.fetch!(unquote(opts), :adapter)
      @driver AdapterCase.driver(@adapter)

      test "open returns a typed error for random 4 KiB input" do
        path = corrupt_path("vialkeeper-random-sqlite", :crypto.strong_rand_bytes(4_096))

        assert_typed_error(@adapter.open(path, %{}))
      end

      test "open returns a typed error for a real database truncated to 100 bytes" do
        {bundle, path} = database_path("vialkeeper-truncated-sqlite")
        assert {:ok, adapter} = @adapter.create(path, %{})
        assert :ok = @adapter.close(adapter)

        contents = File.read!(path)
        assert byte_size(contents) > 100
        File.write!(path, binary_part(contents, 0, 100))

        on_exit(fn -> VialKeeper.TempDatabase.cleanup(bundle) end)

        assert_typed_error(@adapter.open(path, %{}))
      end

      test "open rejects a foreign schema with unsupported_format" do
        {bundle, path} = database_path("vialkeeper-foreign-sqlite")
        {:ok, conn} = Connection.open(path, driver: @driver)

        try do
          assert :ok =
                   Connection.exec(
                     conn,
                     "CREATE TABLE foreign_data(id INTEGER)"
                   )
        after
          assert :ok = Connection.close(conn)
        end

        on_exit(fn -> VialKeeper.TempDatabase.cleanup(bundle) end)

        assert {:error, %VialKeeper.Error{code: :unsupported_format}} = @adapter.open(path, %{})
      end

      test "open freezes the zero-byte artifact result as unsupported_format" do
        path = corrupt_path("vialkeeper-empty-sqlite", <<>>)

        assert {:error, %VialKeeper.Error{code: :unsupported_format}} = @adapter.open(path, %{})
      end

      test "open returns a typed error for a directory path" do
        {:ok, bundle} = VialKeeper.TempDatabase.create(prefix: "vialkeeper-directory-sqlite")
        on_exit(fn -> VialKeeper.TempDatabase.cleanup(bundle) end)

        assert_typed_error(@adapter.open(bundle, %{}))
      end

      test "open_reader returns a typed error for a corrupt artifact" do
        path = corrupt_path("vialkeeper-corrupt-reader", :crypto.strong_rand_bytes(4_096))

        writer = %VialKeeper.Storage.SQLite.Adapter{
          path: path,
          storage_mode: :disk,
          driver: @driver,
          identity: %{database_uuid: VialKeeper.UUID.v4()}
        }

        assert_typed_error(@adapter.open_reader(writer))
      end

      test "open_reader rejects a writer UUID mismatch" do
        {bundle, path} = database_path("vialkeeper-reader-uuid-mismatch")
        assert {:ok, writer} = @adapter.create(path, %{})

        on_exit(fn ->
          _ = @adapter.close(writer)
          VialKeeper.TempDatabase.cleanup(bundle)
        end)

        mismatched_writer = %{
          writer
          | identity: Map.put(writer.identity, :database_uuid, VialKeeper.UUID.v4())
        }

        assert {:error,
                %VialKeeper.Error{
                  code: :database_unavailable,
                  details: %{reason: :uuid_mismatch}
                }} = @adapter.open_reader(mismatched_writer)
      end

      defp corrupt_path(prefix, contents) do
        {bundle, path} = database_path(prefix)
        File.write!(path, contents)
        on_exit(fn -> VialKeeper.TempDatabase.cleanup(bundle) end)
        path
      end

      defp database_path(prefix) do
        {:ok, bundle} = VialKeeper.TempDatabase.create(prefix: prefix)
        {bundle, AdapterCase.adapter_path(@adapter, bundle)}
      end

      defp assert_typed_error(result) do
        assert {:error, %VialKeeper.Error{}} = result
      end
    end
  end
end
