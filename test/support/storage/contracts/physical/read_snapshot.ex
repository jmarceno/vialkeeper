defmodule VialKeeper.Storage.Contracts.Physical.ReadSnapshot do
  @moduledoc """
  Shared snapshot-isolation tests for the SQLite-dialect storage engines.

  Injected into one test module per engine (`test/physical/sqlite/` and
  `test/physical/turso/`).
  """

  defmacro __using__(opts) do
    # quality:reason contract tests are injected via quote into each adapter module
    # credo:disable-for-next-line Credo.Check.Refactor.LongQuoteBlocks
    quote do
      use ExUnit.Case, async: true

      alias VialKeeper.Storage.AdapterCase
      alias VialKeeper.Storage.SQLite.{Context, Lifecycle}
      alias VialKeeper.Storage.Transaction

      @adapter Keyword.fetch!(unquote(opts), :adapter)

      test "in-flight snapshot does not see a concurrent writer commit" do
        writer = create_writer!("vialkeeper-read-snapshot")

        assert {:ok, %{revision: revision}} =
                 @adapter.apply_local_mutation(writer, %{
                   operation: :put,
                   document_id: "doc",
                   body: %{"n" => 1}
                 })

        assert {:ok, reader_ctx} = Lifecycle.open_reader(@adapter.to_context(writer))
        parent = self()

        task =
          Task.async(fn ->
            Transaction.run_snapshot(reader_ctx, fn ctx ->
              {:ok, reader} = Context.unwrap(ctx)
              {:ok, first} = @adapter.get_document(reader, %{document_id: "doc"})
              send(parent, {:in_snapshot, self()})

              receive do
                :continue -> :ok
              end

              {:ok, second} = @adapter.get_document(reader, %{document_id: "doc"})
              {:ok, {first.body, second.body}}
            end)
          end)

        assert_receive {:in_snapshot, pid}, 1_000

        assert {:ok, _} =
                 @adapter.apply_local_mutation(writer, %{
                   operation: :put,
                   document_id: "doc",
                   if_revision: revision,
                   body: %{"n" => 2}
                 })

        send(pid, :continue)
        assert {:ok, {%{"n" => 1}, %{"n" => 1}}} = Task.await(task)

        {:ok, reader} = Context.unwrap(reader_ctx)

        assert {:ok, %{body: %{"n" => 2}}} =
                 @adapter.get_document(reader, %{document_id: "doc"})

        assert :ok = Lifecycle.close_reader(reader_ctx)
      end

      test "nested snapshots join the outer snapshot" do
        writer = create_writer!("vialkeeper-nested-snapshot")

        assert {:ok, reader_ctx} = Lifecycle.open_reader(@adapter.to_context(writer))

        assert {:ok, :joined} =
                 Transaction.run_snapshot(reader_ctx, fn ctx ->
                   Transaction.run_snapshot(ctx, fn _nested -> {:ok, :joined} end)
                 end)

        assert :ok = Lifecycle.close_reader(reader_ctx)
      end

      test "conflict get and revision get stay on one snapshot" do
        writer = create_writer!("vialkeeper-read-snapshot-multi")

        assert {:ok, %{revision: revision}} =
                 @adapter.apply_local_mutation(writer, %{
                   operation: :put,
                   document_id: "doc",
                   body: %{"n" => 1}
                 })

        assert {:ok, reader_ctx} = Lifecycle.open_reader(@adapter.to_context(writer))
        parent = self()

        task =
          Task.async(fn ->
            Transaction.run_snapshot(reader_ctx, fn ctx ->
              {:ok, reader} = Context.unwrap(ctx)

              {:ok, with_conflicts} =
                @adapter.get_document(reader, %{document_id: "doc", include_conflicts: true})

              {:ok, historical} =
                @adapter.get_revision(reader, %{document_id: "doc", revision_id: revision})

              send(parent, {:in_snapshot, self()})

              receive do
                :continue -> :ok
              end

              {:ok, after_wait} =
                @adapter.get_document(reader, %{document_id: "doc", include_conflicts: true})

              {:ok, {with_conflicts.body, historical.body, after_wait.body}}
            end)
          end)

        assert_receive {:in_snapshot, pid}, 1_000

        assert {:ok, _} =
                 @adapter.apply_local_mutation(writer, %{
                   operation: :put,
                   document_id: "doc",
                   if_revision: revision,
                   body: %{"n" => 2}
                 })

        send(pid, :continue)
        assert {:ok, {%{"n" => 1}, %{"n" => 1}, %{"n" => 1}}} = Task.await(task)
        assert :ok = Lifecycle.close_reader(reader_ctx)
      end

      defp create_writer!(prefix) do
        {:ok, bundle} = VialKeeper.TempDatabase.create(prefix: prefix)
        path = AdapterCase.adapter_path(@adapter, bundle)

        assert {:ok, writer} = @adapter.create(path, %{storage_mode: :disk})

        on_exit(fn ->
          _ = @adapter.close(writer)
          VialKeeper.TempDatabase.cleanup(bundle)
        end)

        writer
      end
    end
  end
end
