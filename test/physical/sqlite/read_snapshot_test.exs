defmodule ElixirDB.StorageAdapter.ReadSnapshotTest do
  @moduledoc "Covers deferred snapshot isolation for concurrent writers. plan §8.2"
  use ExUnit.Case, async: true

  @moduletag :sqlite_physical

  alias ElixirDB.Storage.SQLite.{Adapter, Context, Lifecycle}
  alias ElixirDB.Storage.Transaction

  test "in-flight snapshot does not see a concurrent writer commit" do
    {:ok, bundle} = ElixirDB.TempDatabase.create(prefix: "elixirdb-read-snapshot")
    sqlite = ElixirDB.TempDatabase.sqlite_path(bundle)

    assert {:ok, writer} = Adapter.create(sqlite, %{storage_mode: :disk})

    on_exit(fn ->
      _ = Adapter.close(writer)
      ElixirDB.TempDatabase.cleanup(bundle)
    end)

    assert {:ok, %{revision: revision}} =
             Adapter.apply_local_mutation(writer, %{
               operation: :put,
               document_id: "doc",
               body: %{"n" => 1}
             })

    assert {:ok, reader_ctx} = Lifecycle.open_reader(Adapter.to_context(writer))
    parent = self()

    task =
      Task.async(fn ->
        Transaction.run_snapshot(reader_ctx, fn ctx ->
          {:ok, reader} = Context.unwrap(ctx)
          {:ok, first} = Adapter.get_document(reader, %{document_id: "doc"})
          send(parent, {:in_snapshot, self()})

          receive do
            :continue -> :ok
          end

          {:ok, second} = Adapter.get_document(reader, %{document_id: "doc"})
          {:ok, {first.body, second.body}}
        end)
      end)

    assert_receive {:in_snapshot, pid}, 1_000

    assert {:ok, _} =
             Adapter.apply_local_mutation(writer, %{
               operation: :put,
               document_id: "doc",
               if_revision: revision,
               body: %{"n" => 2}
             })

    send(pid, :continue)
    assert {:ok, {%{"n" => 1}, %{"n" => 1}}} = Task.await(task)

    {:ok, reader} = Context.unwrap(reader_ctx)

    assert {:ok, %{body: %{"n" => 2}}} =
             Adapter.get_document(reader, %{document_id: "doc"})

    assert :ok = Lifecycle.close_reader(reader_ctx)
  end

  test "nested snapshots join the outer snapshot" do
    {:ok, bundle} = ElixirDB.TempDatabase.create(prefix: "elixirdb-nested-snapshot")
    sqlite = ElixirDB.TempDatabase.sqlite_path(bundle)

    assert {:ok, writer} = Adapter.create(sqlite, %{storage_mode: :disk})

    on_exit(fn ->
      _ = Adapter.close(writer)
      ElixirDB.TempDatabase.cleanup(bundle)
    end)

    assert {:ok, reader_ctx} = Lifecycle.open_reader(Adapter.to_context(writer))

    assert {:ok, :joined} =
             Transaction.run_snapshot(reader_ctx, fn ctx ->
               Transaction.run_snapshot(ctx, fn _nested -> {:ok, :joined} end)
             end)

    assert :ok = Lifecycle.close_reader(reader_ctx)
  end

  test "conflict get and revision get stay on one snapshot" do
    {:ok, bundle} = ElixirDB.TempDatabase.create(prefix: "elixirdb-read-snapshot-multi")
    sqlite = ElixirDB.TempDatabase.sqlite_path(bundle)

    assert {:ok, writer} = Adapter.create(sqlite, %{storage_mode: :disk})

    on_exit(fn ->
      _ = Adapter.close(writer)
      ElixirDB.TempDatabase.cleanup(bundle)
    end)

    assert {:ok, %{revision: revision}} =
             Adapter.apply_local_mutation(writer, %{
               operation: :put,
               document_id: "doc",
               body: %{"n" => 1}
             })

    assert {:ok, reader_ctx} = Lifecycle.open_reader(Adapter.to_context(writer))
    parent = self()

    task =
      Task.async(fn ->
        Transaction.run_snapshot(reader_ctx, fn ctx ->
          {:ok, reader} = Context.unwrap(ctx)

          {:ok, with_conflicts} =
            Adapter.get_document(reader, %{document_id: "doc", include_conflicts: true})

          {:ok, historical} =
            Adapter.get_revision(reader, %{document_id: "doc", revision_id: revision})

          send(parent, {:in_snapshot, self()})

          receive do
            :continue -> :ok
          end

          {:ok, after_wait} =
            Adapter.get_document(reader, %{document_id: "doc", include_conflicts: true})

          {:ok, {with_conflicts.body, historical.body, after_wait.body}}
        end)
      end)

    assert_receive {:in_snapshot, pid}, 1_000

    assert {:ok, _} =
             Adapter.apply_local_mutation(writer, %{
               operation: :put,
               document_id: "doc",
               if_revision: revision,
               body: %{"n" => 2}
             })

    send(pid, :continue)
    assert {:ok, {%{"n" => 1}, %{"n" => 1}, %{"n" => 1}}} = Task.await(task)
    assert :ok = Lifecycle.close_reader(reader_ctx)
  end
end
