defmodule VialKeeper.Storage.SequenceBoundsTest do
  @moduledoc """
  Every sequence-producing write command runs with exactly the reservation
  `VialKeeper.Runtime.WriteKeys.sequence_count/1` sizes for it, and never
  needs a number beyond it.
  """
  use ExUnit.Case, async: true

  alias VialKeeper.Commands
  alias VialKeeper.DerivedView.Engine
  alias VialKeeper.Revisions.Id
  alias VialKeeper.Runtime.WriteKeys
  alias VialKeeper.Storage.{AdapterCase, Services}
  alias VialKeeper.Storage.Contracts.Derived.Support, as: Derived
  alias VialKeeper.Storage.Services.Sequences
  alias VialKeeper.Storage.SQLite.{Adapter, ChangeLog}

  setup do
    {:ok, bundle} = VialKeeper.TempDatabase.create(prefix: "vialkeeper-sequence-bounds")
    {:ok, adapter} = Adapter.create(VialKeeper.TempDatabase.sqlite_path(bundle), %{})

    on_exit(fn ->
      _ = Adapter.close(adapter)
      VialKeeper.TempDatabase.cleanup(bundle)
    end)

    {:ok, context: Adapter.to_context(adapter)}
  end

  test "document writes stay within their reservations", %{context: context} do
    put = %Commands.PutDocument{request: %{document_id: "a", body: %{"n" => 1}}}

    assert {:ok, %{revision: a1}} =
             bounded(context, put, fn ->
               Services.apply_local_mutation(context, Map.put(put.request, :operation, :put))
             end)

    bulk = %Commands.BulkWrite{
      request: %{
        operations: [
          %{operation: :put, document_id: "a", if_revision: a1, body: %{"n" => 2}},
          %{operation: :put, document_id: "b", body: %{"n" => 1}},
          %{operation: :put, document_id: "c", body: %{"n" => 1}}
        ]
      }
    }

    assert {:ok, [%{revision: a2}, _b, _c]} =
             bounded(context, bulk, fn -> Services.apply_bulk_mutation(context, bulk.request) end)

    delete = %Commands.DeleteDocument{request: %{document_id: "a", if_revision: a2}}

    assert {:ok, _} =
             bounded(context, delete, fn ->
               Services.apply_local_mutation(context, Map.put(delete.request, :operation, :delete))
             end)
  end

  test "imports and conflict resolution stay within their reservations", %{context: context} do
    history_id = VialKeeper.RevisionFixtures.shared_history_id()
    {:ok, root} = Id.calculate("doc", history_id, nil, false, %{"v" => 0}, %{})
    {:ok, left} = Id.calculate("doc", history_id, root, false, %{"v" => "left"}, %{})
    {:ok, right} = Id.calculate("doc", history_id, root, false, %{"v" => "right"}, %{})
    {:ok, other} = Id.calculate("other", history_id, nil, false, %{"v" => 1}, %{})

    import = %Commands.ImportRevisionChains{
      request: %{
        chains: [
          chain("doc", left, [{root, nil, %{"v" => 0}}, {left, root, %{"v" => "left"}}]),
          chain("doc", right, [{root, nil, %{"v" => 0}}, {right, root, %{"v" => "right"}}]),
          chain("other", other, [{other, nil, %{"v" => 1}}])
        ]
      }
    }

    assert WriteKeys.sequence_count(import) == 2

    assert {:ok, %{documents_changed: 2}} =
             bounded(context, import, fn ->
               Services.import_revision_chains(context, import.request)
             end)

    resolve = %Commands.ResolveConflict{
      request: %{
        document_id: "doc",
        expected_live_revisions: [left, right],
        chosen_parent_revision: left,
        body: %{"v" => "resolved"}
      }
    }

    assert {:ok, %{replayed: false}} =
             bounded(context, resolve, fn -> Services.resolve_conflict(context, resolve.request) end)
  end

  test "derived batches, rebuild pages and prunes stay within their reservations" do
    ctx = Derived.open_derived(Adapter)
    context = Adapter.to_context(ctx.adapter)

    rows = [
      Engine.source_row("one", "1-one", ["alpha"], 1),
      Engine.source_row("two", "1-two", ["beta"], 2)
    ]

    batch = %Commands.ApplyDerivedSourceBatch{
      request:
        Engine.batch_request(
          ctx.materialization_id,
          ctx.source_uuid,
          ctx.history_epoch,
          0,
          1,
          rows,
          []
        )
    }

    assert {:ok, %{applied: true}} =
             bounded(context, batch, fn ->
               Services.apply_derived_source_batch(context, batch.request)
             end)

    removal = %Commands.ApplyDerivedSourceBatch{
      request: %{
        batch.request
        | expected_checkpoint_sequence: 1,
          through_sequence: 2,
          rows: [Engine.source_row("one", "2-one", ["gamma"], 3)],
          removals: ["two"]
      }
    }

    assert {:ok, %{applied: true}} =
             bounded(context, removal, fn ->
               Services.apply_derived_source_batch(context, removal.request)
             end)

    assert {:ok, %{generation: generation}} =
             Services.begin_derived_source_rebuild(context, %{
               materialization_id: ctx.materialization_id,
               source_database_uuid: ctx.source_uuid,
               start_sequence: 0
             })

    page = %Commands.ApplyDerivedRebuildPage{
      request: %{
        materialization_id: ctx.materialization_id,
        source_database_uuid: ctx.source_uuid,
        generation: generation,
        rows: [Engine.source_row("three", "1-three", ["delta"], 4)],
        removals: [],
        after_document_id: "three"
      }
    }

    assert {:ok, _} =
             bounded(context, page, fn ->
               Services.apply_derived_rebuild_page(context, page.request)
             end)

    prune = %Commands.PruneDerivedRebuildStalePage{
      request: %{
        materialization_id: ctx.materialization_id,
        source_database_uuid: ctx.source_uuid,
        generation: generation,
        limit: 10
      }
    }

    assert {:ok, %{removed: 1}} =
             bounded(context, prune, fn ->
               Services.prune_derived_rebuild_stale_page(context, prune.request)
             end)
  end

  test "grouped derived batches stay within their reservations" do
    source_uuid = VialKeeper.UUID.v4()
    {adapter, materialization_id} = Derived.open_stats_derived(Adapter, source_uuid)
    context = Adapter.to_context(adapter)

    rows =
      for n <- 1..4,
          do: Engine.source_row("row-#{n}", "1-row-#{n}", ["group-#{rem(n, 3)}", n], n)

    batch = %Commands.ApplyDerivedSourceBatch{
      request:
        Engine.batch_request(materialization_id, source_uuid, VialKeeper.UUID.v4(), 0, 1, rows, [])
    }

    assert {:ok, %{applied: true}} =
             bounded(context, batch, fn ->
               Services.apply_derived_source_batch(context, batch.request)
             end)
  end

  # Runs `fun` holding exactly the reservation the runtime would make for
  # `command`; taking more than that fails the write with an internal error.
  defp bounded(context, command, fun) do
    count = WriteKeys.sequence_count(command)
    assert count > 0
    uuid = context.identity.database_uuid
    {:ok, high_water} = Sequences.high_water(context)
    assert :ok = ChangeLog.persist_sequence_reservation(context, high_water + count)
    :ok = Sequences.put_reservation(uuid, make_ref(), high_water + 1, high_water + count)

    try do
      fun.()
    after
      assert %{max_used: max_used} = Sequences.pop_reservation(uuid)
      assert max_used <= high_water + count
    end
  end

  defp chain(document_id, leaf, revisions) do
    %{
      document_id: document_id,
      leaf_revision: leaf,
      revisions:
        Enum.map(revisions, fn {revision, parent, body} ->
          AdapterCase.wire_revision(document_id, revision, parent, false, body)
        end)
    }
  end
end
