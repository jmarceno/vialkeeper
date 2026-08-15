defmodule VialKeeper.Retention.ServiceEffectTest do
  @moduledoc """
  Property and effect proofs that Retention.Service plan decisions match
  compact_retention outcomes on Memory (and SQLite when cheap).
  """

  use ExUnit.Case, async: true
  use ExUnitProperties

  alias VialKeeper.Retention.Frontier
  alias VialKeeper.Retention.Service, as: RetentionService
  alias VialKeeper.Storage.AdapterCase
  alias VialKeeper.Storage.BackendContext
  alias VialKeeper.Storage.Ports.Access
  alias VialKeeper.Storage.Services.Facts

  @retention_config %{
    "retention" => %{
      "mode" => "stable_frontier",
      "history_depth" => 0,
      "peer_expiry_ms" => 86_400_000,
      "schedule" => "disabled"
    }
  }

  @adapters [
    VialKeeper.Storage.Memory.Adapter,
    VialKeeper.Storage.SQLite.Adapter
  ]

  for adapter <- @adapters do
    @adapter adapter

    describe "#{inspect(@adapter)} plan→effect" do
      property "compact_retention stats match Retention.Service decide_compaction" do
        check all(
                doc_count <- StreamData.integer(1..4),
                depth <- StreamData.integer(1..3),
                max_runs: 12
              ) do
          {:ok, bundle_path} = VialKeeper.TempDatabase.create(prefix: "vialkeeper-ret-effect")
          path = AdapterCase.adapter_path(@adapter, bundle_path)
          {:ok, adapter} = @adapter.create(path, %{})

          try do
            assert {:ok, _} = @adapter.update_config(adapter, @retention_config)

            for n <- 1..doc_count do
              document_id = "doc-#{n}"

              assert {:ok, %{revision: rev}} =
                       @adapter.apply_local_mutation(adapter, %{
                         operation: :put,
                         document_id: document_id,
                         body: %{"n" => 0}
                       })

              Enum.reduce(1..depth, rev, fn step, parent ->
                assert {:ok, %{revision: next}} =
                         @adapter.apply_local_mutation(adapter, %{
                           operation: :put,
                           document_id: document_id,
                           if_revision: parent,
                           body: %{"n" => step}
                         })

                next
              end)
            end

            context = @adapter.to_context(adapter)
            now = DateTime.utc_now()
            {:ok, decision} = decide(context, now)

            assert {:ok, stats} = @adapter.compact_retention(adapter, %{})

            case decision do
              {:noop, expected} ->
                assert stats.noop? == expected.noop?
                assert stats.new_floor == expected.new_floor
                assert stats.removed_revisions == expected.removed_revisions
                assert stats.removed_changes == expected.removed_changes

              {:apply, _frontier, _plan, effect} ->
                assert stats.noop? == not effect.increment_maintenance?
                assert stats.new_floor == effect.new_floor
                assert stats.new_compaction_epoch == effect.new_compaction_epoch
                assert stats.removed_revisions == effect.result_stats["removed_revisions"]
                assert stats.removed_changes == effect.result_stats["removed_changes"]
            end
          after
            _ = @adapter.close(adapter)
            VialKeeper.TempDatabase.cleanup(bundle_path)
          end
        end
      end
    end
  end

  defp decide(%BackendContext{} = context, now) do
    with {:ok, meta} <- load_meta(context),
         {:ok, peers} <- Facts.list_peer_positions(context),
         {:ok, boundaries} <-
           Facts.list_boundaries(context, source_database_uuid: meta.database_uuid),
         frontier_input =
           RetentionService.compute_frontier_input(
             meta,
             peers,
             RetentionService.retention_mode(meta.config),
             now
           ),
         frontier = Frontier.compute(frontier_input),
         {:ok, plan_docs} <- Facts.list_compaction_documents(context, frontier.candidate_floor) do
      {:ok,
       RetentionService.decide_compaction(meta, peers, boundaries, plan_docs, meta.config, now)}
    end
  end

  defp load_meta(%BackendContext{} = context) do
    case Access.port(context, :lifecycle).identity(context) do
      {:ok, identity} when is_map(identity) ->
        config = Map.get(identity, :config) || VialKeeper.Config.defaults()

        {:ok,
         %{
           database_uuid: Map.fetch!(identity, :database_uuid),
           history_epoch: Map.fetch!(identity, :history_epoch),
           current_sequence: Map.get(identity, :current_sequence, 0),
           retention_floor_sequence: Map.get(identity, :retention_floor_sequence, 0),
           compaction_epoch: Map.get(identity, :compaction_epoch, 0),
           retention_boundary_digest: Map.get(identity, :retention_boundary_digest),
           config: config
         }}

      {:error, _} = error ->
        error
    end
  end
end
