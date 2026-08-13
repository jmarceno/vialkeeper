defmodule ElixirDB.EndToEnd.ReplicationChaosTest do
  @moduledoc """
  Seeded two-node replication convergence and multi-master echo scenarios.
  """

  use ExUnit.Case, async: false

  @moduletag :integration

  alias ElixirDB.ReplicationChaosHelpers, as: Chaos

  @seeds Chaos.seeds!()
  @actions [:pass, :error, :delay, :duplicate, :reorder]

  for seed <- @seeds do
    @tag :slow
    test "source chaos converges without skipped revisions (seed #{seed})" do
      run_source_scenario(unquote(seed))
    end
  end

  @tag :slow
  test "source chaos exercises every action across an isolated aggregate run" do
    aggregate =
      Enum.reduce(@seeds, Chaos.empty_stats(), fn seed, aggregate ->
        Chaos.merge_stats(aggregate, run_source_scenario(seed))
      end)

    Enum.each(@actions, fn action ->
      assert aggregate[action] > 0,
             "seeds #{inspect(@seeds)}: chaos action #{action} never occurred; " <>
               "aggregate stats=#{inspect(aggregate)}"
    end)
  end

  for seed <- @seeds do
    @tag :slow
    test "bidirectional multi-master chaos converges and remains echo-stable (seed #{seed})" do
      seed = unquote(seed)
      cluster = Chaos.create_cluster!("e2e-bidirectional-chaos", [:a, :b], seed)

      try do
        a_uuid = Chaos.uuid(cluster, :a)
        b_uuid = Chaos.uuid(cluster, :b)
        roots = Chaos.seed_bidirectional_corpus!(a_uuid, b_uuid, seed)
        Chaos.diverge_bidirectional!(a_uuid, b_uuid, roots, seed)

        edges = [
          Chaos.chaos_edge!(a_uuid, b_uuid, seed, 11),
          Chaos.chaos_edge!(b_uuid, a_uuid, seed, 21)
        ]

        edges = Chaos.converge_bidirectional!(edges, [a_uuid, b_uuid], seed)

        Chaos.assert_chaos_evidence!(edges, seed)
        Chaos.assert_exact_convergence!([a_uuid, b_uuid], seed)
        Enum.each(edges, &Chaos.assert_edge_checkpoint_aligned!(&1, seed))
        Chaos.assert_delete_update_race!([a_uuid, b_uuid], "shared-13", seed)
        Chaos.assert_delete_update_race!([a_uuid, b_uuid], "shared-19", seed)

        converged = Chaos.snapshot!(a_uuid, seed)
        conflict = Map.fetch!(converged.documents, "shared-1")
        deleted = Map.fetch!(converged.documents, "shared-23")

        assert MapSet.size(conflict.conflicts) == 1,
               "seed #{seed}: divergent updates did not retain one active conflict"

        assert Enum.all?(deleted.leaves, fn {_revision, tombstone?} -> tombstone? end),
               "seed #{seed}: matching deletes did not converge to tombstone-only state"

        checkpoints_before = Chaos.checkpoint_states!(edges, seed)
        stable_checkpoints_before = Chaos.stable_checkpoint_projection(checkpoints_before)
        sequences_before = Chaos.database_sequences!([a_uuid, b_uuid], seed)
        documents_before = converged.documents

        Enum.each(1..2, fn _round ->
          Chaos.run_plain_edge!(a_uuid, b_uuid, seed)
          Chaos.run_plain_edge!(b_uuid, a_uuid, seed)
        end)

        checkpoints_after = Chaos.checkpoint_states!(edges, seed)
        stable_checkpoints_after = Chaos.stable_checkpoint_projection(checkpoints_after)
        sequences_after = Chaos.database_sequences!([a_uuid, b_uuid], seed)
        documents_after = Chaos.snapshot!(a_uuid, seed).documents

        assert stable_checkpoints_after == stable_checkpoints_before,
               "seed #{seed}: no-op echo changed checkpoint protocol version/source sequence"

        assert sequences_after == sequences_before,
               "seed #{seed}: no-op echo advanced a database document sequence"

        assert documents_after == documents_before,
               "seed #{seed}: no-op echo manufactured or removed revisions"

        Chaos.assert_exact_convergence!([a_uuid, b_uuid], seed)
        Chaos.assert_catalog_alive!(seed)
      after
        Chaos.cleanup_cluster(cluster)
      end
    end
  end

  defp run_source_scenario(seed) do
    cluster = Chaos.create_cluster!("e2e-source-chaos", [:a, :b], seed)

    try do
      a_uuid = Chaos.uuid(cluster, :a)
      b_uuid = Chaos.uuid(cluster, :b)
      Chaos.seed_source_corpus!(a_uuid, seed)

      source_before = Chaos.snapshot!(a_uuid, seed)
      edge = Chaos.chaos_edge!(a_uuid, b_uuid, seed, 1)
      edge = Chaos.run_chaos_edge!(edge, seed)
      target_after = Chaos.snapshot!(b_uuid, seed)

      assert map_size(source_before.documents) == 40,
             "seed #{seed}: expected exactly forty source documents"

      Chaos.assert_exact_convergence!([a_uuid, b_uuid], seed)
      Chaos.assert_no_skipped_revisions!(source_before, target_after, seed)
      Chaos.assert_edge_checkpoint_aligned!(edge, seed)
      Chaos.assert_catalog_alive!(seed)

      Chaos.edge_stats(edge)
    after
      Chaos.cleanup_cluster(cluster)
    end
  end
end
