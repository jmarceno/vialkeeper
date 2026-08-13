defmodule ElixirDB.EndToEnd.MultiMasterMeshTest do
  @moduledoc """
  Seeded three-node ring convergence through chaos and a paused partition.
  """

  use ExUnit.Case, async: false

  @moduletag :integration

  alias ElixirDB.ReplicationChaosHelpers, as: Chaos

  @seeds Chaos.seeds!()

  for seed <- @seeds do
    @tag :slow
    test "three-node ring converges through chaos and partition-heal (seed #{seed})" do
      seed = unquote(seed)
      cluster = Chaos.create_cluster!("e2e-multi-master-mesh", [:a, :b, :c], seed)

      try do
        alternate =
          Chaos.create_cluster!("e2e-multi-master-mesh-alternate", [:a, :b, :c], seed)

        try do
          primary = converge_initial_mesh!(cluster, seed, :forward)
          rotated = converge_initial_mesh!(alternate, seed, :reverse)

          assert winner_map!(hd(primary.uuids), seed) == winner_map!(hd(rotated.uuids), seed),
                 "seed #{seed}: winner selection changed under reversed ring execution order"

          assert_partition_heal!(cluster, primary, seed)
        after
          Chaos.cleanup_cluster(alternate)
        end
      after
        Chaos.cleanup_cluster(cluster)
      end
    end
  end

  defp converge_initial_mesh!(cluster, seed, order) do
    uuids = cluster_uuids(cluster)
    roots = Chaos.seed_mesh_corpus!(cluster, seed)
    Chaos.diverge_mesh!(cluster, roots, seed)

    edges =
      cluster
      |> ring_edges(seed, order)
      |> Chaos.converge_ring!(uuids, seed)

    Chaos.assert_chaos_evidence!(edges, seed)
    Chaos.assert_exact_convergence!(uuids, seed)
    Enum.each(edges, &Chaos.assert_edge_checkpoint_aligned!(&1, seed))
    Chaos.assert_delete_update_race!(uuids, "mesh-6", seed)
    Chaos.assert_delete_update_race!(uuids, "mesh-9", seed)
    Chaos.assert_delete_update_race!(uuids, "mesh-12", seed)

    %{edges: edges, roots: roots, uuids: uuids}
  end

  defp assert_partition_heal!(cluster, primary, seed) do
    stats_before_partition = Chaos.edges_stats(primary.edges)
    checkpoints_before_partition = Chaos.checkpoint_states!(primary.edges, seed)

    Chaos.diverge_partition!(cluster, primary.roots, seed)

    assert Chaos.edges_stats(primary.edges) == stats_before_partition,
           "seed #{seed}: a replication endpoint was called while the ring was paused"

    assert Chaos.checkpoint_states!(primary.edges, seed) == checkpoints_before_partition,
           "seed #{seed}: a replication checkpoint changed while the ring was paused"

    partition_states = Enum.map(primary.uuids, &Chaos.snapshot!(&1, seed).documents)

    assert Enum.count_until(Enum.uniq(partition_states), 2) == 2,
           "seed #{seed}: independent partition writes did not create divergent node state"

    edges = Chaos.converge_ring!(primary.edges, primary.uuids, seed)

    Chaos.assert_exact_convergence!(primary.uuids, seed)
    Enum.each(edges, &Chaos.assert_edge_checkpoint_aligned!(&1, seed))
    Chaos.assert_delete_update_race!(primary.uuids, "mesh-17", seed)
    Chaos.assert_delete_update_race!(primary.uuids, "mesh-19", seed)
    Chaos.assert_delete_update_race!(primary.uuids, "mesh-20", seed)
    Chaos.assert_catalog_alive!(seed)
  end

  defp ring_edges(cluster, seed, :forward) do
    a_uuid = Chaos.uuid(cluster, :a)
    b_uuid = Chaos.uuid(cluster, :b)
    c_uuid = Chaos.uuid(cluster, :c)

    [
      Chaos.chaos_edge!(a_uuid, b_uuid, seed, 31),
      Chaos.chaos_edge!(b_uuid, c_uuid, seed, 41),
      Chaos.chaos_edge!(c_uuid, a_uuid, seed, 51)
    ]
  end

  defp ring_edges(cluster, seed, :reverse) do
    cluster
    |> ring_edges(seed, :forward)
    |> Enum.reverse()
  end

  defp cluster_uuids(cluster) do
    Enum.map([:a, :b, :c], &Chaos.uuid(cluster, &1))
  end

  defp winner_map!(uuid, seed) do
    uuid
    |> Chaos.snapshot!(seed)
    |> Map.fetch!(:documents)
    |> Map.new(fn {document_id, state} -> {document_id, state.winner} end)
  end
end
