defmodule VialKeeper.ReplicationChaosHelpers do
  @moduledoc """
  Shared deterministic setup and assertions for replication chaos scenarios.

  The helpers keep endpoint random state alive across full one-shot retries so
  durable checkpoints, rather than a reset random stream, drive recovery.
  """

  import ExUnit.Assertions

  alias VialKeeper.ChaosEndpoint
  alias VialKeeper.MapAccess
  alias VialKeeper.Replication
  alias VialKeeper.Replication.{Id, LocalEndpoint}
  alias VialKeeper.Runtime.DatabaseCatalog
  alias VialKeeper.Storage.AdapterCase
  alias VialKeeper.TestRevisionId, as: RevisionId

  @committed_seeds [11, 23, 37, 41, 53]
  @actions [:pass, :error, :delay, :duplicate, :reorder]
  @maximum_attempts 25
  @replication_options %{batch: 8, batch_documents: 3}
  @changes_limit 100
  @scenario_weights %{pass: 48, error: 2, delay: 10, duplicate: 20, reorder: 20}

  @doc "Returns the committed seeds plus an optional integer `CHAOS_SEED`."
  @spec seeds!() :: [integer()]
  def seeds! do
    case System.get_env("CHAOS_SEED") do
      nil ->
        @committed_seeds

      value ->
        case Integer.parse(value) do
          {seed, ""} -> Enum.uniq(Enum.concat(@committed_seeds, [seed]))
          _other -> raise ArgumentError, "CHAOS_SEED must be an integer, got: #{inspect(value)}"
        end
    end
  end

  @doc "Returns a zeroed action-count map."
  @spec empty_stats() :: %{atom() => non_neg_integer()}
  def empty_stats, do: Map.new(@actions, &{&1, 0})

  @doc "Creates isolated local databases identified by the supplied labels."
  @spec create_cluster!(binary(), [atom()], integer()) :: map()
  def create_cluster!(prefix, labels, seed) do
    root = VialKeeper.Config.database_root()
    unique = System.unique_integer([:positive])

    entries =
      Map.new(labels, fn label ->
        path = "#{prefix}-#{seed}-#{unique}-#{label}.vialkeeper"
        VialKeeper.TempDatabase.cleanup(Path.join(root, path))

        identity =
          DatabaseCatalog.create(path)
          |> ok!(seed, "create database #{label}")

        {label, %{identity: identity, path: path}}
      end)

    %{root: root, entries: entries}
  end

  @doc "Closes, unregisters, and removes every database in a cluster."
  @spec cleanup_cluster(map()) :: :ok
  def cleanup_cluster(%{root: root, entries: entries}) do
    Enum.each(entries, fn {_label, %{identity: identity, path: path}} ->
      _ = DatabaseCatalog.close(identity.database_uuid)
      _ = DatabaseCatalog.unregister(identity.database_uuid)
      VialKeeper.TempDatabase.cleanup(Path.join(root, path))
    end)

    :ok
  end

  @doc "Returns the database UUID for a cluster label."
  @spec uuid(map(), atom()) :: binary()
  def uuid(cluster, label),
    do: cluster.entries |> Map.fetch!(label) |> Map.fetch!(:identity) |> Map.fetch!(:database_uuid)

  @doc "Seeds forty source documents with updates, tombstones, and sibling conflicts."
  @spec seed_source_corpus!(binary(), integer()) :: :ok
  def seed_source_corpus!(uuid, seed) do
    roots =
      Map.new(1..32, fn index ->
        id = source_id(index)
        revision = put!(uuid, id, nil, %{"seed" => seed, "version" => 1}, seed)
        {index, revision}
      end)

    Enum.each(1..12, fn index ->
      put!(
        uuid,
        source_id(index),
        Map.fetch!(roots, index),
        %{"seed" => seed, "version" => 2, "updated" => true},
        seed
      )
    end)

    Enum.each(13..18, fn index ->
      delete!(uuid, source_id(index), Map.fetch!(roots, index), seed)
    end)

    chains =
      Enum.flat_map(33..40, fn index ->
        sibling_chains(source_id(index), seed, "source")
      end)

    DatabaseCatalog.command(
      uuid,
      {:command, :import_revision_chains, %{chains: chains}}
    )
    |> ok!(seed, "import deliberate sibling conflicts")

    :ok
  end

  @doc "Creates a shared two-node corpus and returns its seed revisions."
  @spec seed_bidirectional_corpus!(binary(), binary(), integer()) :: map()
  def seed_bidirectional_corpus!(a_uuid, b_uuid, seed) do
    roots =
      Map.new(1..30, fn index ->
        id = bidirectional_id(index)
        revision = put!(a_uuid, id, nil, %{"seed" => seed, "version" => 1}, seed)
        {index, revision}
      end)

    run_plain_edge!(a_uuid, b_uuid, seed)
    roots
  end

  @doc "Applies concurrent divergent updates and deletes to both peers."
  @spec diverge_bidirectional!(binary(), binary(), map(), integer()) :: :ok
  def diverge_bidirectional!(a_uuid, b_uuid, roots, seed) do
    run_concurrently!(
      [
        fn ->
          Enum.each(1..12, fn index ->
            put!(
              a_uuid,
              bidirectional_id(index),
              Map.fetch!(roots, index),
              %{"seed" => seed, "side" => "a", "version" => 2},
              seed
            )
          end)

          Enum.each(13..18, fn index ->
            delete!(a_uuid, bidirectional_id(index), Map.fetch!(roots, index), seed)
          end)

          Enum.each(19..22, fn index ->
            put!(
              a_uuid,
              bidirectional_id(index),
              Map.fetch!(roots, index),
              %{"seed" => seed, "side" => "a", "race" => "update"},
              seed
            )
          end)

          Enum.each(23..25, fn index ->
            delete!(a_uuid, bidirectional_id(index), Map.fetch!(roots, index), seed)
          end)
        end,
        fn ->
          Enum.each(1..18, fn index ->
            put!(
              b_uuid,
              bidirectional_id(index),
              Map.fetch!(roots, index),
              %{"seed" => seed, "side" => "b", "version" => 2},
              seed
            )
          end)

          Enum.each(19..25, fn index ->
            delete!(b_uuid, bidirectional_id(index), Map.fetch!(roots, index), seed)
          end)
        end
      ],
      seed
    )
  end

  @doc "Creates a shared three-node corpus and returns its seed revisions."
  @spec seed_mesh_corpus!(map(), integer()) :: map()
  def seed_mesh_corpus!(cluster, seed) do
    a_uuid = uuid(cluster, :a)
    b_uuid = uuid(cluster, :b)
    c_uuid = uuid(cluster, :c)

    roots =
      Map.new(1..24, fn index ->
        id = mesh_id(index)
        body = %{"seed" => seed, "version" => 1}
        revision = revision!(id, nil, false, body, seed)

        DatabaseCatalog.command(
          a_uuid,
          {:command, :import_revision_chains,
           %{
             chains: [
               wire_chain(
                 id,
                 revision,
                 [AdapterCase.wire_revision(id, revision, nil, false, body)]
               )
             ]
           }}
        )
        |> ok!(seed, "import deterministic mesh root #{id}")

        {index, revision}
      end)

    run_plain_edge!(a_uuid, b_uuid, seed)
    run_plain_edge!(b_uuid, c_uuid, seed)
    run_plain_edge!(c_uuid, a_uuid, seed)
    roots
  end

  @doc "Applies concurrent overlapping updates and deletes on all ring nodes."
  @spec diverge_mesh!(map(), map(), integer()) :: :ok
  def diverge_mesh!(cluster, roots, seed) do
    a_uuid = uuid(cluster, :a)
    b_uuid = uuid(cluster, :b)
    c_uuid = uuid(cluster, :c)

    run_concurrently!(
      [
        fn ->
          update_mesh_range!(a_uuid, roots, 1..8, seed, "a")
          delete_mesh_range!(a_uuid, roots, 9..11, seed)
          update_mesh_range!(a_uuid, roots, 12..14, seed, "a")
          delete_mesh_range!(a_uuid, roots, 15..16, seed)
        end,
        fn ->
          update_mesh_range!(b_uuid, roots, 1..5, seed, "b")
          update_mesh_range!(b_uuid, roots, 9..11, seed, "b")
          delete_mesh_range!(b_uuid, roots, 12..14, seed)
          delete_mesh_range!(b_uuid, roots, 15..16, seed)
        end,
        fn ->
          update_mesh_range!(c_uuid, roots, 1..5, seed, "c")
          delete_mesh_range!(c_uuid, roots, 6..8, seed)
          update_mesh_range!(c_uuid, roots, 9..14, seed, "c")
          delete_mesh_range!(c_uuid, roots, 15..16, seed)
        end
      ],
      seed
    )
  end

  @doc "Applies independent writes while the ring is intentionally paused."
  @spec diverge_partition!(map(), map(), integer()) :: :ok
  def diverge_partition!(cluster, roots, seed) do
    a_uuid = uuid(cluster, :a)
    b_uuid = uuid(cluster, :b)
    c_uuid = uuid(cluster, :c)

    run_concurrently!(
      [
        fn ->
          delete!(a_uuid, mesh_id(17), Map.fetch!(roots, 17), seed)
          update_mesh_range!(a_uuid, roots, 18..20, seed, "partition-a")
        end,
        fn ->
          update_mesh_range!(b_uuid, roots, 17..19, seed, "partition-b")
          delete!(b_uuid, mesh_id(20), Map.fetch!(roots, 20), seed)
        end,
        fn ->
          update_mesh_range!(c_uuid, roots, 17..18, seed, "partition-c")
          delete_mesh_range!(c_uuid, roots, 19..20, seed)
        end
      ],
      seed
    )
  end

  @doc "Builds a deterministic chaos-wrapped directed replication edge."
  @spec chaos_edge!(binary(), binary(), integer(), integer()) :: map()
  def chaos_edge!(source_uuid, target_uuid, seed, ordinal) do
    seed_edge_checkpoints!(source_uuid, target_uuid, seed)

    source =
      source_uuid
      |> local_endpoint!(seed)
      |> ChaosEndpoint.wrap(
        seed: seed * 100 + ordinal * 2,
        weights: @scenario_weights,
        delay_ms: 1..5
      )

    target =
      target_uuid
      |> local_endpoint!(seed)
      |> ChaosEndpoint.wrap(
        seed: seed * 100 + ordinal * 2 + 1,
        weights: @scenario_weights,
        delay_ms: 1..5
      )

    %{
      source_uuid: source_uuid,
      target_uuid: target_uuid,
      source: source,
      target: target,
      runs: 0,
      attempts: 0,
      retryable_errors: 0
    }
  end

  @doc "Runs a chaos edge, retrying complete one-shots from durable checkpoints."
  @spec run_chaos_edge!(map(), integer()) :: map()
  def run_chaos_edge!(edge, seed) do
    evidence = retry_one_shot(edge, seed, 1)

    edge
    |> Map.update!(:runs, &(&1 + 1))
    |> Map.update!(:attempts, &(&1 + evidence.attempts))
    |> Map.update!(:retryable_errors, &(&1 + evidence.retryable_errors))
  end

  @doc "Runs a plain one-shot edge."
  @spec run_plain_edge!(binary(), binary(), integer()) :: :ok
  def run_plain_edge!(source_uuid, target_uuid, seed) do
    seed_edge_checkpoints!(source_uuid, target_uuid, seed)

    case Replication.one_shot(source_uuid, target_uuid, @replication_options) do
      {:ok, %{status: :completed}} ->
        :ok

      other ->
        flunk("seed #{seed}: plain replication failed: #{inspect(other)}")
    end
  end

  @doc "Runs bounded bidirectional chaos rounds until both peers are exact."
  @spec converge_bidirectional!([map()], [binary()], integer()) :: [map()]
  def converge_bidirectional!(edges, uuids, seed) do
    converge_edges(edges, uuids, seed, 3, "bidirectional")
  end

  @doc "Runs at most five ring rounds until nodes and edge checkpoints align."
  @spec converge_ring!([map()], [binary()], integer()) :: [map()]
  def converge_ring!(edges, uuids, seed) do
    converge_edges(edges, uuids, seed, 5, "ring")
  end

  @doc "Returns aggregate chaos action counts for one edge."
  @spec edge_stats(map()) :: map()
  def edge_stats(edge) do
    merge_stats(ChaosEndpoint.stats(edge.source), ChaosEndpoint.stats(edge.target))
  end

  @doc "Returns aggregate chaos action counts for several edges."
  @spec edges_stats([map()]) :: map()
  def edges_stats(edges) do
    Enum.reduce(edges, empty_stats(), fn edge, total ->
      merge_stats(total, edge_stats(edge))
    end)
  end

  @doc "Returns reorder actions that changed multi-chain import arrival order."
  @spec effective_reorders([map()]) :: non_neg_integer()
  def effective_reorders(edges) do
    Enum.reduce(edges, 0, fn edge, total ->
      total + ChaosEndpoint.effective_reorders(edge.source) +
        ChaosEndpoint.effective_reorders(edge.target)
    end)
  end

  @doc "Returns accumulated one-shot retry evidence for several edges."
  @spec edges_evidence([map()]) :: map()
  def edges_evidence(edges) do
    Enum.reduce(edges, %{runs: 0, attempts: 0, retryable_errors: 0}, fn edge, total ->
      %{
        runs: total.runs + edge.runs,
        attempts: total.attempts + edge.attempts,
        retryable_errors: total.retryable_errors + edge.retryable_errors
      }
    end)
  end

  @doc "Asserts endpoint errors caused retries and data imports saw duplicate or reorder."
  @spec assert_chaos_evidence!([map()], integer()) :: :ok
  def assert_chaos_evidence!(edges, seed) do
    stats = edges_stats(edges)
    evidence = edges_evidence(edges)

    assert stats.error > 0,
           "seed #{seed}: chaos endpoints did not emit a retryable error; stats=#{inspect(stats)}"

    assert evidence.retryable_errors > 0 and evidence.attempts > evidence.runs,
           "seed #{seed}: one-shot retries did not observe endpoint failures; " <>
             "evidence=#{inspect(evidence)}"

    assert effective_reorders(edges) > 0,
           "seed #{seed}: no multi-chain import changed arrival order; stats=#{inspect(stats)}"

    :ok
  end

  @doc "Adds two chaos action-count maps."
  @spec merge_stats(map(), map()) :: map()
  def merge_stats(left, right) do
    Map.new(@actions, fn action ->
      {action, Map.get(left, action, 0) + Map.get(right, action, 0)}
    end)
  end

  @doc "Captures exact leaves, winners, conflicts, and reachable revisions."
  @spec snapshot!(binary(), integer()) :: map()
  def snapshot!(uuid, seed) do
    %{current_sequence: sequence} =
      DatabaseCatalog.command(uuid, {:command, :identity, %{}})
      |> ok!(seed, "read database identity")

    %{results: results} =
      VialKeeper.Changes.read(uuid, %{since: 0, limit: @changes_limit})
      |> ok!(seed, "read complete changes feed")

    endpoint = local_endpoint!(uuid, seed)

    documents =
      Map.new(results, fn change ->
        document_id = MapAccess.get(change, :document_id)

        leaves =
          change
          |> MapAccess.get(:leaf_revisions, [])
          |> Enum.map(fn leaf ->
            {MapAccess.get(leaf, :revision), MapAccess.get(leaf, :deleted, false)}
          end)
          |> MapSet.new()

        winner = MapAccess.get(change, :winning_revision)

        conflicts =
          leaves
          |> Enum.filter(fn {revision, deleted} -> not deleted and revision != winner end)
          |> Enum.map(&elem(&1, 0))
          |> MapSet.new()

        {document_id,
         %{
           leaves: leaves,
           winner: winner,
           conflicts: conflicts,
           revisions: revision_ids!(endpoint, document_id, leaves, seed)
         }}
      end)

    %{sequence: sequence, documents: documents}
  end

  @doc "Asserts exact replicated document state across every supplied node."
  @spec assert_exact_convergence!([binary()], integer()) :: :ok
  def assert_exact_convergence!([first_uuid | rest], seed) do
    expected = snapshot!(first_uuid, seed).documents

    Enum.each(rest, fn uuid ->
      actual = snapshot!(uuid, seed).documents

      assert actual == expected,
             "seed #{seed}: exact leaf/revision/winner/conflict state differs for #{uuid}; " <>
               "expected=#{inspect(expected)} actual=#{inspect(actual)}"
    end)

    :ok
  end

  @doc "Asserts a replicated target contains every captured source revision."
  @spec assert_no_skipped_revisions!(map(), map(), integer()) :: :ok
  def assert_no_skipped_revisions!(source_before, target_after, seed) do
    Enum.each(source_before.documents, fn {document_id, expected} ->
      actual = Map.fetch!(target_after.documents, document_id)

      assert MapSet.subset?(expected.revisions, actual.revisions),
             "seed #{seed}: skipped revisions for #{document_id}: " <>
               inspect(MapSet.difference(expected.revisions, actual.revisions))
    end)

    :ok
  end

  @doc "Asserts both copies of an edge checkpoint equal its source sequence."
  @spec assert_edge_checkpoint_aligned!(map(), integer()) :: :ok
  def assert_edge_checkpoint_aligned!(edge, seed) do
    replication_id = replication_id!(edge.source_uuid, edge.target_uuid, seed)
    expected = snapshot!(edge.source_uuid, seed).sequence
    source = checkpoint_state!(edge.source_uuid, replication_id, seed)
    target = checkpoint_state!(edge.target_uuid, replication_id, seed)

    assert source.source_sequence == expected,
           "seed #{seed}: source checkpoint #{source.source_sequence} != source sequence #{expected}"

    assert target.source_sequence == expected,
           "seed #{seed}: target checkpoint #{target.source_sequence} != source sequence #{expected}"

    :ok
  end

  @doc "Captures both durable checkpoint copies for every directed edge."
  @spec checkpoint_states!([map()], integer()) :: map()
  def checkpoint_states!(edges, seed) do
    Map.new(edges, fn edge ->
      replication_id = replication_id!(edge.source_uuid, edge.target_uuid, seed)

      states = %{
        source: checkpoint_state!(edge.source_uuid, replication_id, seed),
        target: checkpoint_state!(edge.target_uuid, replication_id, seed)
      }

      {{edge.source_uuid, edge.target_uuid}, states}
    end)
  end

  @doc "Projects checkpoint fields that remain stable during a no-op echo."
  @spec stable_checkpoint_projection(map()) :: map()
  def stable_checkpoint_projection(states) do
    Map.new(states, fn {edge, copies} ->
      projected =
        Map.new(copies, fn {side, checkpoint} ->
          {side,
           %{
             protocol_version: checkpoint.protocol_version,
             source_sequence: checkpoint.source_sequence
           }}
        end)

      {edge, projected}
    end)
  end

  @doc "Returns current document sequences for a set of nodes."
  @spec database_sequences!([binary()], integer()) :: map()
  def database_sequences!(uuids, seed) do
    Map.new(uuids, fn uuid -> {uuid, snapshot!(uuid, seed).sequence} end)
  end

  @doc "Asserts a delete/update sibling race retains both leaves and chooses a live winner."
  @spec assert_delete_update_race!([binary()], binary(), integer()) :: :ok
  def assert_delete_update_race!(uuids, document_id, seed) do
    states =
      Enum.map(uuids, fn uuid ->
        snapshot!(uuid, seed).documents |> Map.fetch!(document_id)
      end)

    [expected | rest] = states

    assert Enum.all?(rest, &(&1 == expected)),
           "seed #{seed}: delete/update race state differs for #{document_id}"

    assert Enum.any?(expected.leaves, fn {_revision, deleted} -> deleted end),
           "seed #{seed}: delete/update race #{document_id} lost its tombstone"

    assert Enum.any?(expected.leaves, fn {_revision, deleted} -> not deleted end),
           "seed #{seed}: delete/update race #{document_id} lost its live update"

    assert {expected.winner, false} in expected.leaves,
           "seed #{seed}: delete/update race #{document_id} did not choose a live winner"

    :ok
  end

  @doc "Asserts the central database catalog remains alive."
  @spec assert_catalog_alive!(integer()) :: :ok
  def assert_catalog_alive!(seed) do
    pid = Process.whereis(DatabaseCatalog)

    assert is_pid(pid) and Process.alive?(pid),
           "seed #{seed}: DatabaseCatalog crashed during chaos replication"

    :ok
  end

  defp retry_one_shot(edge, seed, attempt) when attempt <= @maximum_attempts do
    case Replication.one_shot_endpoints(edge.source, edge.target, @replication_options) do
      {:ok, %{status: :completed}} ->
        %{attempts: attempt, retryable_errors: attempt - 1}

      {:error, %VialKeeper.Error{retryable: true}} when attempt < @maximum_attempts ->
        retry_one_shot(edge, seed, attempt + 1)

      {:error, %VialKeeper.Error{retryable: true} = error} ->
        flunk(
          "seed #{seed}: chaos edge #{edge.source_uuid}->#{edge.target_uuid} " <>
            "did not complete in #{@maximum_attempts} attempts: #{inspect(error)}"
        )

      other ->
        flunk(
          "seed #{seed}: chaos edge #{edge.source_uuid}->#{edge.target_uuid} " <>
            "failed non-retryably on attempt #{attempt}: #{inspect(other)}"
        )
    end
  end

  defp converge_edges(edges, uuids, seed, maximum_rounds, label) do
    Enum.reduce_while(1..maximum_rounds, edges, fn round, current_edges ->
      next_edges = Enum.map(current_edges, &run_chaos_edge!(&1, seed))

      if exact_convergence?(uuids, seed) and checkpoints_aligned?(next_edges, seed) do
        {:halt, next_edges}
      else
        continue_or_fail(next_edges, round, maximum_rounds, seed, label)
      end
    end)
  end

  defp continue_or_fail(_edges, maximum_rounds, maximum_rounds, seed, label) do
    flunk("seed #{seed}: #{label} did not converge within #{maximum_rounds} rounds")
  end

  defp continue_or_fail(edges, _round, _maximum_rounds, _seed, _label),
    do: {:cont, edges}

  defp exact_convergence?(uuids, seed) do
    uuids
    |> Enum.map(&snapshot!(&1, seed).documents)
    |> Enum.uniq()
    |> length()
    |> Kernel.==(1)
  end

  defp checkpoints_aligned?(edges, seed) do
    Enum.all?(edges, fn edge ->
      replication_id = replication_id!(edge.source_uuid, edge.target_uuid, seed)
      expected = snapshot!(edge.source_uuid, seed).sequence
      source = checkpoint_state!(edge.source_uuid, replication_id, seed)
      target = checkpoint_state!(edge.target_uuid, replication_id, seed)
      source.source_sequence == expected and target.source_sequence == expected
    end)
  end

  defp checkpoint_state!(uuid, replication_id, seed) do
    record =
      DatabaseCatalog.command(
        uuid,
        {:command, :get_local_record, "checkpoints", replication_id}
      )
      |> ok!(seed, "read checkpoint #{replication_id}")

    value = MapAccess.get(record, :value)

    unless is_map(value) do
      flunk("seed #{seed}: checkpoint #{replication_id} is missing on #{uuid}")
    end

    %{
      record_version: MapAccess.get(record, :version),
      checkpoint_version: MapAccess.get(value, :checkpoint_version),
      protocol_version: MapAccess.get(value, :version),
      source_sequence: MapAccess.get(value, :source_sequence)
    }
  end

  defp replication_id!(source_uuid, target_uuid, seed) do
    Id.calculate(source_uuid, target_uuid, "push", "one_shot")
    |> ok!(seed, "calculate replication id")
  end

  defp seed_edge_checkpoints!(source_uuid, target_uuid, seed) do
    replication_id = replication_id!(source_uuid, target_uuid, seed)

    VialKeeper.TestReplicationCheckpoint.seed_matching_checkpoints!(
      source_uuid,
      target_uuid,
      replication_id
    )
  end

  defp revision_ids!(endpoint, document_id, leaves, seed) do
    leaf_revisions = Enum.map(leaves, &elem(&1, 0))

    %{chains: chains} =
      LocalEndpoint.get_revision_chains(endpoint, %{
        documents: [%{document_id: document_id, leaf_revisions: leaf_revisions}]
      })
      |> ok!(seed, "read revision chains for #{document_id}")

    chains
    |> Enum.flat_map(fn chain ->
      chain
      |> MapAccess.get(:revisions, [])
      |> Enum.map(fn revision ->
        MapAccess.get(revision, :revision) || MapAccess.get(revision, :revision_id)
      end)
    end)
    |> Enum.reject(&is_nil/1)
    |> MapSet.new()
  end

  defp sibling_chains(document_id, seed, label) do
    root_body = %{"seed" => seed, "role" => "root", "label" => label}
    left_body = %{"seed" => seed, "role" => "left", "label" => label}
    right_body = %{"seed" => seed, "role" => "right", "label" => label}

    root = revision!(document_id, nil, false, root_body, seed)
    left = revision!(document_id, root, false, left_body, seed)
    right = revision!(document_id, root, false, right_body, seed)

    [
      wire_chain(
        document_id,
        left,
        [
          AdapterCase.wire_revision(document_id, root, nil, false, root_body),
          AdapterCase.wire_revision(document_id, left, root, false, left_body)
        ]
      ),
      wire_chain(
        document_id,
        right,
        [
          AdapterCase.wire_revision(document_id, root, nil, false, root_body),
          AdapterCase.wire_revision(document_id, right, root, false, right_body)
        ]
      )
    ]
  end

  defp wire_chain(document_id, leaf_revision, revisions) do
    %{
      document_id: document_id,
      leaf_revision: leaf_revision,
      revisions: revisions
    }
  end

  defp revision!(document_id, parent, deleted, body, seed) do
    RevisionId.calculate(document_id, parent, deleted, body)
    |> ok!(seed, "calculate revision for #{document_id}")
  end

  defp update_mesh_range!(uuid, roots, range, seed, side) do
    Enum.each(range, fn index ->
      put!(
        uuid,
        mesh_id(index),
        Map.fetch!(roots, index),
        %{"seed" => seed, "side" => side, "version" => 2},
        seed
      )
    end)
  end

  defp delete_mesh_range!(uuid, roots, range, seed) do
    Enum.each(range, fn index ->
      delete!(uuid, mesh_id(index), Map.fetch!(roots, index), seed)
    end)
  end

  defp put!(uuid, id, parent, body, seed) do
    request = %{id: id, body: body}
    request = if parent, do: Map.put(request, :if_revision, parent), else: request

    uuid
    |> VialKeeper.Documents.put(request)
    |> ok!(seed, "put #{id}")
    |> Map.fetch!(:revision)
  end

  defp delete!(uuid, id, parent, seed) do
    VialKeeper.Documents.delete(uuid, %{id: id, if_revision: parent})
    |> ok!(seed, "delete #{id}")
    |> Map.fetch!(:revision)
  end

  defp run_concurrently!(callbacks, seed) do
    callbacks
    |> Task.async_stream(
      &run_writer/1,
      max_concurrency: length(callbacks),
      ordered: false,
      timeout: 30_000
    )
    |> Enum.each(fn
      {:ok, :ok} -> :ok
      {:ok, {:error, reason}} -> flunk("seed #{seed}: concurrent writer failed: #{reason}")
      {:exit, reason} -> flunk("seed #{seed}: concurrent writer exited: #{inspect(reason)}")
    end)
  end

  defp run_writer(callback) do
    callback.()
    :ok
  end

  defp local_endpoint!(uuid, seed) do
    LocalEndpoint.new(uuid)
    |> ok!(seed, "open local replication endpoint")
  end

  defp ok!({:ok, value}, _seed, _operation), do: value

  defp ok!(other, seed, operation) do
    flunk("seed #{seed}: #{operation} failed: #{inspect(other)}")
  end

  defp source_id(index), do: "source-#{index}"
  defp bidirectional_id(index), do: "shared-#{index}"
  defp mesh_id(index), do: "mesh-#{index}"
end
