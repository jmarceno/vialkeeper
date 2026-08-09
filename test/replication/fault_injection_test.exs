defmodule ElixirDB.Replication.FaultInjectionTest do
  @moduledoc """
  Plan §12.4 / gap B2: inject retryable failures before and after every phase transition.

  Core assertion: every injected retryable failure may repeat work but MUST NOT
  skip a committed source revision (exact leaf sets + full revision ids + checkpoints).
  """
  use ExUnit.Case, async: false

  alias ElixirDB.Changes
  alias ElixirDB.Documents
  alias ElixirDB.Error
  alias ElixirDB.Eventual
  alias ElixirDB.FaultAdapter
  alias ElixirDB.FaultEndpoint
  alias ElixirDB.MapAccess
  alias ElixirDB.Replication.{Id, LocalEndpoint, Worker}
  alias ElixirDB.Runtime.DatabaseCatalog
  alias ElixirDB.Storage.AdapterCase
  alias ElixirDB.TestRevisionId, as: RevisionId

  @phases [
    :handshake,
    :read_changes,
    :diff,
    :fetch_chains,
    :sync_blobs,
    :import,
    :checkpoint_target,
    :checkpoint_source
  ]

  @injection_points Enum.flat_map(@phases, fn phase ->
                      [phase, :"after_#{phase}"]
                    end)

  # Subset injected via FaultEndpoint (Plan §12.4 wrapper model). Checkpoint
  # after-faults stay on phase_hook because a successful put_checkpoint advances
  # CAS version before the after_* hook — endpoint after-faults would then CAS-fail.
  # Blob mid-transfer stages use dedicated endpoint points below.
  @endpoint_fault_points [
    :handshake,
    :after_handshake,
    :read_changes,
    :after_read_changes,
    :diff,
    :after_diff,
    :fetch_chains,
    :after_fetch_chains,
    :sync_blobs,
    :import,
    :after_import
  ]

  # Plan §22.6 blob sync stages. Mapped to phase_hook names fired by sync_blobs/4
  # plus Endpoint open_blob/put_blob wrappers.
  @blob_sync_fault_points [
    :sync_blobs,
    :after_diff_blobs,
    :before_blob_transfer,
    :after_open_blob,
    :after_put_blob,
    :after_sync_blobs
  ]

  setup do
    prefix = "fault-#{System.unique_integer([:positive])}"
    a_path = prefix <> "-a.elixirdb"
    b_path = prefix <> "-b.elixirdb"
    root = ElixirDB.Config.database_root()

    for path <- [a_path, b_path] do
      ElixirDB.TempDatabase.cleanup(Path.join(root, path))
    end

    {:ok, a} = DatabaseCatalog.create(a_path)
    {:ok, b} = DatabaseCatalog.create(b_path)

    on_exit(fn ->
      for {identity, path} <- [{a, a_path}, {b, b_path}] do
        _ = DatabaseCatalog.close(identity.database_uuid)
        _ = DatabaseCatalog.unregister(identity.database_uuid)
        ElixirDB.TempDatabase.cleanup(Path.join(root, path))
      end
    end)

    {:ok, a: a, b: b}
  end

  for point <- @injection_points do
    test "retryable fault at #{point} may repeat work but never skips source revision", %{
      a: a,
      b: b
    } do
      point = unquote(point)
      seed = :erlang.phash2({point, System.unique_integer([:positive])})
      _ = build_history(a.database_uuid, seed, point)

      source_snapshot = source_revision_snapshot(a.database_uuid)
      assert map_size(source_snapshot) >= 2
      source_sequence = source_sequence!(a.database_uuid)
      assert source_sequence >= 2

      # At least one conflicted two-leaf document must be present.
      assert Enum.any?(source_snapshot, fn {_id, meta} -> MapSet.size(meta.leaves) >= 2 end),
             "expected a conflicted multi-leaf document in source history"

      {:ok, seen} = Agent.start_link(fn -> [] end)

      assert {:ok, local_source} = LocalEndpoint.new(a.database_uuid)
      assert {:ok, local_target} = LocalEndpoint.new(b.database_uuid)

      {source, target, options} =
        if point in @endpoint_fault_points do
          {side, endpoint_point} = map_phase_to_endpoint(point)
          fault = {:once, retryable_fault(point)}

          source = FaultEndpoint.wrap(local_source)
          target = FaultEndpoint.wrap(local_target)

          {source, target} = inject_endpoint_fault(side, source, target, endpoint_point, fault)

          options = %{
            source: source,
            target: target,
            replication_id: nil,
            mode: "one_shot",
            direction: "push",
            retry: %{base_delay_ms: 5, max_delay_ms: 40, jitter_ms: 0, max_attempts: 8},
            phase_hook: phase_observer_hook(seen)
          }

          {source, target, options}
        else
          {:ok, faults} =
            Agent.start_link(fn ->
              FaultAdapter.wrap(:replication)
              |> FaultAdapter.inject(point, {:once, retryable_fault(point)})
            end)

          source = FaultEndpoint.wrap(local_source)
          target = FaultEndpoint.wrap(local_target)

          options = %{
            source: source,
            target: target,
            replication_id: nil,
            mode: "one_shot",
            direction: "push",
            retry: %{base_delay_ms: 5, max_delay_ms: 40, jitter_ms: 0, max_attempts: 8},
            phase_hook: phase_fault_hook(seen, faults)
          }

          {source, target, options}
        end

      assert {:ok, replication_id} =
               Id.calculate(
                 a.database_uuid,
                 b.database_uuid,
                 "push",
                 "one_shot"
               )

      :ok =
        ElixirDB.TestReplicationCheckpoint.seed_matching_checkpoints!(
          a.database_uuid,
          b.database_uuid,
          replication_id
        )

      options = %{options | replication_id: replication_id, source: source, target: target}

      assert {:ok, pid} = Worker.start_link(options)
      ref = Process.monitor(pid)
      :gen_statem.cast(pid, :start)
      assert_receive {:DOWN, ^ref, :process, ^pid, :normal}, 15_000

      phases = Agent.get(seen, & &1)
      parent = parent_phase(point)

      assert Enum.count(phases, &(&1 == parent)) >= 2,
             "expected phase #{parent} retried after #{point} fault; phases=#{inspect(phases)}"

      assert_no_skipped_revisions(
        a.database_uuid,
        b.database_uuid,
        replication_id,
        source_snapshot,
        source_sequence
      )
    end
  end

  for point <- @blob_sync_fault_points do
    test "blob sync fault at #{point} may repeat transfer but never skips revision", %{
      a: a,
      b: b
    } do
      point = unquote(point)
      payload = "blob-fault-#{System.unique_integer([:positive])}"

      assert {:ok, %{blob: digest, length: length}} =
               ElixirDB.Attachments.upload_stream(a.database_uuid, [payload])

      assert {:ok, %{revision: revision}} =
               ElixirDB.Documents.put(a.database_uuid, %{
                 id: "blob-doc",
                 body: %{"n" => 1},
                 attachments: %{
                   "note.bin" => %{blob: digest, content_type: "application/octet-stream"}
                 }
               })

      _ = length

      {:ok, seen} = Agent.start_link(fn -> [] end)

      {:ok, faults} =
        Agent.start_link(fn ->
          FaultAdapter.wrap(:replication)
          |> FaultAdapter.inject(point, {:once, retryable_fault(point)})
        end)

      assert {:ok, local_source} = LocalEndpoint.new(a.database_uuid)
      assert {:ok, local_target} = LocalEndpoint.new(b.database_uuid)
      source = FaultEndpoint.wrap(local_source)
      target = FaultEndpoint.wrap(local_target)

      assert {:ok, replication_id} =
               Id.calculate(a.database_uuid, b.database_uuid, "push", "one_shot")

      :ok =
        ElixirDB.TestReplicationCheckpoint.seed_matching_checkpoints!(
          a.database_uuid,
          b.database_uuid,
          replication_id
        )

      options = %{
        source: source,
        target: target,
        replication_id: replication_id,
        mode: "one_shot",
        direction: "push",
        retry: %{base_delay_ms: 5, max_delay_ms: 40, jitter_ms: 0, max_attempts: 8},
        phase_hook: phase_fault_hook(seen, faults)
      }

      assert {:ok, pid} = Worker.start_link(options)
      ref = Process.monitor(pid)
      :gen_statem.cast(pid, :start)
      assert_receive {:DOWN, ^ref, :process, ^pid, :normal}, 15_000

      phases = Agent.get(seen, & &1)

      assert point in phases,
             "expected fault point #{point} observed; phases=#{inspect(phases)}"

      assert :sync_blobs in phases
      assert :import in phases

      assert {:ok, got} = ElixirDB.Documents.get(b.database_uuid, %{id: "blob-doc"})
      assert got.revision == revision
      entry = Map.fetch!(got.attachments, "note.bin")
      assert MapAccess.get(entry, :digest) == digest

      assert {:ok, []} = ElixirDB.Attachments.diff_blobs(b.database_uuid, [digest])
    end
  end

  test "retryable fault at waiting/after_waiting never skips later source revision", %{
    a: a,
    b: b
  } do
    for point <- [:waiting, :after_waiting] do
      {:ok, faults} =
        Agent.start_link(fn ->
          FaultAdapter.wrap(:replication)
          |> FaultAdapter.inject(point, {:once, retryable_fault(point)})
        end)

      {:ok, seen} = Agent.start_link(fn -> [] end)

      assert {:ok, local_source} = LocalEndpoint.new(a.database_uuid)
      assert {:ok, local_target} = LocalEndpoint.new(b.database_uuid)
      source = FaultEndpoint.wrap(local_source)
      target = FaultEndpoint.wrap(local_target)

      assert {:ok, replication_id} =
               Id.calculate(
                 a.database_uuid,
                 b.database_uuid,
                 "push",
                 "continuous"
               )

      :ok =
        ElixirDB.TestReplicationCheckpoint.seed_matching_checkpoints!(
          a.database_uuid,
          b.database_uuid,
          replication_id
        )

      options = %{
        source: source,
        target: target,
        replication_id: replication_id,
        mode: "continuous",
        direction: "push",
        wait_ms: 30,
        retry: %{base_delay_ms: 5, max_delay_ms: 40, jitter_ms: 0, max_attempts: 8},
        phase_hook: phase_fault_hook(seen, faults)
      }

      assert {:ok, pid} = Worker.start_link(options)
      ref = Process.monitor(pid)
      :gen_statem.cast(pid, :start)

      Eventual.eventually(
        fn ->
          observed = Agent.get(seen, & &1)
          point in observed and Enum.count(observed, &(&1 == point)) >= 2
        end,
        timeout: 10_000,
        message: "#{point} was not retried after injected failure"
      )

      assert {:ok, %{revision: revision}} =
               Documents.put(a.database_uuid, %{
                 id: "after-#{point}",
                 body: %{"n" => 99, "point" => Atom.to_string(point)}
               })

      expected_sequence = source_sequence!(a.database_uuid)

      Eventual.eventually(
        fn ->
          case Documents.get(b.database_uuid, %{id: "after-#{point}"}) do
            {:ok, %{revision: ^revision, body: %{"n" => 99}}} -> true
            _ -> false
          end
        end,
        timeout: 10_000,
        message: "committed source revision after #{point} fault was skipped"
      )

      assert {:ok, target_ep} = LocalEndpoint.new(b.database_uuid)
      assert {:ok, source_ep} = LocalEndpoint.new(a.database_uuid)

      Eventual.eventually(
        fn ->
          with {:ok, %{value: target_cp}} <-
                 LocalEndpoint.get_checkpoint(target_ep, replication_id),
               {:ok, %{value: source_cp}} <-
                 LocalEndpoint.get_checkpoint(source_ep, replication_id) do
            target_seq = MapAccess.get(target_cp, :source_sequence)
            source_seq = MapAccess.get(source_cp, :source_sequence)
            target_seq == expected_sequence and source_seq == expected_sequence
          else
            _ -> false
          end
        end,
        timeout: 10_000,
        message:
          "checkpoints must equal source sequence #{expected_sequence} after #{point} recovery"
      )

      target_leaves = source_revision_snapshot(b.database_uuid)
      assert Map.has_key?(target_leaves, "after-#{point}")
      assert MapSet.member?(target_leaves["after-#{point}"].leaves, revision)

      :gen_statem.cast(pid, :cancel)
      assert_receive {:DOWN, ^ref, :process, ^pid, :normal}, 5_000
    end
  end

  test "FaultEndpoint injects after import_revision_chains without dropping revision", %{
    a: a,
    b: b
  } do
    seed = 42_001
    point = :after_import_revision_chains
    _ = build_history(a.database_uuid, seed, point)
    source_snapshot = source_revision_snapshot(a.database_uuid)
    source_sequence = source_sequence!(a.database_uuid)

    assert {:ok, local_source} = LocalEndpoint.new(a.database_uuid)
    assert {:ok, local_target} = LocalEndpoint.new(b.database_uuid)

    source = FaultEndpoint.wrap(local_source)

    target =
      FaultEndpoint.wrap(local_target)
      |> FaultEndpoint.inject(
        :after_import_revision_chains,
        {:once, retryable_fault(point)}
      )

    assert {:ok, replication_id} =
             Id.calculate(
               a.database_uuid,
               b.database_uuid,
               "push",
               "one_shot"
             )

    :ok =
      ElixirDB.TestReplicationCheckpoint.seed_matching_checkpoints!(
        a.database_uuid,
        b.database_uuid,
        replication_id
      )

    options = %{
      source: source,
      target: target,
      replication_id: replication_id,
      mode: "one_shot",
      direction: "push",
      retry: %{base_delay_ms: 5, max_delay_ms: 40, jitter_ms: 0, max_attempts: 8}
    }

    assert {:ok, pid} = Worker.start_link(options)
    ref = Process.monitor(pid)
    :gen_statem.cast(pid, :start)
    assert_receive {:DOWN, ^ref, :process, ^pid, :normal}, 15_000

    assert FaultEndpoint.hits(target)[:after_import_revision_chains] >= 1
    assert FaultEndpoint.pending_faults(target)[:after_import_revision_chains] == nil

    assert_no_skipped_revisions(
      a.database_uuid,
      b.database_uuid,
      replication_id,
      source_snapshot,
      source_sequence
    )
  end

  test "worker enters real :completed gen_statem state before stop", %{a: a, b: b} do
    assert {:ok, %{revision: revision}} =
             Documents.put(a.database_uuid, %{id: "terminal", body: %{"ok" => true}})

    assert {:ok, source} = LocalEndpoint.new(a.database_uuid)
    assert {:ok, target} = LocalEndpoint.new(b.database_uuid)

    assert {:ok, replication_id} =
             Id.calculate(
               a.database_uuid,
               b.database_uuid,
               "push",
               "one_shot"
             )

    :ok =
      ElixirDB.TestReplicationCheckpoint.seed_matching_checkpoints!(
        a.database_uuid,
        b.database_uuid,
        replication_id
      )

    parent = self()

    options = %{
      source: source,
      target: target,
      replication_id: replication_id,
      mode: "one_shot",
      direction: "push",
      state_notify: parent
    }

    assert {:ok, pid} = Worker.start_link(options)
    ref = Process.monitor(pid)
    :gen_statem.cast(pid, :start)

    assert_receive {:replication_worker_state, :completed}, 5_000
    assert_receive {:DOWN, ^ref, :process, ^pid, :normal}, 5_000

    assert {:ok, %{revision: ^revision, body: %{"ok" => true}}} =
             Documents.get(b.database_uuid, %{id: "terminal"})
  end

  test "worker enters real :failed gen_statem state when retries exhausted", %{a: a, b: b} do
    assert {:ok, _} =
             Documents.put(a.database_uuid, %{id: "fail-doc", body: %{"n" => 1}})

    {:ok, faults} =
      Agent.start_link(fn ->
        FaultAdapter.wrap(:replication)
        |> FaultAdapter.inject(
          :handshake,
          Error.database_closed("persistent handshake fault")
        )
      end)

    assert {:ok, source} = LocalEndpoint.new(a.database_uuid)
    assert {:ok, target} = LocalEndpoint.new(b.database_uuid)

    assert {:ok, replication_id} =
             Id.calculate(
               a.database_uuid,
               b.database_uuid,
               "push",
               "one_shot"
             )

    parent = self()

    options = %{
      source: source,
      target: target,
      replication_id: replication_id,
      mode: "one_shot",
      direction: "push",
      state_notify: parent,
      retry: %{base_delay_ms: 1, max_delay_ms: 5, jitter_ms: 0, max_attempts: 2},
      phase_hook: fn phase, _context ->
        Agent.get_and_update(faults, fn adapter ->
          case FaultAdapter.maybe_fail(adapter, phase) do
            {:ok, next} -> {:ok, next}
            {:error, error, next} -> {{:error, error}, next}
          end
        end)
      end
    }

    assert {:ok, pid} = Worker.start_link(options)
    ref = Process.monitor(pid)
    :gen_statem.cast(pid, :start)

    assert_receive {:replication_worker_state, :failed}, 5_000
    assert_receive {:DOWN, ^ref, :process, ^pid, :normal}, 5_000

    assert {:error, %{code: :document_not_found}} =
             Documents.get(b.database_uuid, %{id: "fail-doc"})
  end

  defp build_history(uuid, seed, point) do
    primary_id = "committed-#{point}-#{seed}"
    conflict_id = "conflict-#{point}-#{seed}"

    assert {:ok, %{revision: primary_rev}} =
             Documents.put(uuid, %{
               id: primary_id,
               body: %{"seed" => seed, "n" => 1, "role" => "primary"}
             })

    # Conflicted two-leaf document via sibling chain import.
    root_body = %{"seed" => seed, "role" => "root"}
    left_body = %{"seed" => seed, "side" => "left"}
    right_body = %{"seed" => seed, "side" => "right"}
    {:ok, root} = RevisionId.calculate(conflict_id, nil, false, root_body)
    {:ok, left} = RevisionId.calculate(conflict_id, root, false, left_body)
    {:ok, right} = RevisionId.calculate(conflict_id, root, false, right_body)

    assert {:ok, _} =
             DatabaseCatalog.command(uuid, {
               :command,
               :import_revision_chains,
               %{
                 chains: [
                   %{
                     document_id: conflict_id,
                     leaf_revision: left,
                     revisions: [
                       AdapterCase.wire_revision(conflict_id, root, nil, false, root_body),
                       AdapterCase.wire_revision(conflict_id, left, root, false, left_body)
                     ]
                   },
                   %{
                     document_id: conflict_id,
                     leaf_revision: right,
                     revisions: [
                       AdapterCase.wire_revision(conflict_id, root, nil, false, root_body),
                       AdapterCase.wire_revision(conflict_id, right, root, false, right_body)
                     ]
                   }
                 ]
               }
             })

    [
      %{id: primary_id, revision: primary_rev, role: :primary},
      %{id: conflict_id, leaves: [left, right], role: :conflict}
    ]
  end

  # Worker phase → FaultEndpoint injection point (side + endpoint callback name).
  defp map_phase_to_endpoint(:handshake), do: {:source, :identity}
  defp map_phase_to_endpoint(:after_handshake), do: {:source, :after_identity}
  defp map_phase_to_endpoint(:read_changes), do: {:source, :read_changes}
  defp map_phase_to_endpoint(:after_read_changes), do: {:source, :after_read_changes}
  defp map_phase_to_endpoint(:diff), do: {:target, :diff_revisions}
  defp map_phase_to_endpoint(:after_diff), do: {:target, :after_diff_revisions}
  defp map_phase_to_endpoint(:fetch_chains), do: {:source, :get_revision_chains}
  defp map_phase_to_endpoint(:after_fetch_chains), do: {:source, :after_get_revision_chains}
  defp map_phase_to_endpoint(:sync_blobs), do: {:target, :diff_blobs}
  defp map_phase_to_endpoint(:import), do: {:target, :import_revision_chains}
  defp map_phase_to_endpoint(:after_import), do: {:target, :after_import_revision_chains}

  # Injects the scheduled fault into the source or target endpoint based on `side`.
  # Hoisted into a helper so the compiler does not constant-fold `side` per unrolled test
  # (which produced "clause will never match" warnings on the case arms).
  defp inject_endpoint_fault(:source, source, target, endpoint_point, fault),
    do: {FaultEndpoint.inject(source, endpoint_point, fault), target}

  defp inject_endpoint_fault(:target, source, target, endpoint_point, fault),
    do: {source, FaultEndpoint.inject(target, endpoint_point, fault)}

  defp parent_phase(point) do
    case Atom.to_string(point) do
      "after_" <> rest -> String.to_existing_atom(rest)
      _ -> point
    end
  end

  defp phase_observer_hook(seen) do
    fn observed, _context ->
      Agent.update(seen, &(&1 ++ [observed]))
      :ok
    end
  end

  defp phase_fault_hook(seen, faults) do
    fn observed, _context ->
      Agent.update(seen, &(&1 ++ [observed]))
      Agent.get_and_update(faults, &phase_fault_update(&1, observed))
    end
  end

  defp phase_fault_update(adapter, observed) do
    case FaultAdapter.maybe_fail(adapter, observed) do
      {:ok, next} -> {:ok, next}
      {:error, error, next} -> {{:error, error}, next}
    end
  end

  defp source_revision_snapshot(uuid) do
    assert {:ok, %{results: results}} = Changes.read(uuid, %{since: 0, limit: 200})
    assert {:ok, ep} = LocalEndpoint.new(uuid)

    Map.new(results, fn change ->
      document_id = MapAccess.get(change, :document_id)

      leaves =
        MapAccess.get(change, :leaf_revisions, [])
        |> Enum.map(&MapAccess.get(&1, :revision))
        |> Enum.reject(&is_nil/1)
        |> MapSet.new()

      revision_ids = revision_ids_for(ep, document_id, MapSet.to_list(leaves))

      {document_id, %{leaves: leaves, revision_ids: revision_ids}}
    end)
  end

  defp revision_ids_for(ep, document_id, leaf_list) do
    case LocalEndpoint.get_revision_chains(ep, %{
           documents: [%{document_id: document_id, leaf_revisions: leaf_list}]
         }) do
      {:ok, %{chains: chains}} ->
        chains
        |> Enum.flat_map(&chain_revision_ids/1)
        |> Enum.reject(&is_nil/1)
        |> MapSet.new()

      _ ->
        MapSet.new(leaf_list)
    end
  end

  defp chain_revision_ids(chain) do
    chain
    |> MapAccess.get(:revisions, [])
    |> Enum.map(&revision_id/1)
  end

  defp revision_id(revision) do
    case MapAccess.get(revision, :revision, :missing) do
      :missing -> MapAccess.get(revision, :revision_id)
      value -> value
    end
  end

  defp source_sequence!(uuid) do
    assert {:ok, identity} =
             DatabaseCatalog.command(uuid, {:command, :identity, %{}})

    MapAccess.get(identity, :current_sequence)
  end

  defp assert_no_skipped_revisions(
         source_uuid,
         target_uuid,
         replication_id,
         source_snapshot,
         source_sequence
       ) do
    target_snapshot = source_revision_snapshot(target_uuid)

    for {document_id, source_meta} <- source_snapshot do
      target_meta = Map.fetch!(target_snapshot, document_id)

      assert target_meta.leaves == source_meta.leaves,
             "leaf set mismatch for #{document_id}: source=#{inspect(MapSet.to_list(source_meta.leaves))} target=#{inspect(MapSet.to_list(target_meta.leaves))}"

      assert MapSet.subset?(source_meta.revision_ids, target_meta.revision_ids),
             "target missing revisions for #{document_id}: missing=#{inspect(MapSet.difference(source_meta.revision_ids, target_meta.revision_ids) |> MapSet.to_list())}"

      assert {:ok, doc} = Documents.get(target_uuid, %{id: document_id})

      assert MapSet.member?(source_meta.leaves, doc.revision),
             "target winner #{doc.revision} for #{document_id} not in source leaves"
    end

    assert {:ok, source_ep} = LocalEndpoint.new(source_uuid)
    assert {:ok, target_ep} = LocalEndpoint.new(target_uuid)

    assert {:ok, %{value: source_cp}} =
             LocalEndpoint.get_checkpoint(source_ep, replication_id)

    assert {:ok, %{value: target_cp}} =
             LocalEndpoint.get_checkpoint(target_ep, replication_id)

    source_cp_seq = MapAccess.get(source_cp, :source_sequence)
    target_cp_seq = MapAccess.get(target_cp, :source_sequence)

    assert source_cp_seq == source_sequence,
           "source checkpoint #{inspect(source_cp_seq)} lagged source sequence #{source_sequence}"

    assert target_cp_seq == source_sequence,
           "target checkpoint #{inspect(target_cp_seq)} lagged source sequence #{source_sequence}"

    assert source_cp_seq == target_cp_seq

    target_seq = source_sequence!(target_uuid)
    assert target_seq >= source_sequence
  end

  defp retryable_fault(point) do
    Error.database_closed("injected retryable fault at #{point}")
  end
end
