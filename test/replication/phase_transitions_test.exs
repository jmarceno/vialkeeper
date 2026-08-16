defmodule VialKeeper.Replication.PhaseTransitionsTest do
  @moduledoc "Covers replication worker state transitions and failure recovery."

  use ExUnit.Case, async: false

  @moduletag :integration

  alias VialKeeper.FaultAdapter
  alias VialKeeper.Replication.Id
  alias VialKeeper.Replication.LocalEndpoint
  alias VialKeeper.Replication.Worker
  alias VialKeeper.Runtime.DatabaseCatalog

  setup do
    prefix = "phases-#{System.unique_integer([:positive])}"
    a_path = prefix <> "-a.vialkeeper"
    b_path = prefix <> "-b.vialkeeper"

    for path <- [a_path, b_path] do
      VialKeeper.TempDatabase.cleanup(Path.join(VialKeeper.Config.database_root(), path))
    end

    {:ok, a} = DatabaseCatalog.create(a_path)
    {:ok, b} = DatabaseCatalog.create(b_path)

    on_exit(fn ->
      for {identity, path} <- [{a, a_path}, {b, b_path}] do
        _ = DatabaseCatalog.close(identity.database_uuid)
        _ = DatabaseCatalog.unregister(identity.database_uuid)
        VialKeeper.TempDatabase.cleanup(Path.join(VialKeeper.Config.database_root(), path))
      end
    end)

    {:ok, a: a, b: b}
  end

  test "worker emits mandated phases through checkpoint_source", %{a: a, b: b} do
    assert {:ok, _} =
             VialKeeper.Documents.put(a.database_uuid, %{id: "phases", body: %{"n" => 1}})

    {:ok, agent} = Agent.start_link(fn -> [] end)
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
      VialKeeper.TestReplicationCheckpoint.seed_matching_checkpoints!(
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
      phase_hook: fn phase, _context ->
        Agent.update(agent, &Enum.concat(&1, [phase]))
        :ok
      end
    }

    assert {:ok, pid} = Worker.start_link(options)
    ref = Process.monitor(pid)
    :gen_statem.cast(pid, :start)
    assert_receive {:DOWN, ^ref, :process, ^pid, :normal}, 5_000

    assert Agent.get(agent, & &1) == [
             :handshake,
             :after_handshake,
             :read_changes,
             :after_read_changes,
             :diff,
             :after_diff,
             :transfer,
             :before_chain_fetch,
             :after_chain_fetch,
             :after_transfer,
             :import,
             :after_import,
             :checkpoint_target,
             :after_checkpoint_target,
             :checkpoint_source,
             :after_checkpoint_source,
             :report_peer,
             :after_report_peer
           ]

    assert {:ok, %{body: %{"n" => 1}}} =
             VialKeeper.Documents.get(b.database_uuid, %{id: "phases"})
  end

  test "FaultAdapter can schedule a one-shot retryable failure without skipping work", %{
    a: a,
    b: b
  } do
    assert {:ok, %{revision: revision}} =
             VialKeeper.Documents.put(a.database_uuid, %{id: "fault", body: %{"n" => 1}})

    fault =
      FaultAdapter.wrap(:endpoint)
      |> FaultAdapter.inject(
        :before_import,
        {:once, VialKeeper.Error.database_closed("injected retryable fault")}
      )

    assert {:error, %VialKeeper.Error{code: :database_closed}, fault_after} =
             FaultAdapter.maybe_fail(fault, :before_import)

    assert fault_after.faults == %{}

    assert {:ok, _} = FaultAdapter.maybe_fail(fault_after, :before_import)

    assert {:ok, %{status: :completed}} =
             VialKeeper.Replication.one_shot(a.database_uuid, b.database_uuid)

    assert {:ok, %{revision: ^revision, body: %{"n" => 1}}} =
             VialKeeper.Documents.get(b.database_uuid, %{id: "fault"})
  end

  test "worker transfers cleanly when diff has no missing documents", %{a: a, b: b} do
    assert {:ok, %{revision: revision}} =
             VialKeeper.Documents.put(a.database_uuid, %{id: "already-present", body: %{"n" => 1}})

    {:ok, source} = LocalEndpoint.new(a.database_uuid)
    {:ok, target} = LocalEndpoint.new(b.database_uuid)

    assert {:ok, %{chains: chains}} =
             LocalEndpoint.get_revision_chains(source, %{
               documents: [%{document_id: "already-present", leaf_revisions: [revision]}]
             })

    assert {:ok, imported} =
             LocalEndpoint.import_revision_chains(target, %{chains: chains})

    assert {:ok, _} =
             LocalEndpoint.confirm_durable_commit(target, %{imported: imported})

    assert {:ok, replication_id} =
             Id.calculate(a.database_uuid, b.database_uuid, "push", "continuous")

    :ok =
      VialKeeper.TestReplicationCheckpoint.seed_matching_checkpoints!(
        a.database_uuid,
        b.database_uuid,
        replication_id
      )

    {:ok, seen} = Agent.start_link(fn -> [] end)

    options = %{
      source: source,
      target: target,
      replication_id: replication_id,
      mode: "continuous",
      direction: "push",
      wait_ms: 10,
      phase_hook: fn phase, _context ->
        Agent.update(seen, &Enum.uniq(Enum.concat(&1, [phase])))
        :ok
      end
    }

    assert {:ok, pid} = Worker.start_link(options)
    ref = Process.monitor(pid)
    :gen_statem.cast(pid, :start)

    VialKeeper.Eventual.eventually(
      fn ->
        phases = Agent.get(seen, & &1)
        :transfer in phases and :import in phases
      end,
      timeout: 5_000,
      message: "worker did not pass through transfer and import for an empty diff"
    )

    :gen_statem.cast(pid, :cancel)
    assert_receive {:DOWN, ^ref, :process, ^pid, :normal}, 5_000

    assert {:ok, %{revision: ^revision, body: %{"n" => 1}}} =
             VialKeeper.Documents.get(b.database_uuid, %{id: "already-present"})
  end
end
