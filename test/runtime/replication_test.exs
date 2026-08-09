defmodule ElixirDB.Runtime.ReplicationTest do
  alias ElixirDB.Replication.Id
  alias ElixirDB.Replication.JobManager
  alias ElixirDB.Replication.LocalEndpoint
  alias ElixirDB.Replication.Worker
  alias ElixirDB.Runtime.DatabaseCatalog
  use ExUnit.Case, async: false

  setup do
    prefix = "runtime-#{System.unique_integer([:positive])}"
    a_path = prefix <> "-a.elixirdb"
    b_path = prefix <> "-b.elixirdb"
    ElixirDB.TempDatabase.cleanup(Path.join(ElixirDB.Config.database_root(), a_path))
    ElixirDB.TempDatabase.cleanup(Path.join(ElixirDB.Config.database_root(), b_path))
    {:ok, a} = DatabaseCatalog.create(a_path)
    {:ok, b} = DatabaseCatalog.create(b_path)

    on_exit(fn ->
      for {identity, path} <- [{a, a_path}, {b, b_path}] do
        _ = DatabaseCatalog.close(identity.database_uuid)
        _ = DatabaseCatalog.unregister(identity.database_uuid)
        ElixirDB.TempDatabase.cleanup(Path.join(ElixirDB.Config.database_root(), path))
      end
    end)

    {:ok, a: a, b: b}
  end

  test "runtime owns writes and local replication converges", %{a: a, b: b} do
    assert {:ok, %{revision: revision}} =
             ElixirDB.Documents.put(a.database_uuid, %{id: "doc", body: %{"n" => 1}})

    assert {:ok, %{status: :completed}} =
             ElixirDB.Replication.one_shot(a.database_uuid, b.database_uuid)

    assert {:ok, %{revision: ^revision, body: %{"n" => 1}}} =
             ElixirDB.Documents.get(b.database_uuid, %{id: "doc"})

    assert :ok = DatabaseCatalog.close(a.database_uuid)
  end

  @tag :slow
  test "one-shot replication reuses durable checkpoints and transfers later changes", %{a: a, b: b} do
    assert {:ok, %{revision: first}} =
             ElixirDB.Documents.put(a.database_uuid, %{id: "doc", body: %{"n" => 1}})

    assert {:ok, %{status: :completed}} =
             ElixirDB.Replication.one_shot(a.database_uuid, b.database_uuid)

    assert {:ok, %{status: :completed}} =
             ElixirDB.Replication.one_shot(a.database_uuid, b.database_uuid)

    assert {:ok, %{revision: second}} =
             ElixirDB.Documents.put(a.database_uuid, %{
               id: "doc",
               if_revision: first,
               body: %{"n" => 2}
             })

    assert second != first

    assert {:ok, %{status: :completed}} =
             ElixirDB.Replication.one_shot(a.database_uuid, b.database_uuid)

    assert {:ok, %{revision: ^second, body: %{"n" => 2}}} =
             ElixirDB.Documents.get(b.database_uuid, %{id: "doc"})
  end

  test "persistent one-shot jobs report terminal state and converge", %{a: a, b: b} do
    assert {:ok, _} =
             ElixirDB.Documents.put(a.database_uuid, %{id: "job-doc", body: %{"ok" => true}})

    assert {:ok, %{job_id: job_id, state: state}} =
             JobManager.put(a.database_uuid, %{
               "persist" => true,
               "mode" => "one_shot",
               "direction" => "push",
               "endpoint" => %{"kind" => "local", "database_uuid" => b.database_uuid},
               "enabled" => true
             })

    assert state in [
             :idle,
             :handshake,
             :read_changes,
             :diff,
             :transfer,
             :import
           ]

    assert :completed = wait_for_job(a.database_uuid, job_id)

    assert {:ok, %{body: %{"ok" => true}}} =
             ElixirDB.Documents.get(b.database_uuid, %{id: "job-doc"})
  end

  test "job transfer options cannot exceed host ceilings", %{a: a, b: b} do
    base = %{
      "persist" => false,
      "mode" => "one_shot",
      "direction" => "push",
      "endpoint" => %{"kind" => "local", "database_uuid" => b.database_uuid}
    }

    for {key, value} <- [
          {"max_concurrent_chain_fetches", 33},
          {"max_concurrent_blob_transfers", 33},
          {"max_transfer_bytes_in_flight", 4_294_967_297},
          {"batch_documents", 501}
        ] do
      assert {:error, %ElixirDB.Error{code: :invalid_request}} =
               JobManager.put(a.database_uuid, Map.put(base, key, value))
    end
  end

  test "continuous job state survives the caller process exiting", %{a: a, b: b} do
    task =
      Task.async(fn ->
        JobManager.put(a.database_uuid, %{
          "persist" => true,
          "mode" => "continuous",
          "direction" => "push",
          "endpoint" => %{"kind" => "local", "database_uuid" => b.database_uuid},
          "enabled" => true,
          "wait_ms" => 100
        })
      end)

    assert {:ok, %{job_id: job_id}} = Task.await(task, 5_000)

    ElixirDB.Eventual.eventually(
      fn ->
        case JobManager.get(a.database_uuid, job_id) do
          {:ok, %{state: state}} when state in [:waiting, :backoff] -> true
          _ -> false
        end
      end,
      timeout: 5_000,
      message: "continuous job state was lost when its caller exited"
    )

    assert {:ok, %{state: state}} = JobManager.get(a.database_uuid, job_id)
    assert state in [:waiting, :backoff]

    assert {:ok, %{state: :disabled}} =
             JobManager.disable(a.database_uuid, job_id)
  end

  test "disabling a continuous job waits for its worker before close", %{a: a, b: b} do
    assert {:ok, %{job_id: job_id}} =
             JobManager.put(a.database_uuid, %{
               "persist" => true,
               "mode" => "continuous",
               "direction" => "push",
               "endpoint" => %{"kind" => "local", "database_uuid" => b.database_uuid},
               "enabled" => true,
               "wait_ms" => 100
             })

    ElixirDB.Eventual.eventually(
      fn ->
        case JobManager.get(a.database_uuid, job_id) do
          {:ok, %{state: state}} when state in [:waiting, :backoff] -> true
          _ -> false
        end
      end,
      timeout: 5_000,
      message: "continuous job did not reach a cancellable state"
    )

    assert {:ok, %{state: :disabled}} =
             JobManager.disable(a.database_uuid, job_id)

    assert {:ok, %{state: :disabled}} =
             JobManager.get(a.database_uuid, job_id)

    assert :ok = DatabaseCatalog.close(a.database_uuid)
  end

  test "cancelling a continuous job waits for its worker before close", %{a: a, b: b} do
    assert {:ok, %{job_id: job_id}} =
             JobManager.put(a.database_uuid, %{
               "persist" => true,
               "mode" => "continuous",
               "direction" => "push",
               "endpoint" => %{"kind" => "local", "database_uuid" => b.database_uuid},
               "enabled" => true,
               "wait_ms" => 100
             })

    ElixirDB.Eventual.eventually(
      fn ->
        case JobManager.get(a.database_uuid, job_id) do
          {:ok, %{state: state}} when state in [:waiting, :backoff] -> true
          _ -> false
        end
      end,
      timeout: 5_000,
      message: "continuous job did not reach a cancellable state"
    )

    assert {:ok, %{state: :failed}} =
             JobManager.cancel(job_id)

    assert {:ok, %{state: :failed}} = JobManager.get(a.database_uuid, job_id)
    assert :ok = DatabaseCatalog.close(a.database_uuid)
  end

  test "worker reports mandated phase transitions through completed", %{a: a, b: b} do
    assert {:ok, _} =
             ElixirDB.Documents.put(a.database_uuid, %{id: "phases", body: %{"n" => 1}})

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
      phase_hook: fn phase, _context ->
        Agent.update(agent, &(&1 ++ [phase]))
        send(parent, {:phase, phase})
        :ok
      end
    }

    assert {:ok, pid} = Worker.start_link(options)
    ref = Process.monitor(pid)
    :gen_statem.cast(pid, :start)

    assert_receive {:DOWN, ^ref, :process, ^pid, :normal}, 5_000

    phases = Agent.get(agent, & &1)

    assert phases == [
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
             ElixirDB.Documents.get(b.database_uuid, %{id: "phases"})
  end

  test "cancel between phases finishes current work without brutal_kill", %{a: a, b: b} do
    assert {:ok, _} =
             ElixirDB.Documents.put(a.database_uuid, %{id: "cancel-doc", body: %{"n" => 1}})

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
    gate = make_ref()

    options = %{
      source: source,
      target: target,
      replication_id: replication_id,
      mode: "one_shot",
      direction: "push",
      phase_hook: fn phase, _context ->
        send(parent, {:phase, phase})

        if phase == :handshake do
          send(parent, {:blocked, gate})

          receive do
            {:release, ^gate} -> :ok
          after
            5_000 -> :ok
          end
        else
          :ok
        end
      end
    }

    assert {:ok, pid} = Worker.start_link(options)
    ref = Process.monitor(pid)
    :gen_statem.cast(pid, :start)

    assert_receive {:phase, :handshake}, 1_000
    assert_receive {:blocked, ^gate}, 1_000

    :gen_statem.cast(pid, :cancel)
    # Handshake task must still be alive — cancel must not brutal_kill mid-phase.
    assert Process.alive?(pid)

    release_handshake_task(gate)

    assert_receive {:DOWN, ^ref, :process, ^pid, :normal}, 5_000
    refute_receive {:phase, :read_changes}, 0

    # Cancel before read_changes means the target stays empty.
    assert {:error, %{code: :document_not_found}} =
             ElixirDB.Documents.get(b.database_uuid, %{id: "cancel-doc"})
  end

  defp release_handshake_task(gate) do
    ElixirDB.TaskSupervisor
    |> Task.Supervisor.children()
    |> Enum.each(fn pid -> send(pid, {:release, gate}) end)
  end

  defp wait_for_job(uuid, job_id) do
    ElixirDB.Eventual.eventually(
      fn ->
        case JobManager.get(uuid, job_id) do
          {:ok, %{state: state}} when state in [:completed, :failed] -> {:ok, state}
          _ -> false
        end
      end,
      timeout: 5_000,
      message: "persistent replication job did not reach a terminal state"
    )
  end
end
