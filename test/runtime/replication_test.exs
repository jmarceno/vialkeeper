defmodule ElixirDB.Runtime.ReplicationTest do
  use ExUnit.Case, async: false

  setup do
    prefix = "runtime-#{System.unique_integer([:positive])}"
    a_path = prefix <> "-a.db"
    b_path = prefix <> "-b.db"
    _ = File.rm(Path.join(ElixirDB.Config.database_root(), a_path))
    _ = File.rm(Path.join(ElixirDB.Config.database_root(), b_path))
    _ = File.rm(Path.join(ElixirDB.Config.database_root(), a_path <> ".lease"))
    _ = File.rm(Path.join(ElixirDB.Config.database_root(), b_path <> ".lease"))
    {:ok, a} = ElixirDB.Runtime.DatabaseCatalog.create(a_path)
    {:ok, b} = ElixirDB.Runtime.DatabaseCatalog.create(b_path)

    on_exit(fn ->
      for {identity, path} <- [{a, a_path}, {b, b_path}] do
        _ = ElixirDB.Runtime.DatabaseCatalog.close(identity.database_uuid)
        _ = ElixirDB.Runtime.DatabaseCatalog.unregister(identity.database_uuid)
        _ = File.rm(Path.join(ElixirDB.Config.database_root(), path))
        _ = File.rm(Path.join(ElixirDB.Config.database_root(), path <> ".lease"))
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

    assert :ok = ElixirDB.Runtime.DatabaseCatalog.close(a.database_uuid)
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
             ElixirDB.Replication.JobManager.put(a.database_uuid, %{
               "persist" => true,
               "mode" => "one_shot",
               "direction" => "push",
               "endpoint" => %{"kind" => "local", "database_uuid" => b.database_uuid},
               "enabled" => true
             })

    assert state in [:idle, :handshake, :read_changes, :diff, :fetch_chains, :import]

    assert :completed = wait_for_job(a.database_uuid, job_id, 50)

    assert {:ok, %{body: %{"ok" => true}}} =
             ElixirDB.Documents.get(b.database_uuid, %{id: "job-doc"})
  end

  test "worker reports mandated phase transitions through completed", %{a: a, b: b} do
    assert {:ok, _} =
             ElixirDB.Documents.put(a.database_uuid, %{id: "phases", body: %{"n" => 1}})

    {:ok, agent} = Agent.start_link(fn -> [] end)

    assert {:ok, source} = ElixirDB.Replication.LocalEndpoint.new(a.database_uuid)
    assert {:ok, target} = ElixirDB.Replication.LocalEndpoint.new(b.database_uuid)

    assert {:ok, replication_id} =
             ElixirDB.Replication.Id.calculate(
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
      phase_hook: fn phase, _context ->
        Agent.update(agent, &(&1 ++ [phase]))
        send(parent, {:phase, phase})
        :ok
      end
    }

    assert {:ok, pid} = ElixirDB.Replication.Worker.start_link(options)
    ref = Process.monitor(pid)
    :gen_statem.cast(pid, :start)

    assert_receive {:DOWN, ^ref, :process, ^pid, :normal}, 5_000

    phases = Agent.get(agent, & &1)

    assert phases == [
             :handshake,
             :read_changes,
             :diff,
             :fetch_chains,
             :import,
             :checkpoint_target,
             :checkpoint_source
           ]

    assert {:ok, %{body: %{"n" => 1}}} =
             ElixirDB.Documents.get(b.database_uuid, %{id: "phases"})
  end

  test "cancel between phases finishes current work without brutal_kill", %{a: a, b: b} do
    assert {:ok, _} =
             ElixirDB.Documents.put(a.database_uuid, %{id: "cancel-doc", body: %{"n" => 1}})

    assert {:ok, source} = ElixirDB.Replication.LocalEndpoint.new(a.database_uuid)
    assert {:ok, target} = ElixirDB.Replication.LocalEndpoint.new(b.database_uuid)

    assert {:ok, replication_id} =
             ElixirDB.Replication.Id.calculate(
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

    assert {:ok, pid} = ElixirDB.Replication.Worker.start_link(options)
    ref = Process.monitor(pid)
    :gen_statem.cast(pid, :start)

    assert_receive {:phase, :handshake}, 1_000
    assert_receive {:blocked, ^gate}, 1_000

    :gen_statem.cast(pid, :cancel)
    # Handshake task must still be alive — cancel must not brutal_kill mid-phase.
    assert Process.alive?(pid)

    release_handshake_task(gate)

    assert_receive {:DOWN, ^ref, :process, ^pid, :normal}, 5_000
    refute_receive {:phase, :read_changes}, 100

    # Cancel before read_changes means the target stays empty.
    assert {:error, %{code: :document_not_found}} =
             ElixirDB.Documents.get(b.database_uuid, %{id: "cancel-doc"})
  end

  defp release_handshake_task(gate) do
    ElixirDB.TaskSupervisor
    |> Task.Supervisor.children()
    |> Enum.each(fn pid -> send(pid, {:release, gate}) end)
  end

  defp wait_for_job(uuid, job_id, attempts) when attempts > 0 do
    case ElixirDB.Replication.JobManager.get(uuid, job_id) do
      {:ok, %{state: state}} when state in [:completed, :failed] ->
        state

      _ ->
        Process.sleep(20)
        wait_for_job(uuid, job_id, attempts - 1)
    end
  end

  defp wait_for_job(_uuid, _job_id, 0), do: :timeout
end
