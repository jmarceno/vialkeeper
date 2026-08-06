defmodule ElixirDB.Replication.PhaseTransitionsTest do
  use ExUnit.Case, async: false

  alias ElixirDB.FaultAdapter
  alias ElixirDB.Runtime.DatabaseCatalog

  setup do
    prefix = "phases-#{System.unique_integer([:positive])}"
    a_path = prefix <> "-a.db"
    b_path = prefix <> "-b.db"

    for path <- [a_path, b_path] do
      _ = File.rm(Path.join(ElixirDB.Config.database_root(), path))
      _ = File.rm(Path.join(ElixirDB.Config.database_root(), path <> ".lease"))
    end

    {:ok, a} = DatabaseCatalog.create(a_path)
    {:ok, b} = DatabaseCatalog.create(b_path)

    on_exit(fn ->
      for {identity, path} <- [{a, a_path}, {b, b_path}] do
        _ = DatabaseCatalog.close(identity.database_uuid)
        _ = DatabaseCatalog.unregister(identity.database_uuid)
        _ = File.rm(Path.join(ElixirDB.Config.database_root(), path))
        _ = File.rm(Path.join(ElixirDB.Config.database_root(), path <> ".lease"))
      end
    end)

    {:ok, a: a, b: b}
  end

  test "worker emits mandated phases through checkpoint_source", %{a: a, b: b} do
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

    options = %{
      source: source,
      target: target,
      replication_id: replication_id,
      mode: "one_shot",
      direction: "push",
      phase_hook: fn phase, _context ->
        Agent.update(agent, &(&1 ++ [phase]))
        :ok
      end
    }

    assert {:ok, pid} = ElixirDB.Replication.Worker.start_link(options)
    ref = Process.monitor(pid)
    :gen_statem.cast(pid, :start)
    assert_receive {:DOWN, ^ref, :process, ^pid, :normal}, 5_000

    assert Agent.get(agent, & &1) == [
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

  test "FaultAdapter can schedule a one-shot retryable failure without skipping work", %{
    a: a,
    b: b
  } do
    assert {:ok, %{revision: revision}} =
             ElixirDB.Documents.put(a.database_uuid, %{id: "fault", body: %{"n" => 1}})

    fault =
      FaultAdapter.wrap(:endpoint)
      |> FaultAdapter.inject(
        :before_import,
        {:once, ElixirDB.Error.database_closed("injected retryable fault")}
      )

    assert {:error, %ElixirDB.Error{code: :database_closed}, fault_after} =
             FaultAdapter.maybe_fail(fault, :before_import)

    assert fault_after.faults == %{}

    assert {:ok, _} = FaultAdapter.maybe_fail(fault_after, :before_import)

    assert {:ok, %{status: :completed}} =
             ElixirDB.Replication.one_shot(a.database_uuid, b.database_uuid)

    assert {:ok, %{revision: ^revision, body: %{"n" => 1}}} =
             ElixirDB.Documents.get(b.database_uuid, %{id: "fault"})
  end
end
