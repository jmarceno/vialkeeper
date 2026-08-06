defmodule ElixirDB.Replication.FaultInjectionTest do
  @moduledoc """
  Plan §12.4 / gap B2: inject retryable failures at replication phase transitions.

  Core assertion: every injected retryable failure may repeat work but MUST NOT
  skip a committed source revision.
  """
  use ExUnit.Case, async: false

  alias ElixirDB.FaultAdapter
  alias ElixirDB.Runtime.DatabaseCatalog

  setup do
    prefix = "fault-#{System.unique_integer([:positive])}"
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

  for phase <- [:handshake, :read_changes, :import, :checkpoint_target] do
    @tag :slow
    test "retryable failure before #{phase} may repeat work but never skips source revision", %{
      a: a,
      b: b
    } do
      phase = unquote(phase)

      assert {:ok, %{revision: revision}} =
               ElixirDB.Documents.put(a.database_uuid, %{
                 id: "committed-#{phase}",
                 body: %{"phase" => Atom.to_string(phase), "n" => 1}
               })

      assert {:ok, _} =
               ElixirDB.Documents.put(a.database_uuid, %{
                 id: "sibling-#{phase}",
                 body: %{"phase" => Atom.to_string(phase), "n" => 2}
               })

      {:ok, faults} =
        Agent.start_link(fn ->
          FaultAdapter.wrap(:replication)
          |> FaultAdapter.inject(phase, {:once, retryable_fault(phase)})
        end)

      {:ok, seen} = Agent.start_link(fn -> [] end)

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
        retry: %{base_delay_ms: 20, max_delay_ms: 100, jitter_ms: 5, max_attempts: 8},
        phase_hook: fn observed, _context ->
          Agent.update(seen, &(&1 ++ [observed]))

          case Agent.get_and_update(faults, fn adapter ->
                 case FaultAdapter.maybe_fail(adapter, observed) do
                   {:ok, next} -> {:ok, next}
                   {:error, error, next} -> {{:error, error}, next}
                 end
               end) do
            :ok -> :ok
            {:error, error} -> {:error, error}
          end
        end
      }

      assert {:ok, pid} = ElixirDB.Replication.Worker.start_link(options)
      ref = Process.monitor(pid)
      :gen_statem.cast(pid, :start)
      assert_receive {:DOWN, ^ref, :process, ^pid, :normal}, 15_000

      phases = Agent.get(seen, & &1)
      assert phase in phases
      assert Enum.count(phases, &(&1 == phase)) >= 2

      assert {:ok, %{revision: ^revision, body: %{"n" => 1}}} =
               ElixirDB.Documents.get(b.database_uuid, %{id: "committed-#{phase}"})

      assert {:ok, %{body: %{"n" => 2}}} =
               ElixirDB.Documents.get(b.database_uuid, %{id: "sibling-#{phase}"})

      # Source sequence must still be fully represented on the target changes feed.
      assert {:ok, %{results: results}} =
               ElixirDB.Changes.read(b.database_uuid, %{since: 0, limit: 100})

      assert Enum.any?(results, &(&1.document_id == "committed-#{phase}"))
      assert Enum.any?(results, &(&1.document_id == "sibling-#{phase}"))
    end
  end

  @tag :slow
  test "retryable failure at waiting may repeat work but never skips later source revision", %{
    a: a,
    b: b
  } do
    {:ok, faults} =
      Agent.start_link(fn ->
        FaultAdapter.wrap(:replication)
        |> FaultAdapter.inject(:waiting, {:once, retryable_fault(:waiting)})
      end)

    {:ok, seen} = Agent.start_link(fn -> [] end)

    assert {:ok, source} = ElixirDB.Replication.LocalEndpoint.new(a.database_uuid)
    assert {:ok, target} = ElixirDB.Replication.LocalEndpoint.new(b.database_uuid)

    assert {:ok, replication_id} =
             ElixirDB.Replication.Id.calculate(
               a.database_uuid,
               b.database_uuid,
               "push",
               "continuous"
             )

    options = %{
      source: source,
      target: target,
      replication_id: replication_id,
      mode: "continuous",
      direction: "push",
      wait_ms: 50,
      retry: %{base_delay_ms: 20, max_delay_ms: 100, jitter_ms: 5, max_attempts: 8},
      phase_hook: fn observed, _context ->
        Agent.update(seen, &(&1 ++ [observed]))

        case Agent.get_and_update(faults, fn adapter ->
               case FaultAdapter.maybe_fail(adapter, observed) do
                 {:ok, next} -> {:ok, next}
                 {:error, error, next} -> {{:error, error}, next}
               end
             end) do
          :ok -> :ok
          {:error, error} -> {:error, error}
        end
      end
    }

    assert {:ok, pid} = ElixirDB.Replication.Worker.start_link(options)
    ref = Process.monitor(pid)
    :gen_statem.cast(pid, :start)

    ElixirDB.Eventual.eventually(
      fn ->
        :waiting in Agent.get(seen, & &1) and
          Enum.count(Agent.get(seen, & &1), &(&1 == :waiting)) >= 2
      end,
      timeout: 10_000,
      message: "waiting phase was not retried after injected failure"
    )

    assert {:ok, %{revision: revision}} =
             ElixirDB.Documents.put(a.database_uuid, %{
               id: "after-wait",
               body: %{"n" => 99}
             })

    ElixirDB.Eventual.eventually(
      fn ->
        case ElixirDB.Documents.get(b.database_uuid, %{id: "after-wait"}) do
          {:ok, %{revision: ^revision, body: %{"n" => 99}}} -> true
          _ -> false
        end
      end,
      timeout: 10_000,
      message: "committed source revision after waiting fault was skipped"
    )

    :gen_statem.cast(pid, :cancel)
    assert_receive {:DOWN, ^ref, :process, ^pid, :normal}, 5_000
  end

  @tag :slow
  test "failure after import (at checkpoint_target) still retains imported revision", %{
    a: a,
    b: b
  } do
    assert {:ok, %{revision: revision}} =
             ElixirDB.Documents.put(a.database_uuid, %{
               id: "post-import",
               body: %{"kept" => true}
             })

    {:ok, faults} =
      Agent.start_link(fn ->
        FaultAdapter.wrap(:replication)
        |> FaultAdapter.inject(
          :checkpoint_target,
          {:once, retryable_fault(:checkpoint_target)}
        )
      end)

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
      retry: %{base_delay_ms: 20, max_delay_ms: 100, jitter_ms: 5, max_attempts: 8},
      phase_hook: fn observed, _context ->
        case Agent.get_and_update(faults, fn adapter ->
               case FaultAdapter.maybe_fail(adapter, observed) do
                 {:ok, next} -> {:ok, next}
                 {:error, error, next} -> {{:error, error}, next}
               end
             end) do
          :ok -> :ok
          {:error, error} -> {:error, error}
        end
      end
    }

    assert {:ok, pid} = ElixirDB.Replication.Worker.start_link(options)
    ref = Process.monitor(pid)
    :gen_statem.cast(pid, :start)
    assert_receive {:DOWN, ^ref, :process, ^pid, :normal}, 15_000

    assert {:ok, %{revision: ^revision, body: %{"kept" => true}}} =
             ElixirDB.Documents.get(b.database_uuid, %{id: "post-import"})
  end

  defp retryable_fault(phase) do
    ElixirDB.Error.database_closed("injected retryable fault at #{phase}")
  end
end
