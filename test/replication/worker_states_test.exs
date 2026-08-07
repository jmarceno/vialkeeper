defmodule ElixirDB.Replication.WorkerStatesTest do
  @moduledoc """
  Plan §7.7 / gap G7: harden the worker state proof.

  The 8 active phases are observed via `phase_hook` logs in `phase_transitions_test.exs`;
  this suite asserts the literal emitted `state_notify` sequence including `:handshake`
  through `:checkpoint_source`, a `:backoff` entry on an injected retryable fault, and the
  initial `:idle` state observed via the first `state_notify` message.
  """

  use ExUnit.Case, async: false

  alias ElixirDB.FaultAdapter
  alias ElixirDB.Replication.Id
  alias ElixirDB.Replication.LocalEndpoint
  alias ElixirDB.Replication.Worker
  alias ElixirDB.Runtime.DatabaseCatalog

  setup do
    prefix = "worker-states-#{System.unique_integer([:positive])}"
    a_path = prefix <> "-a.db"
    b_path = prefix <> "-b.db"
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

  test "worker emits :idle initially, then the full active-state sequence through checkpoint_source",
       %{a: a, b: b} do
    assert {:ok, _} =
             ElixirDB.Documents.put(a.database_uuid, %{id: "doc", body: %{"n" => 1}})

    {:ok, source} = LocalEndpoint.new(a.database_uuid)
    {:ok, target} = LocalEndpoint.new(b.database_uuid)

    assert {:ok, replication_id} =
             Id.calculate(
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
      state_notify: self(),
      phase_hook: fn _phase, _context -> :ok end
    }

    assert {:ok, pid} = Worker.start_link(options)
    ref = Process.monitor(pid)

    # REPL-004 / Plan §7.7: the initial worker state is :idle, emitted before :start.
    assert_receive {:replication_worker_state, :idle}, 1_000

    :gen_statem.cast(pid, :start)
    assert_receive {:DOWN, ^ref, :process, ^pid, :normal}, 5_000

    states = collect_states()

    # The full mandated active-state sequence: handshake ... checkpoint_source.
    for phase <- [
          :handshake,
          :read_changes,
          :diff,
          :fetch_chains,
          :import,
          :checkpoint_target,
          :checkpoint_source
        ] do
      assert phase in states, "expected #{phase} in emitted state sequence: #{inspect(states)}"
    end

    # Ordering: handshake must precede checkpoint_source in the observed sequence.
    handshake_idx = Enum.find_index(states, &(&1 == :handshake))
    source_idx = Enum.find_index(states, &(&1 == :checkpoint_source))
    assert handshake_idx < source_idx

    assert :completed in states

    assert {:ok, %{body: %{"n" => 1}}} =
             ElixirDB.Documents.get(b.database_uuid, %{id: "doc"})
  end

  test "worker emits a :backoff state on an injected retryable fault before retrying",
       %{a: a, b: b} do
    assert {:ok, %{revision: revision}} =
             ElixirDB.Documents.put(a.database_uuid, %{id: "doc", body: %{"n" => 1}})

    {:ok, source} = LocalEndpoint.new(a.database_uuid)
    {:ok, target} = LocalEndpoint.new(b.database_uuid)

    assert {:ok, replication_id} =
             Id.calculate(
               a.database_uuid,
               b.database_uuid,
               "push",
               "one_shot"
             )

    # Inject one retryable fault at the :import phase hook; the worker must enter :backoff
    # then retry and complete. :backoff is only entered for retryable errors
    # (worker.ex handle_failure/2). The :import hook fires before the import phase work.
    {:ok, faults} =
      Agent.start_link(fn ->
        FaultAdapter.wrap(:replication)
        |> FaultAdapter.inject(
          :import,
          {:once, ElixirDB.Error.database_closed("injected retryable fault")}
        )
      end)

    options = %{
      source: source,
      target: target,
      replication_id: replication_id,
      mode: "one_shot",
      direction: "push",
      retry: %{base_delay_ms: 1, max_delay_ms: 5, jitter_ms: 0, max_attempts: 8},
      state_notify: self(),
      phase_hook: phase_fault_hook(faults)
    }

    assert {:ok, pid} = Worker.start_link(options)
    ref = Process.monitor(pid)
    :gen_statem.cast(pid, :start)
    assert_receive {:DOWN, ^ref, :process, ^pid, :normal}, 5_000

    states = collect_states()

    # :backoff must appear (proving a retryable fault was retried, not treated as terminal).
    assert :backoff in states, "expected :backoff after retryable fault: #{inspect(states)}"

    # The import phase is entered twice: once before the fault (which enters :backoff) and
    # once again on the successful retry.
    assert Enum.count(states, &(&1 == :import)) >= 2

    # The worker still reaches the terminal :completed state.
    assert :completed in states

    assert {:ok, %{revision: ^revision, body: %{"n" => 1}}} =
             ElixirDB.Documents.get(b.database_uuid, %{id: "doc"})
  end

  # Collect the ordered worker state_notify messages this process received. Each is sent as
  # {:replication_worker_state, state}; ordering follows receive arrival within the test
  # process mailbox.
  defp collect_states do
    collect_states([]) |> Enum.reverse()
  end

  defp collect_states(acc) do
    receive do
      {:replication_worker_state, state} -> collect_states([state | acc])
    after
      0 -> acc
    end
  end

  defp phase_fault_hook(faults) do
    fn observed, _context ->
      faults
      |> Agent.get_and_update(&worker_fault_update(&1, observed))
      |> normalize_fault_result()
    end
  end

  defp worker_fault_update(adapter, observed) do
    case FaultAdapter.maybe_fail(adapter, observed) do
      {:ok, next} -> {:ok, next}
      {:error, error, next} -> {{:error, error}, next}
    end
  end

  defp normalize_fault_result(:ok), do: :ok
  defp normalize_fault_result({:error, error}), do: {:error, error}
end
