defmodule ElixirDB.Runtime.CatalogAdmissionRoutingTest do
  @moduledoc """
  Proves per-database admission waiting happens outside the host-global catalog
  GenServer so independent databases remain concurrently schedulable.
  """
  use ExUnit.Case, async: false

  @moduletag :integration

  alias ElixirDB.Eventual
  alias ElixirDB.Runtime.{DatabaseAdmission, DatabaseCatalog}

  setup do
    previous_limits = Application.get_env(:elixir_db, :host_limits)
    previous_policy = Application.get_env(:elixir_db, :admission_policy)

    limits = Keyword.put(previous_limits || [], :admission_limit, 8)

    policy = [
      foreground_weight: 8,
      subscription_weight: 4,
      replication_weight: 2,
      maintenance_weight: 1,
      foreground_reserved_slots: 0,
      subscription_reserved_slots: 0,
      replication_reserved_slots: 0,
      maintenance_reserved_slots: 0
    ]

    Application.put_env(:elixir_db, :host_limits, limits)
    Application.put_env(:elixir_db, :admission_policy, policy)

    a_rel = "catalog-route-a-#{System.unique_integer([:positive])}.elixirdb"
    b_rel = "catalog-route-b-#{System.unique_integer([:positive])}.elixirdb"
    root = ElixirDB.Config.database_root()
    a_abs = Path.join(root, a_rel)
    b_abs = Path.join(root, b_rel)

    for path <- [a_abs, b_abs] do
      ElixirDB.TempDatabase.cleanup(path)
    end

    assert {:ok, a} = DatabaseCatalog.create(a_rel)
    assert {:ok, b} = DatabaseCatalog.create(b_rel)
    assert {:ok, _} = DatabaseCatalog.open(a.database_uuid)
    assert {:ok, _} = DatabaseCatalog.open(b.database_uuid)

    on_exit(fn ->
      Application.delete_env(:elixir_db, :admitted_command_sync)
      Application.put_env(:elixir_db, :host_limits, previous_limits)
      Application.put_env(:elixir_db, :admission_policy, previous_policy)

      for identity <- [a, b] do
        _ = DatabaseCatalog.close(identity.database_uuid)
        _ = DatabaseCatalog.unregister(identity.database_uuid)
      end

      for path <- [a_abs, b_abs] do
        ElixirDB.TempDatabase.cleanup(path)
      end
    end)

    {:ok, uuid_a: a.database_uuid, uuid_b: b.database_uuid}
  end

  test "ensure_command_target opens lazy runtime without executing commands", %{uuid_a: uuid} do
    assert :ok = DatabaseCatalog.close(uuid)
    assert [] = Registry.lookup(ElixirDB.Runtime.DatabaseRegistry, {:owner, uuid})

    assert :ok = DatabaseCatalog.ensure_command_target(uuid)

    assert [{_pid, :ordinary}] =
             Registry.lookup(ElixirDB.Runtime.DatabaseRegistry, {:owner, uuid})
  end

  test "ordinary_open? reads owner registry metadata without listing the catalog", %{
    uuid_a: uuid
  } do
    assert DatabaseCatalog.ordinary_open?(uuid)
    assert :ok = DatabaseCatalog.close(uuid)
    refute DatabaseCatalog.ordinary_open?(uuid)
  end

  test "command_as routes through the requested admission class", %{uuid_a: uuid} do
    assert {:ok, identity} =
             DatabaseCatalog.command_as(uuid, :subscription, {:command, :identity, %{}})

    assert identity.database_uuid == uuid
  end

  test "blocked database A admission queue does not block database B catalog commands", %{
    uuid_a: uuid_a,
    uuid_b: uuid_b
  } do
    parent = self()
    gate_ref = make_ref()
    :ok = Application.put_env(:elixir_db, :admitted_command_sync, {parent, gate_ref, uuid_a})

    blocker =
      Task.async(fn ->
        DatabaseCatalog.command(uuid_a, {:command, :identity, %{}})
      end)

    assert_receive {^gate_ref, :before_begin, blocker_executor}, 2_000

    queued_classes = [:subscription, :replication, :maintenance, :foreground]

    queued =
      Enum.map(queued_classes, fn class ->
        Task.async(fn ->
          result =
            DatabaseCatalog.command_as(uuid_a, class, {:command, :identity, %{}})

          send(parent, {:a_ran, class})
          result
        end)
      end)

    await_a_stats(
      fn stats ->
        stats.queued_subscription >= 1 and stats.queued_replication >= 1 and
          stats.queued_maintenance >= 1 and stats.queued_foreground >= 1
      end,
      uuid_a
    )

    b_done_ref = make_ref()

    b_task =
      Task.async(fn ->
        result = DatabaseCatalog.command(uuid_b, {:command, :identity, %{}})
        send(parent, {:b_done, b_done_ref, result})
        result
      end)

    assert_receive {:b_done, ^b_done_ref, {:ok, identity}}, 2_000
    assert identity.database_uuid == uuid_b
    refute_receive {:a_ran, _}, 0

    send(blocker_executor, {:go, gate_ref})
    Application.delete_env(:elixir_db, :admitted_command_sync)

    assert {:ok, _} = Task.await(blocker, 2_000)
    assert {:ok, _} = Task.await(b_task, 2_000)

    for task <- queued do
      assert {:ok, _} = Task.await(task, 2_000)
    end

    for class <- queued_classes do
      assert_receive {:a_ran, ^class}, 2_000
    end

    await_a_stats(&(&1.total_occupancy == 0), uuid_a)
  end

  defp await_a_stats(predicate, uuid) do
    Eventual.eventually(fn -> poll_a_stats(uuid, predicate) end, timeout: 2_000)
  end

  defp poll_a_stats(uuid, predicate) do
    case DatabaseAdmission.stats(uuid) do
      {:ok, stats} ->
        if predicate.(stats), do: {:ok, stats}, else: false

      _ ->
        false
    end
  end
end
