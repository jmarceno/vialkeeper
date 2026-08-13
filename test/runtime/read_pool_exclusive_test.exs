defmodule ElixirDB.Runtime.ReadPoolExclusiveTest do
  @moduledoc """
  Exclusive commands drain active snapshots and hold the pool quiesced before
  the owner body runs, so no reader overlaps exclusive IO.
  """
  use ExUnit.Case, async: false

  alias ElixirDB.Eventual
  alias ElixirDB.Runtime.{DatabaseCatalog, ReadPool}
  alias ElixirDB.View.Manager

  setup do
    previous_limits = Application.get_env(:elixir_db, :host_limits)

    limits =
      (previous_limits || [])
      |> Keyword.put(:read_pool_size, 1)
      |> Keyword.put(:read_queue_limit, 8)

    Application.put_env(:elixir_db, :host_limits, limits)

    relative = "read-pool-excl-#{System.unique_integer([:positive])}.elixirdb"
    absolute = Path.join(ElixirDB.Config.database_root(), relative)
    ElixirDB.TempDatabase.cleanup(absolute)

    assert {:ok, identity} = DatabaseCatalog.create(relative)
    assert {:ok, _} = DatabaseCatalog.open(identity.database_uuid)
    assert :ok = Manager.await_resumed(identity.database_uuid)

    on_exit(fn ->
      Application.delete_env(:elixir_db, :read_pool_sync)
      Application.delete_env(:elixir_db, :read_pool_probe)
      Application.delete_env(:elixir_db, :admitted_command_owner_body_sync)
      Application.put_env(:elixir_db, :host_limits, previous_limits)
      _ = DatabaseCatalog.close(identity.database_uuid)
      _ = DatabaseCatalog.unregister(identity.database_uuid)
      ElixirDB.TempDatabase.cleanup(absolute)
    end)

    {:ok, uuid: identity.database_uuid}
  end

  test "snapshots complete before exclusive owner body", %{uuid: uuid} do
    assert {:ok, _} = ElixirDB.Documents.put(uuid, %{id: "doc", body: %{"n" => 1}})

    gate = make_ref()
    owner_gate = make_ref()
    probe = make_ref()
    Application.put_env(:elixir_db, :read_pool_sync, {self(), gate, uuid})
    Application.put_env(:elixir_db, :read_pool_probe, {self(), probe})
    Application.put_env(:elixir_db, :admitted_command_owner_body_sync, {self(), owner_gate, uuid})

    holder =
      Task.async(fn ->
        ElixirDB.Documents.get(uuid, %{id: "doc"})
      end)

    assert_receive {^gate, :before_begin, worker}, 2_000
    _ = drain_grants(probe)

    exclusive =
      Task.async(fn ->
        DatabaseCatalog.command(uuid, {:command, :integrity_check, %{}})
      end)

    refute_receive {^owner_gate, :owner_body, _}, 200

    Application.delete_env(:elixir_db, :read_pool_sync)
    send(worker, {:go, gate})
    assert {:ok, %{id: "doc"}} = Task.await(holder, 5_000)

    assert_receive {^owner_gate, :owner_body, owner}, 2_000

    Eventual.eventually(
      fn ->
        case ReadPool.stats(uuid) do
          {:ok, %{active: 0, quiescing?: true}} -> true
          _ -> false
        end
      end,
      timeout: 2_000,
      message: "exclusive must hold the pool quiesced with no active snapshots"
    )

    waiter =
      Task.async(fn ->
        ElixirDB.Documents.get(uuid, %{id: "doc"})
      end)

    Eventual.eventually(
      fn ->
        case ReadPool.stats(uuid) do
          {:ok, %{active: 0, queued: queued, quiescing?: true}} when queued >= 1 -> true
          _ -> false
        end
      end,
      timeout: 2_000,
      message: "a get queued during exclusive waits until exclusive releases"
    )

    grants_during_exclusive = drain_grants(probe)

    assert grants_during_exclusive == [],
           "no snapshot grant may happen mid-exclusive, got #{inspect(grants_during_exclusive)}"

    send(owner, {:go, owner_gate})
    assert {:ok, _} = Task.await(exclusive, 10_000)
    assert {:ok, %{id: "doc"}} = Task.await(waiter, 5_000)
  end

  test "killed exclusive caller releases the pool quiesce", %{uuid: uuid} do
    assert {:ok, _} = ElixirDB.Documents.put(uuid, %{id: "doc", body: %{"n" => 1}})

    gate = make_ref()
    Application.put_env(:elixir_db, :read_pool_sync, {self(), gate, uuid})

    holder =
      Task.async(fn ->
        ElixirDB.Documents.get(uuid, %{id: "doc"})
      end)

    assert_receive {^gate, :before_begin, worker}, 2_000

    {:ok, exclusive_pid} =
      Task.start(fn ->
        DatabaseCatalog.command(uuid, {:command, :integrity_check, %{}})
      end)

    Eventual.eventually(
      fn ->
        case ReadPool.stats(uuid) do
          {:ok, %{quiescing?: true}} -> true
          _ -> false
        end
      end,
      timeout: 2_000,
      message: "exclusive should be waiting for the active snapshot to drain"
    )

    Process.exit(exclusive_pid, :kill)

    Application.delete_env(:elixir_db, :read_pool_sync)
    send(worker, {:go, gate})
    assert {:ok, %{id: "doc"}} = Task.await(holder, 5_000)

    Eventual.eventually(
      fn ->
        case ReadPool.stats(uuid) do
          {:ok, %{active: 0, queued: 0, quiescing?: false}} -> true
          _ -> false
        end
      end,
      timeout: 2_000,
      message: "killed exclusive must not leave the pool quiesced"
    )

    assert {:ok, %{id: "doc"}} = ElixirDB.Documents.get(uuid, %{id: "doc"})
  end

  defp drain_grants(ref, acc \\ []) do
    receive do
      {^ref, :read_pool_grant, class, op} -> drain_grants(ref, [{class, op} | acc])
    after
      0 -> Enum.reverse(acc)
    end
  end
end
