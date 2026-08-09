defmodule ElixirDB.Query.SubscriptionProcessTest do
  use ExUnit.Case, async: false

  alias ElixirDB.Eventual
  alias ElixirDB.Query.Subscriptions
  alias ElixirDB.Runtime.DatabaseCatalog

  setup do
    rel = "subscription-proc-#{System.unique_integer([:positive])}.elixirdb"
    root = ElixirDB.Config.database_root()
    abs = Path.join(root, rel)
    ElixirDB.TempDatabase.cleanup(abs)

    assert {:ok, identity} = DatabaseCatalog.create(rel)
    uuid = identity.database_uuid

    on_exit(fn ->
      _ = DatabaseCatalog.close(uuid)
      _ = DatabaseCatalog.unregister(uuid)
      ElixirDB.TempDatabase.cleanup(abs)
    end)

    assert {:ok, _} =
             DatabaseCatalog.command(
               uuid,
               {:command, :put, %{document_id: "a", body: %{"type" => "task", "title" => "A"}}}
             )

    assert {:ok, _} =
             DatabaseCatalog.command(
               uuid,
               {:command, :put, %{document_id: "b", body: %{"type" => "note"}}}
             )

    {:ok, uuid: uuid}
  end

  test "lazy snapshot delivery then caught_up", %{uuid: uuid} do
    assert {:ok, pid} =
             Subscriptions.open(uuid, %{"query" => %{"selector" => %{"/type" => "task"}}}, self())

    assert {:ok, %{type: :snapshot, document: %{id: "a"}}} = Subscriptions.next(pid, 5_000)
    assert {:ok, %{type: :caught_up, sequence: sequence}} = Subscriptions.next(pid, 5_000)
    assert is_integer(sequence) and sequence >= 1
  end

  test "zero matches returns caught_up only", %{uuid: uuid} do
    assert {:ok, pid} =
             Subscriptions.open(
               uuid,
               %{"query" => %{"selector" => %{"/type" => "missing"}}},
               self()
             )

    assert {:ok, %{type: :caught_up}} = Subscriptions.next(pid, 5_000)
  end

  test "rejects concurrent next callers", %{uuid: uuid} do
    assert {:ok, pid} =
             Subscriptions.open(
               uuid,
               %{"query" => %{"selector" => %{"/type" => "missing"}}, "heartbeat_ms" => 30_000},
               self()
             )

    assert {:ok, %{type: :caught_up}} = Subscriptions.next(pid, 5_000)

    parent = self()

    _waiter =
      spawn(fn ->
        send(parent, {:waiter_started, self()})
        send(parent, {:waiter, Subscriptions.next(pid, 5_000)})
      end)

    assert_receive {:waiter_started, waiter}, 1_000

    Eventual.eventually(
      fn ->
        case :sys.get_state(pid).waiter do
          nil -> false
          _from -> :ok
        end
      end,
      message: "expected subscription to hold the first next caller"
    )

    assert {:error, %ElixirDB.Error{code: :invalid_request}} = Subscriptions.next(pid, 1_000)
    Process.exit(waiter, :kill)
  end

  test "heartbeat arrives while waiting", %{uuid: uuid} do
    assert {:ok, pid} =
             Subscriptions.open(
               uuid,
               %{"query" => %{"selector" => %{"/type" => "missing"}}, "heartbeat_ms" => 50},
               self()
             )

    assert {:ok, %{type: :caught_up}} = Subscriptions.next(pid, 5_000)
    assert {:ok, %{type: :heartbeat}} = Subscriptions.next(pid, 5_000)
  end

  test "client death removes subscription", %{uuid: uuid} do
    parent = self()

    client =
      spawn(fn ->
        {:ok, pid} =
          Subscriptions.open(uuid, %{"query" => %{"selector" => %{"/type" => "task"}}}, self())

        send(parent, {:opened, pid})
        receive do: (:stop -> :ok)
      end)

    assert_receive {:opened, pid}, 5_000
    assert Process.alive?(pid)
    Process.exit(client, :kill)

    Eventual.eventually(
      fn ->
        if Process.alive?(pid), do: false, else: :ok
      end,
      message: "expected client death to terminate the subscription"
    )
  end

  test "supervisor and hub are registered after open", %{uuid: uuid} do
    assert [{_, _}] =
             Registry.lookup(
               ElixirDB.Runtime.DatabaseRegistry,
               {:query_subscription_supervisor, uuid}
             )

    assert [{_, _}] =
             Registry.lookup(ElixirDB.Runtime.DatabaseRegistry, {:query_subscription_hub, uuid})

    assert [{_, _}] =
             Registry.lookup(
               ElixirDB.Runtime.DatabaseRegistry,
               {:query_subscription_dynamic_supervisor, uuid}
             )
  end

  test "close is idempotent", %{uuid: uuid} do
    assert {:ok, pid} =
             Subscriptions.open(uuid, %{"query" => %{"selector" => %{"/type" => "task"}}}, self())

    assert :ok = Subscriptions.close(pid)
    assert :ok = Subscriptions.close(pid)
  end
end
