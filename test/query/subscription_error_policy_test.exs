defmodule ElixirDB.Query.SubscriptionErrorPolicyTest do
  @moduledoc "Covers the subscription hub retryable/fatal read error policy."

  use ElixirDB.Observability.OtelCase, async: false

  @moduletag :integration

  alias ElixirDB.Documents
  alias ElixirDB.Eventual
  alias ElixirDB.Query.{SubscriptionHub, Subscriptions}
  alias ElixirDB.Runtime.DatabaseCatalog

  @fail_env :subscription_hub_fail_reads

  setup do
    rel = "subscription-error-policy-#{System.unique_integer([:positive])}.elixirdb"
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

    {:ok, uuid: uuid}
  end

  test "retryable failure then recovery delivers the pending change", %{uuid: uuid} do
    assert {:ok, subscription} = open_subscription(uuid)
    assert {:ok, %{type: :caught_up}} = drain_snapshot(subscription)
    assert 1 = SubscriptionHub.count(uuid)

    set_fail_reads(uuid, ElixirDB.Error.database_overloaded("read pool busy"))

    assert {:ok, %{revision: revision}} = put(uuid, "recovered", %{"value" => 1})

    clear_fail_reads()

    assert {:ok, %{type: :upsert, document: %{revision: ^revision}}} =
             Subscriptions.next(subscription, 10_000)

    assert 1 = SubscriptionHub.count(uuid)
  end

  test "recovery cancels a pending retry timer", %{uuid: uuid} do
    assert {:ok, subscription} = open_subscription(uuid)
    assert {:ok, %{type: :caught_up}} = drain_snapshot(subscription)

    set_fail_reads(uuid, ElixirDB.Error.database_overloaded("read pool busy"))
    assert {:ok, %{revision: _}} = put(uuid, "failed-once", %{"value" => 1})

    [{hub, _}] = Registry.lookup(ElixirDB.Runtime.DatabaseRegistry, {:query_subscription_hub, uuid})

    Eventual.eventually(
      fn ->
        state = :sys.get_state(hub)
        Map.has_key?(state, :retry_timer) and is_tuple(state.retry_timer)
      end,
      message: "retry timer was not scheduled"
    )

    clear_fail_reads()
    assert {:ok, %{revision: _}} = put(uuid, "recovered", %{"value" => 2})

    assert {:ok, %{type: :upsert}} = Subscriptions.next(subscription, 10_000)

    Eventual.eventually(
      fn ->
        state = :sys.get_state(hub)
        state.retry_timer == nil and state.consecutive_read_failures == 0
      end,
      message: "successful read did not clear retry state"
    )
  end

  test "fatal failure still terminates the subscription", %{uuid: uuid} do
    assert {:ok, subscription} = open_subscription(uuid)
    assert {:ok, %{type: :caught_up}} = drain_snapshot(subscription)

    set_fail_reads(uuid, ElixirDB.Error.integrity_violation("revision batch mismatch"))

    parent = self()
    _waiter = spawn(fn -> send(parent, {:next, Subscriptions.next(subscription, 10_000)}) end)

    assert {:ok, %{revision: _}} = put(uuid, "fatal", %{"value" => 1})

    assert_receive {:next, {:error, %{type: :error, error: error}}}, 10_000
    assert error.code == :integrity_violation

    Eventual.eventually(
      fn ->
        case SubscriptionHub.count(uuid) do
          0 -> :ok
          _ -> false
        end
      end,
      message: "expected fatal failure to unregister the subscription"
    )
  end

  test "retries are bounded and eventually surface a terminal error", %{uuid: uuid} do
    assert {:ok, subscription} = open_subscription(uuid)
    assert {:ok, %{type: :caught_up}} = drain_snapshot(subscription)

    set_fail_reads(uuid, ElixirDB.Error.database_overloaded("read pool busy"))

    parent = self()
    _waiter = spawn(fn -> send(parent, {:next, Subscriptions.next(subscription, 35_000)}) end)

    assert {:ok, %{revision: _}} = put(uuid, "bounded", %{"value" => 1})

    assert_receive {:next, {:error, %{type: :error, error: error}}}, 30_000
    assert error.code == :database_overloaded
  end

  defp open_subscription(uuid) do
    Subscriptions.open(
      uuid,
      %{
        "query" => %{"selector" => %{"/kind" => "task"}},
        "heartbeat_ms" => 30_000
      },
      self()
    )
  end

  defp drain_snapshot(subscription) do
    case Subscriptions.next(subscription) do
      {:ok, %{type: :snapshot}} -> drain_snapshot(subscription)
      {:ok, %{type: :caught_up} = event} -> {:ok, event}
      other -> other
    end
  end

  defp set_fail_reads(uuid, error) do
    Application.put_env(:elixir_db, @fail_env, {uuid, error})

    on_exit(fn ->
      Application.delete_env(:elixir_db, @fail_env)
    end)
  end

  defp clear_fail_reads, do: Application.delete_env(:elixir_db, @fail_env)

  defp put(uuid, id, body) do
    request = %{id: id, body: Map.put(body, "kind", "task")}
    Documents.put(uuid, request)
  end
end
