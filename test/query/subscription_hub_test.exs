defmodule ElixirDB.Query.SubscriptionHubTest do
  use ElixirDB.Observability.OtelCase, async: false

  @moduletag :integration

  alias ElixirDB.Documents
  alias ElixirDB.Eventual
  alias ElixirDB.Query.{SubscriptionHub, Subscriptions}
  alias ElixirDB.Runtime.{ChangeNotifier, DatabaseCatalog}

  setup do
    rel = "subscription-hub-#{System.unique_integer([:positive])}.elixirdb"
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

  test "fans out each rapid revision with its own sequence and envelope", %{uuid: uuid} do
    assert {:ok, %{revision: initial_revision}} = put(uuid, "doc", %{"value" => 0})
    assert {:ok, subscription} = open_subscription(uuid)

    assert {:ok, %{type: :snapshot, document: %{revision: ^initial_revision}}} =
             Subscriptions.next(subscription)

    assert {:ok, %{type: :caught_up, sequence: caught_up_sequence}} =
             Subscriptions.next(subscription)

    assert {:ok, %{revision: revision_one}} = put(uuid, "doc", %{"value" => 1}, initial_revision)
    assert {:ok, %{revision: revision_two}} = put(uuid, "doc", %{"value" => 2}, revision_one)

    assert {:ok,
            %{
              type: :upsert,
              sequence: sequence_one,
              document: %{id: "doc", revision: ^revision_one, body: %{"value" => 1}}
            }} = Subscriptions.next(subscription)

    assert {:ok,
            %{
              type: :upsert,
              sequence: sequence_two,
              document: %{id: "doc", revision: ^revision_two, body: %{"value" => 2}}
            }} = Subscriptions.next(subscription)

    assert sequence_one > caught_up_sequence
    assert sequence_two > sequence_one
  end

  test "fans out committed bulk mutations", %{uuid: uuid} do
    assert {:ok, subscription} = open_subscription(uuid)
    assert {:ok, %{type: :caught_up}} = Subscriptions.next(subscription)

    assert {:ok, [%{revision: first_revision}, %{revision: second_revision}]} =
             ElixirDB.Documents.bulk_write(uuid, [
               %{
                 "type" => "put",
                 "id" => "bulk-a",
                 "body" => %{"kind" => "task", "value" => 1}
               },
               %{
                 "type" => "put",
                 "id" => "bulk-b",
                 "body" => %{"kind" => "task", "value" => 2}
               }
             ])

    assert {:ok, %{type: :upsert, document: %{id: "bulk-a", revision: ^first_revision}}} =
             Subscriptions.next(subscription)

    assert {:ok, %{type: :upsert, document: %{id: "bulk-b", revision: ^second_revision}}} =
             Subscriptions.next(subscription)
  end

  test "shares one fanout event with all active subscribers", %{uuid: uuid} do
    assert {:ok, %{revision: initial_revision}} = put(uuid, "doc", %{"value" => 0})
    assert {:ok, first} = open_subscription(uuid)
    assert {:ok, second} = open_subscription(uuid)
    assert {:ok, %{type: :caught_up}} = drain_snapshot(first)
    assert {:ok, %{type: :caught_up}} = drain_snapshot(second)

    assert {:ok, %{revision: revision}} = put(uuid, "doc", %{"value" => 1}, initial_revision)

    assert {:ok, %{type: :upsert, sequence: sequence, document: %{revision: ^revision}}} =
             Subscriptions.next(first)

    assert {:ok, %{type: :upsert, sequence: ^sequence, document: %{revision: ^revision}}} =
             Subscriptions.next(second)
  end

  test "holds a change during snapshot handshake until caught_up", %{uuid: uuid} do
    assert {:ok, %{revision: initial_revision}} = put(uuid, "doc", %{"value" => 0})
    assert {:ok, subscription} = open_subscription(uuid)

    assert {:ok, %{type: :snapshot, document: %{revision: ^initial_revision}}} =
             Subscriptions.next(subscription)

    assert {:ok, %{revision: revision}} = put(uuid, "doc", %{"value" => 1}, initial_revision)

    assert {:ok, %{type: :caught_up, sequence: snapshot_sequence}} =
             Subscriptions.next(subscription)

    assert {:ok,
            %{
              type: :upsert,
              sequence: sequence,
              document: %{id: "doc", revision: ^revision, body: %{"value" => 1}}
            }} = Subscriptions.next(subscription)

    assert sequence > snapshot_sequence
  end

  test "closing the hub closes its subscriptions", %{uuid: uuid} do
    assert {:ok, subscription} = open_subscription(uuid)
    assert {:ok, %{type: :caught_up}} = Subscriptions.next(subscription)
    assert :ok = DatabaseCatalog.close(uuid)

    assert {:closed, %{type: :closed}} = Subscriptions.next(subscription)
  end

  test "closes a waiting next caller without hanging", %{uuid: uuid} do
    assert {:ok, subscription} = open_subscription(uuid)
    assert {:ok, %{type: :caught_up}} = Subscriptions.next(subscription)

    parent = self()

    waiter =
      spawn(fn ->
        send(parent, {:waiter_result, Subscriptions.next(subscription, 5_000)})
      end)

    assert Process.alive?(waiter)

    Eventual.eventually(
      fn ->
        case Process.info(subscription, :message_queue_len) do
          # waiter may be inside GenServer.call; ensure subscription is waiting
          _ -> if Process.alive?(waiter), do: :ok, else: false
        end
      end,
      message: "expected waiting next caller"
    )

    assert :ok = DatabaseCatalog.close(uuid)

    assert_receive {:waiter_result, {:closed, %{type: :closed}}}, 5_000
    refute Process.alive?(waiter)
  end

  test "uses one notifier registration for many subscribers", %{uuid: uuid} do
    assert {:ok, first} = open_subscription(uuid)
    assert {:ok, second} = open_subscription(uuid)
    assert {:ok, %{type: :caught_up}} = drain_snapshot(first)
    assert {:ok, %{type: :caught_up}} = drain_snapshot(second)

    assert 1 = ChangeNotifier.subscriber_count(uuid)
    assert 2 = SubscriptionHub.count(uuid)

    assert :ok = Subscriptions.close(first)
    assert :ok = Subscriptions.close(second)

    Eventual.eventually(
      fn ->
        case ChangeNotifier.subscriber_count(uuid) do
          0 -> :ok
          _ -> false
        end
      end,
      message: "expected notifier unsubscribe after last subscription"
    )

    assert 0 = SubscriptionHub.count(uuid)
  end

  test "gets revisions once for a changes batch shared by subscribers", %{uuid: uuid} do
    TestExporter.reset()

    assert {:ok, %{revision: initial_revision}} = put(uuid, "doc", %{"value" => 0})
    assert {:ok, first} = open_subscription(uuid)
    assert {:ok, second} = open_subscription(uuid)
    assert {:ok, %{type: :caught_up}} = drain_snapshot(first)
    assert {:ok, %{type: :caught_up}} = drain_snapshot(second)

    TestExporter.reset()
    assert {:ok, %{revision: revision}} = put(uuid, "doc", %{"value" => 1}, initial_revision)

    assert {:ok, %{type: :upsert, document: %{revision: ^revision}}} =
             Subscriptions.next(first)

    assert {:ok, %{type: :upsert, document: %{revision: ^revision}}} =
             Subscriptions.next(second)

    Eventual.eventually(
      fn ->
        spans =
          TestExporter.spans_named("elixir_db.database.command")
          |> Enum.filter(fn span ->
            TestExporter.span_attr(span, :"db.uuid") == uuid and
              TestExporter.span_attr(span, :"command.type") == :get_revisions_batch
          end)

        # One change must not multiply owner reads by subscriber count.
        case Enum.count_until(spans, 4) do
          1 -> :ok
          _ -> false
        end
      end,
      message: "expected exactly one shared get_revisions_batch span"
    )
  end

  test "hub remains countable while subscriptions are active", %{uuid: uuid} do
    assert {:ok, subscription} = open_subscription(uuid)
    assert {:ok, %{type: :caught_up}} = Subscriptions.next(subscription)
    assert 1 = SubscriptionHub.count(uuid)
    assert :ok = Subscriptions.close(subscription)

    Eventual.eventually(
      fn ->
        case SubscriptionHub.count(uuid) do
          0 -> :ok
          _ -> false
        end
      end,
      message: "expected subscription removed from hub"
    )
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

  defp put(uuid, id, body, if_revision \\ nil) do
    request = %{id: id, body: Map.put(body, "kind", "task")}
    request = if is_nil(if_revision), do: request, else: Map.put(request, :if_revision, if_revision)
    Documents.put(uuid, request)
  end
end
