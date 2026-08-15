defmodule VialKeeper.Query.SubscriptionCreditsTest do
  @moduledoc "Covers subscription credit accounting and backpressure."

  use ExUnit.Case, async: false

  @moduletag :integration

  alias VialKeeper.Documents
  alias VialKeeper.Eventual
  alias VialKeeper.Query.{SubscriptionHub, Subscriptions}
  alias VialKeeper.Runtime.{ChangeNotifier, DatabaseCatalog}

  @buffer 3

  setup do
    rel = "subscription-credits-#{System.unique_integer([:positive])}.vialkeeper"
    root = VialKeeper.Config.database_root()
    abs = Path.join(root, rel)
    VialKeeper.TempDatabase.cleanup(abs)

    assert {:ok, identity} = DatabaseCatalog.create(rel)
    uuid = identity.database_uuid

    assert {:ok, _} =
             DatabaseCatalog.command(
               uuid,
               {:command, :update_config, %{"subscriptions" => %{"max_buffered_events" => @buffer}}}
             )

    on_exit(fn ->
      _ = DatabaseCatalog.close(uuid)
      _ = DatabaseCatalog.unregister(uuid)
      VialKeeper.TempDatabase.cleanup(abs)
    end)

    {:ok, uuid: uuid}
  end

  test "slow consumer overloads while a fast consumer continues", %{uuid: uuid} do
    assert {:ok, %{revision: revision}} = put(uuid, "doc", %{"value" => 0})
    assert {:ok, fast} = open_subscription(uuid)
    assert {:ok, slow} = open_subscription(uuid)
    assert {:ok, %{type: :caught_up}} = drain_snapshot(fast)
    assert {:ok, %{type: :caught_up}} = drain_snapshot(slow)

    parent = self()

    fast_consumer =
      spawn(fn ->
        consume_fast(fast, parent, 0)
      end)

    revision =
      Enum.reduce(1..(@buffer + 5), revision, fn value, current ->
        assert {:ok, %{revision: next}} = put(uuid, "doc", %{"value" => value}, current)
        assert_receive {:fast_count, count} when count >= value, 5_000
        next
      end)

    assert {:error, %{type: :error, error: %VialKeeper.Error{code: :subscription_overloaded}}} =
             Subscriptions.next(slow, 5_000)

    refute Process.alive?(slow)

    assert {:ok, %{revision: newer}} = put(uuid, "doc", %{"value" => 99}, revision)

    assert_receive {:fast_event, %{type: :upsert, document: %{revision: ^newer}}}, 5_000

    Process.exit(fast_consumer, :kill)
    assert Process.alive?(fast)
    assert 1 = SubscriptionHub.count(uuid)
  end

  test "hub keeps subscription mailbox bounded by credits", %{uuid: uuid} do
    assert {:ok, %{revision: revision}} = put(uuid, "doc", %{"value" => 0})
    assert {:ok, slow} = open_subscription(uuid)
    assert {:ok, %{type: :caught_up}} = drain_snapshot(slow)

    peak =
      Enum.reduce(1..(@buffer + 5), {revision, 0}, fn value, {current, peak_len} ->
        assert {:ok, %{revision: next}} = put(uuid, "doc", %{"value" => value}, current)

        Eventual.eventually(
          fn ->
            case Process.info(slow, :message_queue_len) do
              {_, len} when len >= 0 -> :ok
              nil -> :ok
              _ -> false
            end
          end,
          message: "expected subscription process to remain inspectable"
        )

        len =
          case Process.info(slow, :message_queue_len) do
            {_, queue_len} -> queue_len
            nil -> 0
          end

        # Credits allow at most @buffer incremental messages; the terminal overload
        # control message is outside the credit window (+1).
        assert len <= @buffer + 1
        {next, max(peak_len, len)}
      end)
      |> elem(1)

    assert peak <= @buffer + 1

    assert {:error, %{type: :error, error: %VialKeeper.Error{code: :subscription_overloaded}}} =
             Subscriptions.next(slow, 5_000)
  end

  test "client death unregisters the subscription from the hub", %{uuid: uuid} do
    parent = self()

    client =
      spawn(fn ->
        assert {:ok, subscription} = open_subscription(uuid)
        send(parent, {:opened, subscription})
        Process.sleep(60_000)
      end)

    assert_receive {:opened, subscription}, 5_000
    assert {:ok, %{type: :caught_up}} = drain_snapshot(subscription)
    assert 1 = SubscriptionHub.count(uuid)
    assert 1 = ChangeNotifier.subscriber_count(uuid)

    Process.exit(client, :kill)

    Eventual.eventually(
      fn ->
        case {SubscriptionHub.count(uuid), ChangeNotifier.subscriber_count(uuid)} do
          {0, 0} -> :ok
          _ -> false
        end
      end,
      message: "expected hub and notifier cleanup after client death"
    )

    refute Process.alive?(subscription)
  end

  test "duplicate credit returns cannot exceed the configured window", %{uuid: uuid} do
    assert {:ok, _} = put(uuid, "doc", %{"value" => 0})
    assert {:ok, subscription} = open_subscription(uuid)
    assert {:ok, %{type: :caught_up}} = drain_snapshot(subscription)

    [{hub, _}] =
      Registry.lookup(VialKeeper.Runtime.DatabaseRegistry, {:query_subscription_hub, uuid})

    Enum.each(1..(@buffer * 3), fn _ ->
      SubscriptionHub.return_credit(uuid, subscription)
    end)

    Eventual.eventually(
      fn ->
        credits = :sys.get_state(hub).subscriptions[subscription].credits
        if credits == @buffer, do: :ok, else: false
      end,
      message: "expected credits capped at max_buffered_events"
    )

    assert :sys.get_state(hub).subscriptions[subscription].credits == @buffer
  end

  test "filtered events return credit so later matches can deliver", %{uuid: uuid} do
    assert {:ok, %{revision: revision}} = put(uuid, "doc", %{"value" => 0})
    assert {:ok, subscription} = open_subscription(uuid)
    assert {:ok, %{type: :caught_up}} = drain_snapshot(subscription)

    [{hub, _}] =
      Registry.lookup(VialKeeper.Runtime.DatabaseRegistry, {:query_subscription_hub, uuid})

    # Use one credit without consuming, leaving one credit available.
    assert {:ok, %{revision: revision, sequence: first_sequence}} =
             put(uuid, "doc", %{"value" => 1}, revision)

    Eventual.eventually(
      fn ->
        hub_state = :sys.get_state(hub)
        subscription_state = :sys.get_state(subscription)

        if hub_state.cursor_sequence >= first_sequence and :queue.len(subscription_state.queue) == 1,
          do: :ok,
          else: false
      end,
      message: "expected matching update to be queued"
    )

    # Non-matching update consumes then returns credit after filter evaluation.
    assert {:ok, %{sequence: other_sequence}} =
             Documents.put(uuid, %{
               id: "other",
               body: %{"kind" => "note", "value" => 1}
             })

    Eventual.eventually(
      fn ->
        hub_state = :sys.get_state(hub)
        subscription_state = :sys.get_state(subscription)

        if hub_state.cursor_sequence >= other_sequence and :queue.len(subscription_state.queue) == 1,
          do: :ok,
          else: false
      end,
      message: "expected non-matching update to return its credit"
    )

    assert {:ok, %{revision: matching}} = put(uuid, "doc", %{"value" => 2}, revision)

    assert {:ok, %{type: :upsert, document: %{body: %{"value" => 1}}}} =
             Subscriptions.next(subscription, 5_000)

    assert {:ok, %{type: :upsert, document: %{revision: ^matching, body: %{"value" => 2}}}} =
             Subscriptions.next(subscription, 5_000)
  end

  test "subscription death removes hub routing state", %{uuid: uuid} do
    assert {:ok, subscription} = open_subscription(uuid)
    assert {:ok, %{type: :caught_up}} = drain_snapshot(subscription)
    assert 1 = SubscriptionHub.count(uuid)

    Process.exit(subscription, :kill)

    Eventual.eventually(
      fn ->
        case SubscriptionHub.count(uuid) do
          0 -> :ok
          _ -> false
        end
      end,
      message: "expected hub to drop dead subscription"
    )
  end

  test "returning credit after consume allows further fanout", %{uuid: uuid} do
    assert {:ok, %{revision: revision}} = put(uuid, "doc", %{"value" => 0})
    assert {:ok, subscription} = open_subscription(uuid)
    assert {:ok, %{type: :caught_up}} = drain_snapshot(subscription)

    revision =
      Enum.reduce(1..@buffer, revision, fn value, current ->
        assert {:ok, %{revision: next}} = put(uuid, "doc", %{"value" => value}, current)
        next
      end)

    for value <- 1..@buffer do
      assert {:ok, %{type: :upsert, document: %{body: %{"value" => ^value}}}} =
               Subscriptions.next(subscription, 5_000)
    end

    assert {:ok, %{revision: newer}} = put(uuid, "doc", %{"value" => 100}, revision)

    assert {:ok, %{type: :upsert, document: %{revision: ^newer, body: %{"value" => 100}}}} =
             Subscriptions.next(subscription, 5_000)
  end

  defp consume_fast(subscription, parent, count) do
    case Subscriptions.next(subscription, 5_000) do
      {:ok, %{type: :upsert} = event} ->
        send(parent, {:fast_event, event})
        send(parent, {:fast_count, count + 1})
        consume_fast(subscription, parent, count + 1)

      {:ok, _other} ->
        consume_fast(subscription, parent, count)

      other ->
        send(parent, {:fast_done, other})
    end
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
