defmodule VialKeeper.Query.SubscriptionActivateRaceTest do
  @moduledoc "Covers the activate/reset race on subscription re-snapshotting."

  use ExUnit.Case, async: false

  @moduletag :integration

  alias VialKeeper.Documents
  alias VialKeeper.Eventual
  alias VialKeeper.Query.{SubscriptionHub, Subscriptions}
  alias VialKeeper.Runtime.{AttachmentCoordinator, DatabaseCatalog, DatabaseRegistry}

  setup do
    rel = "subscription-activate-race-#{System.unique_integer([:positive])}.vialkeeper"
    root = VialKeeper.Config.database_root()
    abs = Path.join(root, rel)
    VialKeeper.TempDatabase.cleanup(abs)

    assert {:ok, identity} = DatabaseCatalog.create(rel)
    uuid = identity.database_uuid
    assert {:ok, _} = DatabaseCatalog.open(uuid)

    on_exit(fn ->
      _ = DatabaseCatalog.close(uuid)
      _ = DatabaseCatalog.unregister(uuid)
      Application.delete_env(:vial_keeper, :subscription_activate_sync)
      VialKeeper.TempDatabase.cleanup(abs)
    end)

    {:ok, uuid: uuid}
  end

  test "activate answers a pending_snapshot subscriber with history_truncated", %{uuid: uuid} do
    assert {:ok, _seq} = SubscriptionHub.begin_subscription(uuid, self(), 16)

    assert {:error, %VialKeeper.Error{code: :history_truncated}} =
             SubscriptionHub.activate(uuid, self(), 0)

    # Normal snapshot path is unchanged after snapshot_ready.
    assert :ok = SubscriptionHub.snapshot_ready(uuid, self(), 0)
    assert :ok = SubscriptionHub.activate(uuid, self(), 0)

    # A double activate from :active stays an invalid_request.
    assert {:error, %VialKeeper.Error{code: :invalid_request}} =
             SubscriptionHub.activate(uuid, self(), 0)

    assert :ok = SubscriptionHub.unregister(uuid, self())

    # An unregistered pid is still an invalid_request.
    assert {:error, %VialKeeper.Error{code: :invalid_request}} =
             SubscriptionHub.activate(uuid, self(), 0)
  end

  test "reset racing a blocked activate leaves the subscription delivering", %{uuid: uuid} do
    configure_retention(uuid)
    assert {:ok, %{revision: keep_revision}} = put(uuid, "keep", %{"title" => "keep"})

    barrier = make_ref()
    Application.put_env(:vial_keeper, :subscription_activate_sync, {self(), barrier, uuid})

    assert {:ok, subscription} = open_subscription(uuid)

    # Drain the initial snapshot; the final document empties the drain so the
    # next call is the one that hits activate_after_snapshot.
    assert {:ok, %{type: :snapshot, document: %{id: "keep"}}} =
             Subscriptions.next(subscription, 5_000)

    blocker = Task.async(fn -> Subscriptions.next(subscription, 5_000) end)
    assert_receive {^barrier, :activate_ready, ^subscription}, 5_000
    assert Process.alive?(subscription)

    [{hub, _}] = Registry.lookup(DatabaseRegistry, {:query_subscription_hub, uuid})

    :sys.suspend(hub)
    assert {:ok, _} = put(uuid, "late", %{"title" => "late"})

    assert {:ok, %{new_floor: floor}} =
             DatabaseCatalog.command(uuid, {:command, :compact_retention, %{}})

    assert floor > 0
    :sys.resume(hub)

    # The hub resets the subscription (sending :subscription_reset) strictly
    # before the blocked activate call is allowed to continue.
    Eventual.eventually(
      fn ->
        state = :sys.get_state(hub)

        case Map.get(state.subscriptions, subscription) do
          %{mode: :pending_snapshot} -> true
          _ -> false
        end
      end,
      timeout: 5_000,
      message: "expected subscription reset to pending_snapshot"
    )

    send(subscription, {:go, barrier})
    Application.delete_env(:vial_keeper, :subscription_activate_sync)

    # The blocked activate answers the race as a reset, not a terminal error.
    assert {:ok, %{type: :reset}} = Task.await(blocker, 5_000)
    assert Process.alive?(subscription)
    assert SubscriptionHub.count(uuid) == 1

    # The queued :subscription_reset drives the canonical reset transition,
    # which emits its own reset event (via reset_pending) before snapshots.
    assert {:ok, %{type: :reset}} = Subscriptions.next(subscription, 5_000)

    members =
      collect_until_caught_up(subscription, [])
      |> Enum.map(& &1.document.id)
      |> MapSet.new()

    assert members == MapSet.new(["keep", "late"])

    wait_for_attachment_gc(uuid)

    assert {:ok, %{revision: newer}} =
             put(uuid, "keep", %{"title" => "after-reset"}, keep_revision)

    assert {:ok, %{type: :upsert, document: %{id: "keep", revision: ^newer}}} =
             Subscriptions.next(subscription, 5_000)

    # An ordinary (non-barrier) retention reset on the same subscription still
    # delivers a fresh snapshot and keeps the subscription alive.
    wait_for_attachment_gc(uuid)

    assert {:ok, _} = put(uuid, "third", %{"title" => "third"})

    [{hub, _}] = Registry.lookup(DatabaseRegistry, {:query_subscription_hub, uuid})

    :sys.suspend(hub)

    assert {:ok, %{new_floor: floor}} =
             DatabaseCatalog.command(uuid, {:command, :compact_retention, %{}})

    assert floor > 0
    :sys.resume(hub)

    assert {:ok, %{type: :reset}} = Subscriptions.next(subscription, 5_000)
    assert Process.alive?(subscription)

    members =
      collect_until_caught_up(subscription, [])
      |> Enum.map(& &1.document.id)
      |> MapSet.new()

    assert members == MapSet.new(["keep", "late", "third"])
  end

  defp configure_retention(uuid) do
    assert {:ok, _} =
             DatabaseCatalog.command(
               uuid,
               {:command, :update_config,
                %{
                  "retention" => %{
                    "mode" => "stable_frontier",
                    "history_depth" => 0,
                    "peer_expiry_ms" => 86_400_000,
                    "schedule" => "disabled"
                  }
                }}
             )
  end

  defp wait_for_attachment_gc(uuid) do
    Eventual.eventually(
      fn ->
        case AttachmentCoordinator.status(uuid) do
          %{gc_barrier: false, gc_active: false, gc_queued: false, gc_scheduled: false} -> true
          _ -> false
        end
      end,
      timeout: 5_000,
      message: "attachment GC after compact did not become idle"
    )
  end

  defp collect_until_caught_up(subscription, documents) do
    case Subscriptions.next(subscription, 5_000) do
      {:ok, %{type: :snapshot} = event} ->
        collect_until_caught_up(subscription, [event | documents])

      {:ok, %{type: :caught_up}} ->
        Enum.reverse(documents)

      other ->
        flunk("expected snapshot or caught_up, got: #{inspect(other)}")
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

  defp put(uuid, id, body, if_revision \\ nil) do
    request = %{id: id, body: Map.put(body, "kind", "task")}
    request = if is_nil(if_revision), do: request, else: Map.put(request, :if_revision, if_revision)
    Documents.put(uuid, request)
  end
end
