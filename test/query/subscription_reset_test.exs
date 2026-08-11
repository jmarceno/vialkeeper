defmodule ElixirDB.Query.SubscriptionResetTest do
  use ExUnit.Case, async: false

  @moduletag :integration

  alias ElixirDB.Documents
  alias ElixirDB.Eventual
  alias ElixirDB.Query.Subscriptions
  alias ElixirDB.Runtime.{AttachmentCoordinator, DatabaseCatalog}

  setup do
    rel = "subscription-reset-#{System.unique_integer([:positive])}.elixirdb"
    root = ElixirDB.Config.database_root()
    abs = Path.join(root, rel)
    ElixirDB.TempDatabase.cleanup(abs)

    assert {:ok, identity} = DatabaseCatalog.create(rel)
    uuid = identity.database_uuid

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

    on_exit(fn ->
      _ = DatabaseCatalog.close(uuid)
      _ = DatabaseCatalog.unregister(uuid)
      ElixirDB.TempDatabase.cleanup(abs)
    end)

    {:ok, uuid: uuid}
  end

  test "history truncation emits reset and a replacement snapshot", %{uuid: uuid} do
    assert {:ok, %{revision: keep_revision}} = put(uuid, "keep", %{"title" => "keep"})
    assert {:ok, %{revision: drop_revision}} = put(uuid, "drop", %{"title" => "drop"})
    assert {:ok, subscription} = open_subscription(uuid)

    snapshots = collect_until_caught_up(subscription, [])
    assert MapSet.new(Enum.map(snapshots, & &1.document.id)) == MapSet.new(["drop", "keep"])

    assert Enum.any?(snapshots, fn event ->
             event.document.id == "keep" and event.document.revision == keep_revision
           end)

    assert Enum.any?(snapshots, fn event ->
             event.document.id == "drop" and event.document.revision == drop_revision
           end)

    [{hub, _}] =
      Registry.lookup(ElixirDB.Runtime.DatabaseRegistry, {:query_subscription_hub, uuid})

    :sys.suspend(hub)

    assert {:ok, %{revision: late_revision}} = put(uuid, "late", %{"title" => "late"})

    assert {:ok, %{new_floor: new_floor}} =
             DatabaseCatalog.command(uuid, {:command, :compact_retention, %{}})

    assert new_floor > 0
    :sys.resume(hub)

    assert {:ok, %{type: :reset, sequence: reset_sequence}} =
             Subscriptions.next(subscription, 5_000)

    members =
      collect_until_caught_up(subscription, [])
      |> Enum.map(& &1.document.id)
      |> MapSet.new()

    assert members == MapSet.new(["drop", "keep", "late"])
    assert reset_sequence >= new_floor

    wait_for_attachment_gc(uuid)

    assert {:ok, %{revision: newer}} =
             put(uuid, "keep", %{"title" => "after-reset"}, keep_revision)

    assert {:ok,
            %{
              type: :upsert,
              sequence: sequence,
              document: %{id: "keep", revision: ^newer, body: %{"title" => "after-reset"}}
            }} = Subscriptions.next(subscription, 5_000)

    assert sequence > reset_sequence
    _ = late_revision
  end

  test "reset clears stale membership before replacement snapshots", %{uuid: uuid} do
    assert {:ok, %{revision: revision}} = put(uuid, "only", %{"title" => "one"})
    assert {:ok, subscription} = open_subscription(uuid)

    assert {:ok, %{type: :snapshot, document: %{id: "only"}}} =
             Subscriptions.next(subscription, 5_000)

    assert {:ok, %{type: :caught_up}} = Subscriptions.next(subscription, 5_000)

    assert {:ok, _} = Documents.delete(uuid, %{id: "only", if_revision: revision})
    assert {:ok, %{type: :remove, id: "only"}} = Subscriptions.next(subscription, 5_000)

    assert {:ok, _} = put(uuid, "fresh", %{"title" => "two"})

    assert {:ok, %{type: :upsert, document: %{id: "fresh"}}} =
             Subscriptions.next(subscription, 5_000)

    [{hub, _}] =
      Registry.lookup(ElixirDB.Runtime.DatabaseRegistry, {:query_subscription_hub, uuid})

    :sys.suspend(hub)
    assert {:ok, _} = put(uuid, "newer", %{"title" => "three"})

    assert {:ok, %{new_floor: floor}} =
             DatabaseCatalog.command(uuid, {:command, :compact_retention, %{}})

    assert floor > 0
    :sys.resume(hub)

    assert {:ok, %{type: :reset}} = Subscriptions.next(subscription, 5_000)

    members =
      collect_until_caught_up(subscription, [])
      |> Enum.map(& &1.document.id)
      |> MapSet.new()

    assert members == MapSet.new(["fresh", "newer"])
  end

  test "maintenance below hub cursor triggers reset without reading truncated history", %{
    uuid: uuid
  } do
    assert {:ok, _} = put(uuid, "a", %{"title" => "a"})
    assert {:ok, subscription} = open_subscription(uuid)
    _ = collect_until_caught_up(subscription, [])

    [{hub, _}] =
      Registry.lookup(ElixirDB.Runtime.DatabaseRegistry, {:query_subscription_hub, uuid})

    cursor_before = :sys.get_state(hub).cursor_sequence
    :sys.suspend(hub)
    assert {:ok, _} = put(uuid, "b", %{"title" => "b"})

    assert {:ok, %{new_floor: floor}} =
             DatabaseCatalog.command(uuid, {:command, :compact_retention, %{}})

    assert floor > cursor_before
    :sys.resume(hub)

    assert {:ok, %{type: :reset, sequence: sequence}} = Subscriptions.next(subscription, 5_000)
    assert sequence >= floor

    state_after_reset_start = fn ->
      state = :sys.get_state(hub)
      # Cursor advances only after replacement snapshot_ready, not to the floor alone.
      state.cursor_sequence >= sequence or state.resetting
    end

    assert state_after_reset_start.()

    members =
      collect_until_caught_up(subscription, [])
      |> Enum.map(& &1.document.id)
      |> MapSet.new()

    assert members == MapSet.new(["a", "b"])
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
