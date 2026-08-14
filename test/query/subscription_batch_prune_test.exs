defmodule ElixirDB.Query.SubscriptionBatchPruneTest do
  @moduledoc """
  Covers the hub resettable classification for batch-pruned revision references.

  The injected-error tests prove the classifier deterministically: when
  compaction retains revisions between the hub's two batch snapshots, the second
  snapshot surfaces an `integrity_violation` whose message names a missing
  revision/document. These cases assert that the hub routes that shape into a
  reset + replacement snapshot instead of killing every subscription. They do
  not reproduce the scheduler timing of compaction running between the two
  snapshots; the injected-error hook is a deterministic proof of the
  classifier, not a race reproduction.
  """

  use ElixirDB.Observability.OtelCase, async: false

  @moduletag :integration

  alias ElixirDB.Documents
  alias ElixirDB.Eventual
  alias ElixirDB.Query.{SubscriptionHub, Subscriptions}
  alias ElixirDB.Runtime.DatabaseCatalog

  @fail_env :subscription_hub_fail_reads

  setup do
    rel = "subscription-batch-prune-#{System.unique_integer([:positive])}.elixirdb"
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

  test "batch-pruned revision reference resets instead of killing the subscription", %{
    uuid: uuid
  } do
    assert {:ok, subscription} = open_subscription(uuid)
    assert {:ok, %{type: :caught_up}} = drain_snapshot(subscription)

    set_fail_reads(
      uuid,
      ElixirDB.Error.integrity_violation(
        "changes entry references a missing revision",
        %{document_id: "d", revision_id: "r"}
      )
    )

    assert {:ok, %{revision: _}} = put(uuid, "pruned", %{"value" => 1})

    wait_until_resetting(uuid)

    clear_fail_reads()

    assert {:ok, %{type: :reset}} = Subscriptions.next(subscription, 10_000)

    members =
      collect_until_caught_up(subscription, [])
      |> Enum.map(& &1.document.id)
      |> MapSet.new()

    assert members == MapSet.new(["pruned"])

    Eventual.eventually(
      fn ->
        case SubscriptionHub.count(uuid) do
          1 -> :ok
          _ -> false
        end
      end,
      message: "expected the resettable path to keep the subscription registered"
    )
  end

  test "batch-pruned document reference resets instead of killing the subscription", %{
    uuid: uuid
  } do
    assert {:ok, subscription} = open_subscription(uuid)
    assert {:ok, %{type: :caught_up}} = drain_snapshot(subscription)

    set_fail_reads(
      uuid,
      ElixirDB.Error.integrity_violation(
        "changes entry references a missing document",
        %{document_id: "d"}
      )
    )

    assert {:ok, %{revision: _}} = put(uuid, "doc-pruned", %{"value" => 1})

    wait_until_resetting(uuid)

    clear_fail_reads()

    assert {:ok, %{type: :reset}} = Subscriptions.next(subscription, 10_000)

    members =
      collect_until_caught_up(subscription, [])
      |> Enum.map(& &1.document.id)
      |> MapSet.new()

    assert members == MapSet.new(["doc-pruned"])

    Eventual.eventually(
      fn ->
        case SubscriptionHub.count(uuid) do
          1 -> :ok
          _ -> false
        end
      end,
      message: "expected the resettable path to keep the subscription registered"
    )
  end

  test "other integrity violations stay fatal", %{uuid: uuid} do
    assert {:ok, subscription} = open_subscription(uuid)
    assert {:ok, %{type: :caught_up}} = drain_snapshot(subscription)

    set_fail_reads(uuid, ElixirDB.Error.integrity_violation("some other corruption message", %{}))

    parent = self()
    _waiter = spawn(fn -> send(parent, {:next, Subscriptions.next(subscription, 10_000)}) end)

    assert {:ok, %{revision: _}} = put(uuid, "corrupt", %{"value" => 1})

    assert_receive {:next, {:error, %{type: :error, error: error}}}, 10_000
    assert error.code == :integrity_violation

    Eventual.eventually(
      fn ->
        case SubscriptionHub.count(uuid) do
          0 -> :ok
          _ -> false
        end
      end,
      message: "expected fatal integrity violation to unregister the subscription"
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

  defp collect_until_caught_up(subscription, documents) do
    case Subscriptions.next(subscription, 5_000) do
      {:ok, %{type: :reset}} ->
        clear_fail_reads()
        collect_until_caught_up(subscription, documents)

      {:ok, %{type: :snapshot} = event} ->
        collect_until_caught_up(subscription, [event | documents])

      {:ok, %{type: :caught_up}} ->
        Enum.reverse(documents)

      other ->
        flunk("expected reset, snapshot or caught_up, got: #{inspect(other)}")
    end
  end

  defp wait_until_resetting(uuid) do
    [{hub, _}] =
      Registry.lookup(ElixirDB.Runtime.DatabaseRegistry, {:query_subscription_hub, uuid})

    Eventual.eventually(
      fn ->
        case :sys.get_state(hub) do
          %{resetting: true} -> :ok
          _ -> false
        end
      end,
      timeout: 5_000,
      message: "expected the hub to enter the resetting state after the injected batch-prune error"
    )
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
