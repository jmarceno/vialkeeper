defmodule ElixirDB.Replication.CompactBootstrapTest do
  use ExUnit.Case, async: false

  @moduletag :integration

  alias ElixirDB.MapAccess
  alias ElixirDB.Replication
  alias ElixirDB.Replication.LocalEndpoint
  alias ElixirDB.Runtime.DatabaseCatalog

  @retention_config %{
    "retention" => %{
      "mode" => "stable_frontier",
      "history_depth" => 0,
      "peer_expiry_ms" => 86_400_000,
      "schedule" => "disabled"
    }
  }

  setup do
    prefix = "compact-boot-#{System.unique_integer([:positive])}"
    a_path = prefix <> "-a.elixirdb"
    b_path = prefix <> "-b.elixirdb"
    root = ElixirDB.Config.database_root()

    for path <- [a_path, b_path] do
      ElixirDB.TempDatabase.cleanup(Path.join(root, path))
    end

    {:ok, a} = DatabaseCatalog.create(a_path)
    {:ok, b} = DatabaseCatalog.create(b_path)

    assert {:ok, _} =
             DatabaseCatalog.command(a.database_uuid, {:command, :update_config, @retention_config})

    on_exit(fn ->
      for {identity, path} <- [{a, a_path}, {b, b_path}] do
        _ = DatabaseCatalog.close(identity.database_uuid)
        _ = DatabaseCatalog.unregister(identity.database_uuid)
        ElixirDB.TempDatabase.cleanup(Path.join(root, path))
      end
    end)

    {:ok, a: a, b: b}
  end

  test "target bootstraps after source compaction advances floor beyond checkpoint", %{a: a, b: b} do
    assert {:ok, _} =
             ElixirDB.Documents.put(a.database_uuid, %{id: "first", body: %{"n" => 1}})

    assert {:ok, %{status: :completed}} = Replication.one_shot(a.database_uuid, b.database_uuid)

    assert {:ok, %{body: %{"n" => 1}}} =
             ElixirDB.Documents.get(b.database_uuid, %{id: "first"})

    assert {:ok, _} =
             ElixirDB.Documents.put(a.database_uuid, %{id: "second", body: %{"n" => 2}})

    assert {:ok, %{current_sequence: 2}} =
             DatabaseCatalog.command(a.database_uuid, {:command, :identity, %{}})

    assert {:ok, _} = update_peer_safe(a.database_uuid, b.database_uuid, 2)

    assert {:ok, %{new_floor: 2}} =
             DatabaseCatalog.command(a.database_uuid, {:command, :compact_retention, %{}})

    assert {:error, %ElixirDB.Error{code: :history_truncated}} =
             DatabaseCatalog.command(
               a.database_uuid,
               {:command, :read_changes, %{since: 0, limit: 10}}
             )

    assert {:error, %ElixirDB.Error{code: :document_not_found}} =
             ElixirDB.Documents.get(b.database_uuid, %{id: "second"})

    {:ok, source} = LocalEndpoint.new(a.database_uuid)
    {:ok, target} = LocalEndpoint.new(b.database_uuid)

    assert {:ok, handshake} =
             Replication.handshake(source, target, %{mode: "one_shot", direction: "push"})

    assert handshake.bootstrap_required
    assert handshake.reconcile_reason == :below_floor

    assert {:ok, %{status: :completed}} = Replication.one_shot(a.database_uuid, b.database_uuid)

    assert {:ok, %{body: %{"n" => 2}}} =
             ElixirDB.Documents.get(b.database_uuid, %{id: "second"})
  end

  test "handshake reconcile marks bootstrap when checkpoint is below retention floor", %{a: a, b: b} do
    assert {:ok, _} =
             ElixirDB.Documents.put(a.database_uuid, %{id: "floor", body: %{"n" => 1}})

    assert {:ok, %{status: :completed}} = Replication.one_shot(a.database_uuid, b.database_uuid)

    assert {:ok, _} =
             ElixirDB.Documents.put(a.database_uuid, %{id: "after-floor", body: %{"n" => 2}})

    assert {:ok, _} = update_peer_safe(a.database_uuid, b.database_uuid, 2)

    assert {:ok, %{new_floor: 2}} =
             DatabaseCatalog.command(a.database_uuid, {:command, :compact_retention, %{}})

    {:ok, source} = LocalEndpoint.new(a.database_uuid)
    {:ok, target} = LocalEndpoint.new(b.database_uuid)

    assert {:ok, context} =
             Replication.handshake(source, target, %{mode: "one_shot", direction: "push"})

    assert context.bootstrap_required
    assert context.reconcile_reason == :below_floor
    assert context.since == 2
  end

  defp update_peer_safe(source_uuid, peer_uuid, safe_sequence) do
    {:ok, identity} = DatabaseCatalog.command(source_uuid, {:command, :identity, %{}})

    {:ok, record} =
      DatabaseCatalog.command(source_uuid, {:command, :get_local_record, "peer_ledger", peer_uuid})

    version = MapAccess.get(record, :version, 0)
    value = MapAccess.get(record, :value, %{})
    epoch = MapAccess.get(identity, :history_epoch)
    future = DateTime.utc_now() |> DateTime.add(60, :second) |> DateTime.to_iso8601()

    DatabaseCatalog.command(
      source_uuid,
      {:command, :put_peer_position_cas,
       %{
         expected_version: version,
         value:
           Map.merge(value, %{
             "peer_database_uuid" => peer_uuid,
             "source_database_uuid" => source_uuid,
             "source_history_epoch" => epoch,
             "safe_source_sequence" => safe_sequence,
             "last_seen_at" => DateTime.utc_now() |> DateTime.to_iso8601(),
             "lease_expires_at" => future,
             "status" => "active"
           })
       }}
    )
  end
end
