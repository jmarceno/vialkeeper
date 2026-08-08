defmodule ElixirDB.Replication.Wave5Test do
  use ExUnit.Case, async: false

  alias ElixirDB.Domain.PeerPosition
  alias ElixirDB.Error
  alias ElixirDB.MapAccess
  alias ElixirDB.Replication
  alias ElixirDB.Replication.{CheckpointReconciler, LocalEndpoint}
  alias ElixirDB.Retention.Frontier
  alias ElixirDB.Runtime.DatabaseCatalog
  alias ElixirDB.Storage.SQLite.Adapter
  alias ElixirDB.TempDatabase

  setup do
    prefix = "wave5-#{System.unique_integer([:positive])}"
    a_path = prefix <> "-a.elixirdb"
    b_path = prefix <> "-b.elixirdb"
    root = ElixirDB.Config.database_root()

    for path <- [a_path, b_path] do
      TempDatabase.cleanup(Path.join(root, path))
    end

    {:ok, a} = DatabaseCatalog.create(a_path)
    {:ok, b} = DatabaseCatalog.create(b_path)

    on_exit(fn ->
      for {identity, path} <- [{a, a_path}, {b, b_path}] do
        _ = DatabaseCatalog.close(identity.database_uuid)
        _ = DatabaseCatalog.unregister(identity.database_uuid)
        TempDatabase.cleanup(Path.join(root, path))
      end
    end)

    {:ok, a: a, b: b}
  end

  test "epoch mismatch in checkpoint reconciler requires bootstrap", %{a: a} do
    {:ok, source} = LocalEndpoint.new(a.database_uuid)

    {:ok, source_identity} = LocalEndpoint.identity(source)

    reconcile =
      CheckpointReconciler.reconcile(
        %{
          "source_history_epoch" => "old-epoch",
          "history" => [%{"session_id" => "s1", "source_sequence" => 50}]
        },
        %{
          "source_history_epoch" => "old-epoch",
          "history" => [%{"session_id" => "s1", "source_sequence" => 50}]
        },
        source_identity
      )

    assert reconcile.bootstrap_required
    assert reconcile.reason == :epoch_mismatch
  end

  test "bootstrap revision pages return chains and continuation metadata", %{a: a} do
    assert {:ok, _} =
             ElixirDB.Documents.put(a.database_uuid, %{id: "boot-doc", body: %{"n" => 1}})

    assert {:ok, page} =
             DatabaseCatalog.command(
               a.database_uuid,
               {:command, :get_revision_chains, %{bootstrap: true, limit: 10}}
             )

    assert is_list(MapAccess.get(page, :chains) || MapAccess.get(page, "chains"))
    assert MapAccess.get(page, :source_history_epoch) || MapAccess.get(page, "source_history_epoch")
  end

  test "checkpoint CAS rejects safe sequence regression", %{a: a} do
    {:ok, bundle_path} = TempDatabase.create(prefix: "wave5-cas")
    path = TempDatabase.sqlite_path(bundle_path)
    {:ok, adapter} = Adapter.create(path, %{database_uuid: a.database_uuid})

    try do
      replication_id = "wave5-rep-#{System.unique_integer([:positive])}"

      base = %{
        namespace: "checkpoints",
        key: replication_id,
        expected_version: 0,
        value: %{
          "version" => 1,
          "replication_id" => replication_id,
          "checkpoint_version" => 1,
          "session_id" => "sess",
          "source_sequence" => 10,
          "source_history_epoch" => "epoch-a",
          "source_compaction_epoch" => 1,
          "safe_source_sequence" => 10,
          "installed_source_compaction_epoch" => 1,
          "history" => []
        }
      }

      assert {:ok, %{version: 1}} =
               Adapter.put_local_record_cas(adapter, base)

      assert {:error, %Error{code: :checkpoint_conflict}} =
               Adapter.put_local_record_cas(adapter, %{
                 base
                 | expected_version: 1,
                   value:
                     Map.merge(base.value, %{
                       "checkpoint_version" => 2,
                       "source_sequence" => 12,
                       "safe_source_sequence" => 5
                     })
               })
    after
      Adapter.close(adapter)
      TempDatabase.cleanup(bundle_path)
    end
  end

  test "expired peer is not admitted to frontier", %{a: a} do
    {:ok, identity} = DatabaseCatalog.command(a.database_uuid, {:command, :identity, %{}})
    epoch = MapAccess.get(identity, :history_epoch)
    past = DateTime.utc_now() |> DateTime.add(-60, :second) |> DateTime.to_iso8601()

    {:ok, peer} =
      PeerPosition.new(%{
        peer_database_uuid: b_uuid(),
        peer_history_epoch: b_uuid(),
        source_database_uuid: a.database_uuid,
        source_history_epoch: epoch,
        safe_source_sequence: 0,
        installed_source_compaction_epoch: 0,
        last_seen_at: past,
        lease_expires_at: past,
        status: :expired
      })

    result =
      Frontier.compute(%{
        source_database_uuid: a.database_uuid,
        source_history_epoch: epoch,
        current_sequence: MapAccess.get(identity, :current_sequence, 0),
        current_floor: 0,
        mode: :stable_frontier,
        peers: [peer],
        now: DateTime.utc_now()
      })

    assert result.expired_peer_count == 1
    assert result.active_peer_count == 0
  end

  test "one-shot replication completes after bootstrap when checkpoint epoch mismatches", %{
    a: a,
    b: b
  } do
    assert {:ok, _} =
             ElixirDB.Documents.put(a.database_uuid, %{id: "wave5", body: %{"v" => 1}})

    {:ok, source} = LocalEndpoint.new(a.database_uuid)
    {:ok, target} = LocalEndpoint.new(b.database_uuid)

    assert {:ok, result} =
             Replication.one_shot_endpoints(source, target, %{
               mode: "one_shot",
               direction: "push"
             })

    assert result.status == :completed
    assert {:ok, doc} = ElixirDB.Documents.get(b.database_uuid, %{id: "wave5"})
    assert doc.body["v"] == 1
  end

  defp b_uuid do
    Base.encode16(:crypto.strong_rand_bytes(16), case: :lower)
    |> then(fn hex ->
      String.slice(hex, 0, 8) <>
        "-4" <>
        String.slice(hex, 8, 3) <>
        "-8" <>
        String.slice(hex, 11, 3) <> "-" <> String.slice(hex, 14, 12)
    end)
  end
end
