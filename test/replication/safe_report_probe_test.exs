defmodule ElixirDB.Replication.SafeReportProbeTest do
  use ExUnit.Case, async: false

  alias ElixirDB.MapAccess
  alias ElixirDB.Replication
  alias ElixirDB.Replication.LocalEndpoint
  alias ElixirDB.Runtime.DatabaseCatalog
  alias ElixirDB.Storage.SQLite.Adapter

  @retention_config %{
    "retention" => %{
      "mode" => "stable_frontier",
      "history_depth" => 0,
      "peer_expiry_ms" => 86_400_000,
      "schedule" => "disabled"
    }
  }

  setup do
    prefix = "safe-probe-#{System.unique_integer([:positive])}"
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

  test "adapter reports local-origin changes on target", %{b: b} do
    {:ok, bundle_path} = ElixirDB.TempDatabase.create(prefix: "safe-probe-adapter")
    path = ElixirDB.TempDatabase.sqlite_path(bundle_path)
    {:ok, adapter} = Adapter.create(path, %{database_uuid: b.database_uuid})

    try do
      assert {:ok, false} = Adapter.has_local_origin_changes?(adapter)

      assert {:ok, _} =
               Adapter.apply_local_mutation(adapter, %{
                 operation: :put,
                 document_id: "local",
                 body: %{"n" => 1}
               })

      assert {:ok, true} = Adapter.has_local_origin_changes?(adapter)
    after
      Adapter.close(adapter)
      ElixirDB.TempDatabase.cleanup(bundle_path)
    end
  end

  test "safe position does not advance when target has local-origin mutations", %{a: a, b: b} do
    assert {:ok, _} =
             ElixirDB.Documents.put(a.database_uuid, %{id: "seed", body: %{"n" => 1}})

    assert {:ok, %{status: :completed}} = Replication.one_shot(a.database_uuid, b.database_uuid)

    assert {:ok, _} =
             DatabaseCatalog.command(a.database_uuid, {:command, :compact_retention, %{}})

    {:ok, source} = LocalEndpoint.new(a.database_uuid)
    {:ok, target} = LocalEndpoint.new(b.database_uuid)
    options = %{mode: "one_shot", direction: "push"}

    assert {:ok, false} = LocalEndpoint.has_local_origin_changes?(target)

    assert {:ok, context} = Replication.handshake(source, target, options)
    context = with_boundary_gates(context, source)

    assert {:ok, _} =
             ElixirDB.Documents.put(a.database_uuid, %{id: "next", body: %{"n" => 2}})

    assert {:ok, context} = run_batch(source, target, context, options)

    first_safe = context.safe_source_sequence
    assert first_safe > 0

    assert {:ok, _} =
             ElixirDB.Documents.put(b.database_uuid, %{id: "local-only", body: %{"x" => 1}})

    assert {:ok, true} = LocalEndpoint.has_local_origin_changes?(target)

    assert {:ok, _} =
             ElixirDB.Documents.put(a.database_uuid, %{id: "later", body: %{"n" => 3}})

    assert {:ok, blocked_context} = run_batch(source, target, context, options)

    assert blocked_context.safe_source_sequence == first_safe
  end

  defp with_boundary_gates(context, source) do
    {:ok, source_identity} = LocalEndpoint.identity(source)
    compaction_epoch = MapAccess.get(source_identity, :compaction_epoch, 0)

    %{
      context
      | installed_source_compaction_epoch: compaction_epoch,
        boundaries_installed_through: compaction_epoch
    }
  end

  defp run_batch(source, target, context, options) do
    with {:ok, context} <- Replication.read_changes(source, context, options),
         {:ok, context} <- Replication.diff(target, context, options),
         {:ok, context} <- Replication.fetch_chains(source, context, options),
         {:ok, context} <- Replication.import_chains(target, context, options),
         {:ok, context} <- Replication.checkpoint_target(source, target, context, options) do
      Replication.checkpoint_source(source, context, options)
    end
  end
end
