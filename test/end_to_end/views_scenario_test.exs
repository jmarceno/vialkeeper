defmodule VialKeeper.EndToEnd.ViewsScenarioTest do
  @moduledoc "End-to-end tests for declarative view HTTP behavior."
  use ExUnit.Case, async: false

  @moduletag :integration

  alias VialKeeper.Eventual
  alias VialKeeper.Runtime.DatabaseCatalog
  alias VialKeeper.Views

  test "replication transfers documents only and view state survives reopen" do
    prefix = "views-e2e-#{System.unique_integer([:positive])}"
    source_path = prefix <> "-source.vialkeeper"
    target_path = prefix <> "-target.vialkeeper"

    {:ok, source} = DatabaseCatalog.create(source_path)
    {:ok, target} = DatabaseCatalog.create(target_path)

    on_exit(fn ->
      for {uuid, path} <- [
            {source.database_uuid, source_path},
            {target.database_uuid, target_path}
          ] do
        _ = DatabaseCatalog.close(uuid)
        _ = DatabaseCatalog.unregister(uuid)
        VialKeeper.TempDatabase.cleanup(Path.join(VialKeeper.Config.database_root(), path))
      end
    end)

    assert {:ok, %{"view_id" => view_id}} =
             Views.create(source.database_uuid, %{
               "name" => "live",
               "key" => [%{"path" => "/kind"}],
               "value" => %{"path" => "/value"}
             })

    assert {:ok, %{revision: revision}} =
             VialKeeper.Documents.put(source.database_uuid, %{
               id: "doc",
               body: %{"kind" => "task", "value" => 1}
             })

    await_view(source.database_uuid, view_id, 1)

    assert {:ok, %{status: :completed}} =
             VialKeeper.Replication.one_shot(source.database_uuid, target.database_uuid)

    assert {:ok, %{revision: ^revision, body: %{"kind" => "task", "value" => 1}}} =
             VialKeeper.Documents.get(target.database_uuid, %{id: "doc"})

    assert {:ok, []} = Views.list(target.database_uuid)

    assert {:error, %VialKeeper.Error{code: :view_not_found}} =
             Views.state(target.database_uuid, view_id)

    assert {:ok, %{revision: deleted_revision}} =
             VialKeeper.Documents.delete(source.database_uuid, %{id: "doc", if_revision: revision})

    assert deleted_revision != revision
    await_view(source.database_uuid, view_id, 0)

    assert {:ok, %{status: :completed}} =
             VialKeeper.Replication.one_shot(source.database_uuid, target.database_uuid)

    assert {:error, %VialKeeper.Error{code: :document_not_found}} =
             VialKeeper.Documents.get(target.database_uuid, %{id: "doc"})

    assert :ok = DatabaseCatalog.close(source.database_uuid)
    assert {:ok, [%{"view_id" => ^view_id, "name" => "live"}]} = Views.list(source.database_uuid)
    await_view(source.database_uuid, view_id, 0)
  end

  test "view rows follow the winning revision when replication creates a conflict" do
    prefix = "views-e2e-conflict-#{System.unique_integer([:positive])}"
    source_path = prefix <> "-source.vialkeeper"
    target_path = prefix <> "-target.vialkeeper"

    {:ok, source} = DatabaseCatalog.create(source_path)
    {:ok, target} = DatabaseCatalog.create(target_path)

    on_exit(fn ->
      for {uuid, path} <- [
            {source.database_uuid, source_path},
            {target.database_uuid, target_path}
          ] do
        _ = DatabaseCatalog.close(uuid)
        _ = DatabaseCatalog.unregister(uuid)
        VialKeeper.TempDatabase.cleanup(Path.join(VialKeeper.Config.database_root(), path))
      end
    end)

    assert {:ok, %{"view_id" => view_id}} =
             Views.create(source.database_uuid, %{
               "name" => "winning",
               "key" => [%{"path" => "/kind"}],
               "value" => %{"path" => "/value"}
             })

    assert {:ok, %{revision: source_revision}} =
             VialKeeper.Documents.put(source.database_uuid, %{
               id: "conflicted",
               body: %{"kind" => "task", "value" => "source"}
             })

    assert {:ok, %{revision: target_revision}} =
             VialKeeper.Documents.put(target.database_uuid, %{
               id: "conflicted",
               body: %{"kind" => "task", "value" => "target"}
             })

    assert {:ok, %{status: :completed}} =
             VialKeeper.Replication.one_shot(target.database_uuid, source.database_uuid)

    assert source_revision != target_revision

    assert {:ok, %{revision: winning_revision, body: winning_body, conflicts: conflicts}} =
             VialKeeper.Documents.get(source.database_uuid, %{
               id: "conflicted",
               include_conflicts: true
             })

    assert winning_revision in [source_revision, target_revision]
    assert [losing_revision] = conflicts
    assert losing_revision != winning_revision

    await_view(source.database_uuid, view_id, 1)

    assert {:ok, %{results: [%{"id" => "conflicted", "value" => winning_value}]}} =
             DatabaseCatalog.command(
               source.database_uuid,
               {:command, :query_view, %{"view_id" => view_id, "limit" => 10}}
             )

    assert winning_value == winning_body["value"]
  end

  test "compaction preserves a caught-up view" do
    path = "views-e2e-compact-#{System.unique_integer([:positive])}.vialkeeper"
    {:ok, identity} = DatabaseCatalog.create(path)
    uuid = identity.database_uuid

    on_exit(fn ->
      _ = DatabaseCatalog.close(uuid)
      _ = DatabaseCatalog.unregister(uuid)
      VialKeeper.TempDatabase.cleanup(Path.join(VialKeeper.Config.database_root(), path))
    end)

    assert {:ok, %{"view_id" => view_id}} =
             Views.create(uuid, %{
               "name" => "compact",
               "key" => [%{"path" => "/kind"}],
               "value" => %{"path" => "/value"}
             })

    for {id, value} <- [{"a", 1}, {"b", 2}] do
      assert {:ok, _} =
               VialKeeper.Documents.put(uuid, %{id: id, body: %{"kind" => "task", "value" => value}})
    end

    await_view(uuid, view_id, 2)

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

    assert {:ok, %{new_floor: floor}} =
             DatabaseCatalog.command(uuid, {:command, :compact_retention, %{trigger: :explicit}})

    assert floor > 0
    await_view(uuid, view_id, 2)
  end

  defp await_view(uuid, view_id, expected_count) do
    Eventual.eventually(
      fn ->
        case DatabaseCatalog.command(
               uuid,
               {:command, :query_view, %{"view_id" => view_id, "limit" => 10}}
             ) do
          {:ok, %{results: results}} -> length(results) == expected_count
          _ -> false
        end
      end,
      timeout: 5_000,
      message: "view did not converge to expected winning-state rows"
    )
  end
end
