defmodule ElixirDB.Maintenance.FacadeTest do
  @moduledoc "Covers the application facade for database maintenance operations."

  use ExUnit.Case, async: false

  @moduletag :integration

  alias ElixirDB.Documents
  alias ElixirDB.Error
  alias ElixirDB.Maintenance
  alias ElixirDB.Runtime.DatabaseCatalog

  test "unknown databases return typed errors and invalid compact requests are rejected" do
    uuid = ElixirDB.UUID.v4()

    assert {:error, %Error{}} = Maintenance.compact(uuid, %{})
    assert {:error, %Error{}} = Maintenance.integrity_check(uuid)
    assert {:error, %Error{}} = Maintenance.attachment_gc(uuid)

    assert {:error, %Error{code: :invalid_request}} = Maintenance.compact(uuid, [])
  end

  test "compact returns the public retention stats shape" do
    path = "maintenance-facade-#{System.unique_integer([:positive])}.elixirdb"
    assert {:ok, identity} = DatabaseCatalog.create(path)
    uuid = identity.database_uuid

    on_exit(fn -> cleanup(uuid, path) end)

    assert {:ok, _} =
             DatabaseCatalog.command(uuid, {
               :command,
               :update_config,
               %{
                 "retention" => %{
                   "mode" => "stable_frontier",
                   "history_depth" => 0,
                   "peer_expiry_ms" => 86_400_000,
                   "schedule" => "disabled"
                 }
               }
             })

    assert {:ok, _} = Documents.put(uuid, %{id: "doc", body: %{"n" => 1}})
    assert {:ok, stats} = Maintenance.compact(uuid, %{})

    for key <- [
          "noop",
          "old_floor",
          "new_floor",
          "old_compaction_epoch",
          "new_compaction_epoch",
          "removed_changes",
          "removed_revisions",
          "removed_boundaries",
          "active_peer_count",
          "expired_peer_count"
        ] do
      assert Map.has_key?(stats, key), "missing compact stat #{key}"
    end

    assert stats["new_floor"] >= stats["old_floor"]
  end

  defp cleanup(uuid, path) do
    _ = DatabaseCatalog.close(uuid)
    _ = DatabaseCatalog.unregister(uuid)
    root = ElixirDB.Config.database_root()
    ElixirDB.TempDatabase.cleanup(Path.join(root, path))
  end
end
