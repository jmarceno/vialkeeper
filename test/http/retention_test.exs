defmodule ElixirDB.HTTP.RetentionTest do
  @moduledoc "Covers HTTP retention identity and handshake behavior."

  use ExUnit.Case, async: false

  @moduletag :integration

  alias ElixirDB.Runtime.DatabaseCatalog
  alias ElixirDB.TestServer

  test "identity includes retention handshake fields" do
    server = TestServer.start_supervised!()
    path = "retention-identity-#{System.unique_integer([:positive])}.elixirdb"

    {:ok, %{status: 201, body: created}} =
      Req.post(server.base_url <> "/v1/databases", json: %{"path" => path})

    uuid = created["data"]["database_uuid"]

    on_exit(fn -> cleanup(uuid, path) end)

    {:ok, %{status: 200, body: body}} =
      ElixirDB.TestReplicationWire.request(
        :get,
        server.base_url <> "/v1/databases/#{uuid}/replication/identity"
      )

    data = body["data"]

    for key <- [
          "database_uuid",
          "history_epoch",
          "current_sequence",
          "retention_floor",
          "compaction_epoch",
          "retention_boundary_digest",
          "retention_mode",
          "replication_protocol_major",
          "revision_algorithm_version",
          "canonicalization_version"
        ] do
      assert Map.has_key?(data, key), "missing identity field #{key}"
    end

    assert data["database_uuid"] == uuid
    assert data["retention_mode"] == "disabled"
  end

  test "compact returns bounded retention stats" do
    server = TestServer.start_supervised!()
    path = "retention-compact-#{System.unique_integer([:positive])}.elixirdb"

    {:ok, %{status: 201, body: created}} =
      Req.post(server.base_url <> "/v1/databases", json: %{"path" => path})

    uuid = created["data"]["database_uuid"]
    on_exit(fn -> cleanup(uuid, path) end)

    assert {:ok, %{status: 200}} =
             Req.put(server.base_url <> "/v1/databases/#{uuid}/config",
               json: %{
                 "retention" => %{
                   "mode" => "stable_frontier",
                   "history_depth" => 0,
                   "peer_expiry_ms" => 86_400_000,
                   "schedule" => "disabled"
                 }
               }
             )

    assert {:ok, %{status: 201}} =
             Req.post(server.base_url <> "/v1/databases/#{uuid}/documents/put",
               json: %{"id" => "doc", "body" => %{"n" => 1}}
             )

    {:ok, %{status: 200, body: compact}} =
      Req.post(server.base_url <> "/v1/databases/#{uuid}/compact", json: %{})

    data = compact["data"]

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
      assert Map.has_key?(data, key), "missing compact stat #{key}"
    end

    assert data["new_floor"] >= data["old_floor"]
  end

  test "changes below retention floor return HTTP 410 history_truncated" do
    server = TestServer.start_supervised!()
    path = "retention-truncated-#{System.unique_integer([:positive])}.elixirdb"

    {:ok, %{status: 201, body: created}} =
      Req.post(server.base_url <> "/v1/databases", json: %{"path" => path})

    uuid = created["data"]["database_uuid"]
    on_exit(fn -> cleanup(uuid, path) end)

    assert {:ok, %{status: 200}} =
             Req.put(server.base_url <> "/v1/databases/#{uuid}/config",
               json: %{
                 "retention" => %{
                   "mode" => "stable_frontier",
                   "history_depth" => 0,
                   "peer_expiry_ms" => 86_400_000,
                   "schedule" => "disabled"
                 }
               }
             )

    assert {:ok, %{status: 201}} =
             Req.post(server.base_url <> "/v1/databases/#{uuid}/documents/put",
               json: %{"id" => "doc", "body" => %{"n" => 1}}
             )

    assert {:ok, %{status: 200}} =
             Req.post(server.base_url <> "/v1/databases/#{uuid}/compact", json: %{})

    {:ok, %{status: 410, body: body}} =
      Req.post(server.base_url <> "/v1/databases/#{uuid}/changes",
        json: %{"since" => 0, "limit" => 10, "wait_ms" => 0}
      )

    assert body["error"]["code"] == "history_truncated"
    assert is_map(body["error"]["details"])
    assert body["error"]["details"]["retention_floor"] >= 1
  end

  test "boundary page endpoint returns API-015 shape" do
    server = TestServer.start_supervised!()
    path = "retention-boundaries-#{System.unique_integer([:positive])}.elixirdb"

    {:ok, %{status: 201, body: created}} =
      Req.post(server.base_url <> "/v1/databases", json: %{"path" => path})

    uuid = created["data"]["database_uuid"]
    on_exit(fn -> cleanup(uuid, path) end)

    {:ok, %{status: 200, body: body}} =
      ElixirDB.TestReplicationWire.request(
        :post,
        server.base_url <> "/v1/databases/#{uuid}/replication/boundaries",
        %{}
      )

    data = body["data"]

    for key <- [
          "source_history_epoch",
          "compaction_epoch",
          "boundary_digest",
          "next_page",
          "boundaries"
        ] do
      assert Map.has_key?(data, key)
    end

    assert is_list(data["boundaries"])
  end

  defp cleanup(uuid, path) do
    _ = DatabaseCatalog.close(uuid)
    _ = DatabaseCatalog.unregister(uuid)
    root = ElixirDB.Config.database_root()
    ElixirDB.TempDatabase.cleanup(Path.join(root, path))
  end
end
