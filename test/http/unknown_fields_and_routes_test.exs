defmodule ElixirDB.HTTP.UnknownFieldsAndRoutesTest do
  use ExUnit.Case, async: false

  @moduletag :integration

  alias ElixirDB.Runtime.DatabaseCatalog
  alias ElixirDB.TestServer

  test "registration rejects unknown fields" do
    server = TestServer.start_supervised!()

    assert {:ok, %{status: 400, body: body}} =
             Req.post(server.base_url <> "/v1/registrations",
               json: %{"path" => "http-unknown-reg.elixirdb", "extra" => true}
             )

    assert %{"error" => %{"code" => "invalid_request", "retryable" => false}} = body
  end

  test "GET databases plus table-driven unknown fields over Bandit" do
    server = TestServer.start_supervised!()
    path = "http-list-#{System.unique_integer([:positive])}.elixirdb"

    assert {:ok, %{status: 201, body: created}} =
             Req.post(server.base_url <> "/v1/databases", json: %{"path" => path})

    uuid = created["data"]["database_uuid"]

    on_exit(fn ->
      _ = DatabaseCatalog.close(uuid)
      _ = DatabaseCatalog.unregister(uuid)
      ElixirDB.TempDatabase.cleanup(Path.join(ElixirDB.Config.database_root(), path))
    end)

    assert {:ok, %{status: 200, body: listed}} = Req.get(server.base_url <> "/v1/databases")

    assert Enum.any?(List.wrap(listed["data"]), fn entry ->
             entry["database_uuid"] == uuid
           end)

    assert {:ok, %{status: 200}} = Req.get(server.base_url <> "/v1/databases/#{uuid}")

    assert {:ok, %{status: 201, body: put_body}} =
             Req.post(server.base_url <> "/v1/databases/#{uuid}/documents/put",
               json: %{"id" => "seed", "body" => %{"k" => 1}}
             )

    revision = put_body["data"]["revision"]
    replication_id = "rep_" <> Base.encode16(:crypto.strong_rand_bytes(4), case: :lower)

    cases = [
      {:post, "/v1/databases/#{uuid}/documents/put", %{"id" => "x", "body" => %{}, "nope" => 1}},
      {:post, "/v1/databases/#{uuid}/documents/get", %{"id" => "seed", "extra" => true}},
      {:post, "/v1/databases/#{uuid}/documents/delete",
       %{"id" => "seed", "if_revision" => revision, "noise" => 1}},
      {:post, "/v1/databases/#{uuid}/documents/resolve",
       %{
         "id" => "seed",
         "expected_live_revisions" => [revision],
         "chosen_parent_revision" => revision,
         "body" => %{},
         "mystery" => true
       }},
      {:post, "/v1/databases/#{uuid}/indexes",
       %{
         "name" => "by-kind",
         "type" => "structured",
         "fields" => [%{"path" => "/kind", "type" => "string", "direction" => "asc"}],
         "unexpected" => true
       }},
      {:post, "/v1/databases/#{uuid}/query", %{"selector" => %{"/kind" => "task"}, "bonus" => 1}},
      {:post, "/v1/databases/#{uuid}/query",
       %{"search" => %{"index" => "missing", "text" => "term", "mode" => "wildcard"}}},
      {:post, "/v1/databases/#{uuid}/changes",
       %{"since" => 0, "limit" => 10, "wait_ms" => 0, "extra" => true}},
      {:post, "/v1/databases/#{uuid}/changes/stream",
       %{"since" => 0, "limit" => 10, "heartbeat_ms" => 0, "noise" => true}},
      {:post, "/v1/databases/#{uuid}/query/stream",
       %{"query" => %{"selector" => %{}}, "heartbeat_ms" => 1000, "noise" => true}},
      {:post, "/v1/databases/#{uuid}/replications",
       %{
         "persist" => true,
         "mode" => "one_shot",
         "direction" => "push",
         "endpoint" => %{
           "kind" => "remote",
           "base_url" => "http://127.0.0.1:9",
           "database_uuid" => ElixirDB.UUID.v4()
         },
         "enabled" => false,
         "mystery" => true
       }},
      {:post, "/v1/databases/#{uuid}/replication/changes",
       %{"since" => 0, "limit" => 10, "wait_ms" => 0, "extra" => true}},
      {:post, "/v1/databases/#{uuid}/replication/revisions/diff",
       %{"documents" => [], "extra" => true}},
      {:post, "/v1/databases/#{uuid}/replication/revisions/get",
       %{"documents" => [], "extra" => true}},
      {:post, "/v1/databases/#{uuid}/replication/revisions/put",
       %{"chains" => [], "extra" => true}},
      {:put, "/v1/databases/#{uuid}/replication/checkpoints/#{replication_id}",
       %{
         "expected_checkpoint_version" => 0,
         "version" => 1,
         "checkpoint_version" => 1,
         "replication_id" => replication_id,
         "session_id" => ElixirDB.UUID.v4(),
         "source_sequence" => 0,
         "history" => [],
         "extra" => true
       }}
    ]

    for {method, route, body} <- cases do
      assert {:ok, %{status: 400, body: resp}} =
               Req.request(method: method, url: server.base_url <> route, json: body)

      assert resp["error"]["code"] == "invalid_request",
             "#{method} #{route} expected invalid_request, got #{inspect(resp)}"

      assert resp["error"]["retryable"] == false
    end
  end
end
