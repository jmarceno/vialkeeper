defmodule ElixirDB.HTTP.MaterializedViewsTest do
  @moduledoc "Covers the materialized-view HTTP contract and generated-data access."
  use ExUnit.Case, async: false

  alias ElixirDB.Eventual
  alias ElixirDB.JSON.Canonical
  alias ElixirDB.Runtime.DatabaseCatalog
  alias ElixirDB.TestServer

  test "creates, reports, queries, indexes, and views generated documents" do
    server = TestServer.start_supervised!()
    source_path = "http-materialized-source-#{System.unique_integer([:positive])}.elixirdb"
    source_abs = Path.join(ElixirDB.Config.database_root(), source_path)
    ElixirDB.TempDatabase.cleanup(source_abs)
    on_exit(fn -> ElixirDB.TempDatabase.cleanup(source_abs) end)

    assert {:ok, %{status: 201, body: %{"data" => %{"database_uuid" => source_uuid}}}} =
             Req.post(server.base_url <> "/v1/databases", json: %{"path" => source_path})

    assert {:ok, %{status: 201}} =
             Req.post(server.base_url <> "/v1/databases/#{source_uuid}/documents/put",
               json: %{"id" => "one", "body" => %{"kind" => "sale", "amount" => 7}}
             )

    assert {:ok, %{status: 400, body: %{"error" => %{"code" => "invalid_request"}}}} =
             Req.post(server.base_url <> "/v1/materialized-views",
               json: %{"name" => "invalid", "unexpected" => true}
             )

    request = %{
      "name" => "HTTP Sales",
      "sources" => [source_uuid],
      "map" => %{
        "key" => [%{"path" => "/kind"}],
        "value" => %{"path" => "/amount"}
      }
    }

    assert {:ok, %{status: 201, body: %{"data" => created}}} =
             Req.post(server.base_url <> "/v1/materialized-views", json: request)

    derived_uuid = created["database_uuid"]

    on_exit(fn ->
      cleanup_databases([{source_uuid, source_path}, {derived_uuid, created["database_path"]}])
    end)

    assert created["database_kind"] == "derived"
    assert created["database_path"] =~ "_derived/http-sales--"
    assert created["database_path"] =~ ".derived.elixirdb"
    assert is_binary(created["definition_digest"])

    assert {:ok, %{status: 200, body: %{"data" => listed}}} =
             Req.get(server.base_url <> "/v1/materialized-views")

    assert [%{"database_uuid" => ^derived_uuid}] =
             Enum.filter(listed, &(&1["database_uuid"] == derived_uuid))

    assert Eventual.eventually(
             fn ->
               case Req.post(
                      server.base_url <> "/v1/databases/#{derived_uuid}/documents/get",
                      json: %{"id" => map_id(source_uuid, "one")}
                    ) do
                 {:ok, %{status: 200, body: %{"data" => %{"body" => %{"value" => 7}}}}} ->
                   true

                 _ ->
                   false
               end
             end,
             message: "HTTP materializer did not generate the mapped document"
           )

    assert {:ok, %{status: 200, body: %{"data" => detail}}} =
             Req.get(server.base_url <> "/v1/materialized-views/#{derived_uuid}")

    assert detail["definition_digest"] == created["definition_digest"]

    assert detail["sources"] |> hd() |> Map.take(["source_database_uuid", "checkpoint_sequence"]) ==
             %{"source_database_uuid" => source_uuid, "checkpoint_sequence" => 1}

    refute IO.iodata_to_binary(JSON.encode_to_iodata!(detail)) =~ "source_document_id"

    assert {:ok, %{status: 201, body: %{"data" => index}}} =
             Req.post(server.base_url <> "/v1/databases/#{derived_uuid}/indexes",
               json: %{
                 "name" => "by-source",
                 "type" => "structured",
                 "fields" => [%{"path" => "/source_database_uuid", "type" => "string"}]
               }
             )

    assert is_binary(index["index_id"])

    assert {:ok, %{status: 200, body: %{"data" => query}}} =
             Req.post(server.base_url <> "/v1/databases/#{derived_uuid}/query",
               json: %{
                 "selector" => %{"/source_database_uuid" => source_uuid},
                 "index" => "by-source"
               }
             )

    assert [%{"id" => id}] = query["results"]
    assert id == map_id(source_uuid, "one")

    assert {:ok, %{status: 201, body: %{"data" => %{"view_id" => view_id}}}} =
             Req.post(server.base_url <> "/v1/databases/#{derived_uuid}/views",
               json: %{
                 "name" => "generated-by-source",
                 "key" => [%{"path" => "/source_document_id"}]
               }
             )

    assert Eventual.eventually(
             fn ->
               case Req.post(
                      server.base_url <> "/v1/databases/#{derived_uuid}/views/#{view_id}/query",
                      json: %{"consistency" => "consistent"}
                    ) do
                 {:ok, %{status: 200, body: %{"data" => %{"results" => [%{"id" => ^id}]}}}} ->
                   true

                 _ ->
                   false
               end
             end,
             message: "local view did not index generated documents"
           )

    cleanup_databases([{source_uuid, source_path}, {derived_uuid, created["database_path"]}])
  end

  test "enforces lifecycle actions and disabled refresh rules" do
    server = TestServer.start_supervised!()
    source_path = "http-materialized-lifecycle-#{System.unique_integer([:positive])}.elixirdb"
    source_abs = Path.join(ElixirDB.Config.database_root(), source_path)
    ElixirDB.TempDatabase.cleanup(source_abs)
    on_exit(fn -> ElixirDB.TempDatabase.cleanup(source_abs) end)

    assert {:ok, %{status: 201, body: %{"data" => %{"database_uuid" => source_uuid}}}} =
             Req.post(server.base_url <> "/v1/databases", json: %{"path" => source_path})

    assert {:ok, %{status: 201, body: %{"data" => created}}} =
             Req.post(server.base_url <> "/v1/materialized-views",
               json: %{
                 "name" => "Lifecycle",
                 "sources" => [source_uuid],
                 "map" => %{"key" => [%{"path" => "/kind"}]}
               }
             )

    derived_uuid = created["database_uuid"]

    on_exit(fn ->
      cleanup_databases([{source_uuid, source_path}, {derived_uuid, created["database_path"]}])
    end)

    assert {:ok, %{status: 200, body: %{"data" => disabled}}} =
             Req.post(server.base_url <> "/v1/materialized-views/#{derived_uuid}/disable",
               json: %{}
             )

    assert disabled["enabled"] == false
    assert disabled["runtime_status"] == "stopped"

    for action <- ["refresh", "rebuild"] do
      assert {:ok, %{status: 400, body: %{"error" => %{"code" => "invalid_request"}}}} =
               Req.post(
                 server.base_url <> "/v1/materialized-views/#{derived_uuid}/#{action}",
                 json: %{}
               )
    end

    assert {:ok, %{status: 200, body: %{"data" => enabled}}} =
             Req.post(server.base_url <> "/v1/materialized-views/#{derived_uuid}/enable",
               json: %{}
             )

    assert enabled["enabled"] == true

    assert {:ok, %{status: 200, body: %{"data" => refreshed}}} =
             Req.post(server.base_url <> "/v1/materialized-views/#{derived_uuid}/refresh",
               json: %{}
             )

    assert refreshed["accepted"] == true

    assert {:ok, %{status: 200, body: %{"data" => rebuilt}}} =
             Req.post(server.base_url <> "/v1/materialized-views/#{derived_uuid}/rebuild",
               json: %{}
             )

    assert rebuilt["accepted"] == true

    assert {:ok, %{status: 400, body: %{"error" => %{"code" => "invalid_request"}}}} =
             Req.post(server.base_url <> "/v1/materialized-views/#{derived_uuid}/disable",
               json: %{"unexpected" => true}
             )

    cleanup_databases([{source_uuid, source_path}, {derived_uuid, created["database_path"]}])
  end

  defp map_id(source_uuid, document_id) do
    digest = :crypto.hash(:sha256, Canonical.encode!([source_uuid, document_id]))
    "m-" <> Base.encode16(digest, case: :lower)
  end

  defp cleanup_databases(databases) do
    Enum.each(databases, fn {uuid, path} ->
      disable_derived(uuid)
      _ = DatabaseCatalog.close(uuid)
      _ = DatabaseCatalog.unregister(uuid)
      ElixirDB.TempDatabase.cleanup(Path.join(ElixirDB.Config.database_root(), path))
    end)
  end

  defp disable_derived(uuid) do
    case DatabaseCatalog.info(uuid) do
      {:ok, %{"database_kind" => "derived"}} ->
        _ = DatabaseCatalog.command(uuid, {:command, :set_derived_enabled, %{enabled: false}})
        :ok

      {:ok, %{database_kind: :derived}} ->
        _ = DatabaseCatalog.command(uuid, {:command, :set_derived_enabled, %{enabled: false}})
        :ok

      _ ->
        :ok
    end
  end
end
