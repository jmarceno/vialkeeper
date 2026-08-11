defmodule ElixirDB.HTTP.FederationTest do
  use ExUnit.Case, async: false

  alias ElixirDB.Documents
  alias ElixirDB.Federation.Normalizer
  alias ElixirDB.Runtime.DatabaseCatalog
  alias ElixirDB.TestServer

  setup do
    root = ElixirDB.Config.database_root()
    first_path = "federation-http-a-#{System.unique_integer([:positive])}.elixirdb"
    second_path = "federation-http-b-#{System.unique_integer([:positive])}.elixirdb"

    Enum.each([first_path, second_path], fn path ->
      ElixirDB.TempDatabase.cleanup(Path.join(root, path))
    end)

    assert {:ok, first} = DatabaseCatalog.create(first_path)
    assert {:ok, second} = DatabaseCatalog.create(second_path)

    on_exit(fn ->
      for {identity, path} <- [{first, first_path}, {second, second_path}] do
        _ = DatabaseCatalog.close(identity.database_uuid)
        _ = DatabaseCatalog.unregister(identity.database_uuid)
        ElixirDB.TempDatabase.cleanup(Path.join(root, path))
      end
    end)

    server = TestServer.start_supervised!()

    {:ok, server: server, first_uuid: first.database_uuid, second_uuid: second.database_uuid}
  end

  test "executes explicit federation queries and resumes with the opaque bookmark", %{
    server: server,
    first_uuid: first_uuid,
    second_uuid: second_uuid
  } do
    seed_documents(first_uuid, [{"a", 1}, {"c", 3}])
    seed_documents(second_uuid, [{"b", 2}, {"d", 4}])

    query = %{
      "selector" => %{"/kind" => "task"},
      "fields" => ["/value"],
      "sort" => [%{"path" => "/value", "direction" => "asc"}],
      "limit" => 2
    }

    assert {:ok, %{status: 200, body: first_page}} =
             Req.post(server.base_url <> "/v1/federation/query",
               json: %{"databases" => [first_uuid, second_uuid], "query" => query}
             )

    assert %{
             "documents" => first_documents,
             "sources" => sources,
             "bookmark" => bookmark
           } = first_page["data"]

    assert Enum.map(first_documents, & &1["id"]) == ["a", "b"]
    assert Enum.all?(first_documents, &is_binary(&1["source_database_uuid"]))
    assert Enum.all?(first_documents, &Map.has_key?(&1, "fields"))
    assert Enum.all?(first_documents, &(not Map.has_key?(&1, "body")))
    assert Enum.all?(first_documents, &(&1["fields"] in [%{"/value" => 1}, %{"/value" => 2}]))
    assert Enum.map(sources, & &1["database_uuid"]) == [first_uuid, second_uuid]
    assert Enum.all?(sources, &is_integer(&1["sequence"]))
    assert is_binary(bookmark)

    assert {:ok, %{status: 200, body: second_page}} =
             Req.post(server.base_url <> "/v1/federation/query",
               json: %{
                 "databases" => [first_uuid, second_uuid],
                 "query" => Map.put(query, "bookmark", bookmark)
               }
             )

    assert Enum.map(second_page["data"]["documents"], & &1["id"]) == ["c", "d"]
    assert second_page["data"]["bookmark"] == nil
  end

  test "rejects unsupported top-level fields at the HTTP boundary", %{server: server} do
    assert {:ok, %{status: 400, body: body}} =
             Req.post(server.base_url <> "/v1/federation/query",
               json: %{
                 "databases" => ["123e4567-e89b-12d3-a456-426614174000"],
                 "query" => %{"limit" => 1},
                 "unexpected" => true
               }
             )

    assert body["error"]["code"] == "invalid_request"
    assert body["error"]["retryable"] == false
  end

  test "lists clean saved definitions and executes only their allowed overrides", %{
    server: server,
    first_uuid: first_uuid
  } do
    seed_documents(first_uuid, [{"open", 1}])

    assert {:ok, normalized} =
             Normalizer.normalize(%{
               databases: [first_uuid],
               query: %{
                 selector: %{"/kind" => "task"},
                 fields: ["/value"],
                 sort: [%{path: "/value", direction: "asc"}],
                 limit: 5
               }
             })

    saved = %{
      name: "open-tasks",
      databases: normalized.databases,
      query: normalized.query,
      fingerprint: normalized.fingerprint
    }

    previous = Application.get_env(:elixir_db, :federation_saved_queries, :missing)
    Application.put_env(:elixir_db, :federation_saved_queries, [saved])

    on_exit(fn ->
      case previous do
        :missing -> Application.delete_env(:elixir_db, :federation_saved_queries)
        value -> Application.put_env(:elixir_db, :federation_saved_queries, value)
      end
    end)

    assert {:ok, %{status: 200, body: listed}} =
             Req.get(server.base_url <> "/v1/federation/saved-queries")

    assert [entry] = listed["data"]
    assert Map.keys(entry) |> Enum.sort() == ["name", "query", "sources"]
    assert entry["name"] == "open-tasks"
    assert entry["sources"] == [first_uuid]
    assert entry["query"]["selector"] == %{"/kind" => "task"}
    assert entry["query"]["fields"] == ["/value"]
    assert entry["query"]["sort"] == [%{"path" => "/value", "direction" => "asc"}]
    assert entry["query"]["limit"] == 5
    refute Map.has_key?(entry, "fingerprint")
    refute Map.has_key?(entry["query"], "bookmark")
    refute Map.has_key?(entry["query"], "predicate")

    assert {:ok, %{status: 200, body: executed}} =
             Req.post(server.base_url <> "/v1/federation/saved-queries/execute",
               json: %{"name" => "open-tasks", "limit" => 1, "bookmark" => nil}
             )

    assert [document] = executed["data"]["documents"]
    assert document["id"] == "open"
    assert document["fields"] == %{"/value" => 1}
    assert document["source_database_uuid"] == first_uuid

    assert {:ok, %{status: 400, body: unknown}} =
             Req.post(server.base_url <> "/v1/federation/saved-queries/execute",
               json: %{"name" => "open-tasks", "databases" => [first_uuid]}
             )

    assert unknown["error"]["code"] == "invalid_request"
  end

  test "loads an unregistered saved source and fails only when executed", %{server: server} do
    missing_uuid = "123e4567-e89b-12d3-a456-426614174099"

    assert {:ok, normalized} =
             Normalizer.normalize(%{
               databases: [missing_uuid],
               query: %{selector: %{}, limit: 1}
             })

    saved = %{
      name: "missing-source",
      databases: normalized.databases,
      query: normalized.query,
      fingerprint: normalized.fingerprint
    }

    previous = Application.get_env(:elixir_db, :federation_saved_queries, :missing)
    Application.put_env(:elixir_db, :federation_saved_queries, [saved])

    on_exit(fn ->
      case previous do
        :missing -> Application.delete_env(:elixir_db, :federation_saved_queries)
        value -> Application.put_env(:elixir_db, :federation_saved_queries, value)
      end
    end)

    assert {:ok, %{status: 200, body: listed}} =
             Req.get(server.base_url <> "/v1/federation/saved-queries")

    assert [%{"name" => "missing-source"}] = listed["data"]

    assert {:ok, %{status: 404, body: executed}} =
             Req.post(server.base_url <> "/v1/federation/saved-queries/execute",
               json: %{"name" => "missing-source"}
             )

    assert executed["error"]["code"] == "database_not_registered"
  end

  defp seed_documents(uuid, values) do
    Enum.each(values, fn {id, value} ->
      assert {:ok, _} =
               Documents.put(uuid, %{
                 id: id,
                 body: %{"kind" => "task", "value" => value}
               })
    end)
  end
end
