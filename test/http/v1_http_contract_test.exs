defmodule ElixirDB.HTTP.V1HTTPContractTest do
  use ExUnit.Case, async: false

  alias ElixirDB.HTTP.Router
  alias ElixirDB.JSON.StrictDecoder
  alias ElixirDB.Runtime.DatabaseCatalog
  alias Plug.Conn

  test "HTTP rejects unknown fields and returns the stable error envelope" do
    response =
      request(:post, "/v1/databases", %{"path" => "contract-invalid.elixirdb", "unexpected" => true})

    assert response.status == 400

    assert response.resp_headers
           |> Enum.any?(fn {key, value} -> key == "x-request-id" and value != "" end)

    assert {:ok, %{"error" => %{"code" => "invalid_request", "retryable" => false}}} =
             StrictDecoder.decode(response.resp_body)
  end

  test "database, query, integrity, and NDJSON changes routes follow V1 envelopes" do
    path = "contract-#{System.unique_integer([:positive])}.elixirdb"
    created = request(:post, "/v1/databases", %{"path" => path})
    assert created.status == 201

    {:ok, %{"data" => %{"database_uuid" => uuid}}} =
      StrictDecoder.decode(created.resp_body)

    on_exit(fn ->
      _ = DatabaseCatalog.close(uuid)
      _ = DatabaseCatalog.unregister(uuid)
      ElixirDB.TempDatabase.cleanup(Path.join(ElixirDB.Config.database_root(), path))
    end)

    put =
      request(:post, "/v1/databases/#{uuid}/documents/put", %{
        "id" => "doc",
        "body" => %{"kind" => "task"}
      })

    assert put.status == 201

    bad_put =
      request(:post, "/v1/databases/#{uuid}/documents/put", %{
        "id" => "bad",
        "body" => %{},
        "unexpected" => true
      })

    assert bad_put.status == 400

    index =
      request(:post, "/v1/databases/#{uuid}/indexes", %{
        "name" => "by-kind",
        "type" => "structured",
        "fields" => [%{"path" => "/kind", "type" => "string", "direction" => "asc"}]
      })

    assert index.status == 201

    query = request(:post, "/v1/databases/#{uuid}/query", %{"selector" => %{"/kind" => "task"}})
    assert query.status == 200

    {:ok, %{"data" => %{"documents" => [%{"id" => "doc"}]}}} =
      StrictDecoder.decode(query.resp_body)

    integrity = request(:post, "/v1/databases/#{uuid}/integrity-check", %{})
    assert integrity.status == 200

    stream =
      request(:post, "/v1/databases/#{uuid}/changes/stream", %{
        "since" => 0,
        "limit" => 10,
        "heartbeat_ms" => 0
      })

    assert stream.status == 200
    assert Conn.get_resp_header(stream, "content-type") |> List.first() =~ "application/x-ndjson"
    assert stream.resp_body =~ "\"type\":\"change\""
    assert stream.resp_body =~ "\"type\":\"caught_up\""
  end

  test "unicode and slash-bearing document ids round-trip through put, get, and changes" do
    path = "contract-ids-#{System.unique_integer([:positive])}.elixirdb"
    created = request(:post, "/v1/databases", %{"path" => path})
    assert created.status == 201

    {:ok, %{"data" => %{"database_uuid" => uuid}}} =
      StrictDecoder.decode(created.resp_body)

    on_exit(fn ->
      _ = DatabaseCatalog.close(uuid)
      _ = DatabaseCatalog.unregister(uuid)
      ElixirDB.TempDatabase.cleanup(Path.join(ElixirDB.Config.database_root(), path))
    end)

    ids = ["café", "日本語", "a/b", "foo%2Fbar", "_foo", "plain-id-1"]

    for id <- ids do
      put =
        request(:post, "/v1/databases/#{uuid}/documents/put", %{
          "id" => id,
          "body" => %{"id_echo" => id}
        })

      assert put.status == 201, "expected 201 for id #{inspect(id)}, got #{put.status}"

      {:ok, %{"data" => %{"revision" => revision}}} = StrictDecoder.decode(put.resp_body)

      get = request(:post, "/v1/databases/#{uuid}/documents/get", %{"id" => id})
      assert get.status == 200

      {:ok, %{"data" => data}} = StrictDecoder.decode(get.resp_body)
      assert data["id"] == id
      assert data["revision"] == revision
      assert data["body"]["id_echo"] == id
    end

    changes =
      request(:post, "/v1/databases/#{uuid}/changes", %{"since" => 0, "limit" => 100})

    assert changes.status == 200
    {:ok, %{"data" => %{"results" => results}}} = StrictDecoder.decode(changes.resp_body)

    changed_ids = MapSet.new(Enum.map(results, & &1["document_id"]))
    assert MapSet.subset?(MapSet.new(ids), changed_ids)
  end

  test "document id grammar table rejects forbidden ids and accepts valid ones" do
    path = "contract-id-grammar-#{System.unique_integer([:positive])}.elixirdb"
    created = request(:post, "/v1/databases", %{"path" => path})
    assert created.status == 201

    {:ok, %{"data" => %{"database_uuid" => uuid}}} =
      StrictDecoder.decode(created.resp_body)

    on_exit(fn ->
      _ = DatabaseCatalog.close(uuid)
      _ = DatabaseCatalog.unregister(uuid)
      ElixirDB.TempDatabase.cleanup(Path.join(ElixirDB.Config.database_root(), path))
    end)

    # Contract from Documents.validate_id/1: empty, NUL, control chars, `_system/`
    # prefix, non-string, and oversized ids are rejected. Underscore prefixes other
    # than `_system/` are allowed. Oversized ids may surface as HTTP 400 or 422
    # depending on whether the typed error is invalid_request or resource_limit.
    cases = [
      {123, [400], "invalid_request"},
      {"", [400], "invalid_request"},
      {"a\0b", [400], "invalid_request"},
      {"a\nb", [400], "invalid_request"},
      {"_system/secret", [400], "invalid_request"},
      {String.duplicate("x", 513), [400, 422], "resource_limit"},
      {"alphanumeric-ok", [201], nil},
      {"café", [201], nil},
      {"a/b", [201], nil},
      {"_foo", [201], nil},
      {"_local", [201], nil}
    ]

    for {id, expected_statuses, expected_code} <- cases do
      response =
        request(:post, "/v1/databases/#{uuid}/documents/put", %{
          "id" => id,
          "body" => %{"ok" => true}
        })

      assert response.status in expected_statuses,
             "id=#{inspect(id)} expected status in #{inspect(expected_statuses)}, got #{response.status}"

      if expected_code do
        assert {:ok, %{"error" => %{"code" => ^expected_code}}} =
                 StrictDecoder.decode(response.resp_body)
      else
        assert {:ok, %{"data" => %{"revision" => _}}} =
                 StrictDecoder.decode(response.resp_body)
      end
    end
  end

  defp request(method, path, body) do
    payload = IO.iodata_to_binary(JSON.encode_to_iodata!(body))

    Plug.Test.conn(method, path, payload)
    |> Conn.put_req_header("content-type", "application/json")
    |> Router.call([])
  end
end
