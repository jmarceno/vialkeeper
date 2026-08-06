defmodule ElixirDB.HTTP.V1HTTPContractTest do
  use ExUnit.Case, async: false

  alias Plug.Conn

  test "HTTP rejects unknown fields and returns the stable error envelope" do
    response =
      request(:post, "/v1/databases", %{"path" => "contract-invalid.db", "unexpected" => true})

    assert response.status == 400

    assert response.resp_headers
           |> Enum.any?(fn {key, value} -> key == "x-request-id" and value != "" end)

    assert {:ok, %{"error" => %{"code" => "invalid_request", "retryable" => false}}} =
             ElixirDB.JSON.StrictDecoder.decode(response.resp_body)
  end

  test "database, query, integrity, and NDJSON changes routes follow V1 envelopes" do
    path = "contract-#{System.unique_integer([:positive])}.db"
    created = request(:post, "/v1/databases", %{"path" => path})
    assert created.status == 201

    {:ok, %{"data" => %{"database_uuid" => uuid}}} =
      ElixirDB.JSON.StrictDecoder.decode(created.resp_body)

    on_exit(fn ->
      _ = ElixirDB.Runtime.DatabaseCatalog.close(uuid)
      _ = ElixirDB.Runtime.DatabaseCatalog.unregister(uuid)
      _ = File.rm(Path.join(ElixirDB.Config.database_root(), path))
      _ = File.rm(Path.join(ElixirDB.Config.database_root(), path <> ".lease"))
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
      ElixirDB.JSON.StrictDecoder.decode(query.resp_body)

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

  defp request(method, path, body) do
    payload = IO.iodata_to_binary(JSON.encode_to_iodata!(body))

    Plug.Test.conn(method, path, payload)
    |> Conn.put_req_header("content-type", "application/json")
    |> ElixirDB.HTTP.Router.call([])
  end
end
