defmodule ElixirDB.HTTP.UnknownFieldsAndRoutesTest do
  use ExUnit.Case, async: false

  alias Plug.Conn

  test "registration rejects unknown fields" do
    response =
      request(:post, "/v1/registrations", %{
        "path" => "http-unknown-reg.db",
        "extra" => true
      })

    assert response.status == 400

    assert {:ok, %{"error" => %{"code" => "invalid_request", "retryable" => false}}} =
             ElixirDB.JSON.StrictDecoder.decode(response.resp_body)
  end

  test "GET /v1/databases and GET /v1/databases/:uuid plus unknown document fields" do
    path = "http-list-#{System.unique_integer([:positive])}.db"
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

    listed = request(:get, "/v1/databases", nil)
    assert listed.status == 200

    {:ok, %{"data" => data}} = ElixirDB.JSON.StrictDecoder.decode(listed.resp_body)

    assert Enum.any?(List.wrap(data), fn entry ->
             entry["database_uuid"] == uuid or entry[:database_uuid] == uuid
           end)

    info = request(:get, "/v1/databases/#{uuid}", nil)
    assert info.status == 200

    bad =
      request(:post, "/v1/databases/#{uuid}/documents/put", %{
        "id" => "x",
        "body" => %{},
        "nope" => 1
      })

    assert bad.status == 400

    assert {:ok, %{"error" => %{"code" => "invalid_request"}}} =
             ElixirDB.JSON.StrictDecoder.decode(bad.resp_body)
  end

  defp request(method, path, nil) do
    Plug.Test.conn(method, path)
    |> ElixirDB.HTTP.Router.call([])
  end

  defp request(method, path, body) when is_map(body) do
    payload = IO.iodata_to_binary(JSON.encode_to_iodata!(body))

    Plug.Test.conn(method, path, payload)
    |> Conn.put_req_header("content-type", "application/json")
    |> ElixirDB.HTTP.Router.call([])
  end
end
