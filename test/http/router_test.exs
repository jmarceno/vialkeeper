defmodule ElixirDB.HTTP.RouterTest do
  use ExUnit.Case, async: false

  test "database and document endpoints return versioned envelopes" do
    path = "http-#{System.unique_integer([:positive])}.db"
    body = IO.iodata_to_binary(JSON.encode_to_iodata!(%{"path" => path}))

    conn =
      Plug.Test.conn(:post, "/v1/databases", body)
      |> Plug.Conn.put_req_header("content-type", "application/json")
      |> ElixirDB.HTTP.Router.call([])

    assert conn.status == 201
    {:ok, created} = ElixirDB.JSON.StrictDecoder.decode(conn.resp_body)
    uuid = created["data"]["database_uuid"]

    put = IO.iodata_to_binary(JSON.encode_to_iodata!(%{"id" => "doc", "body" => %{"ok" => true}}))

    conn =
      Plug.Test.conn(:post, "/v1/databases/#{uuid}/documents/put", put)
      |> Plug.Conn.put_req_header("content-type", "application/json")
      |> ElixirDB.HTTP.Router.call([])

    assert conn.status == 201

    get = IO.iodata_to_binary(JSON.encode_to_iodata!(%{"id" => "doc"}))

    conn =
      Plug.Test.conn(:post, "/v1/databases/#{uuid}/documents/get", get)
      |> Plug.Conn.put_req_header("content-type", "application/json")
      |> ElixirDB.HTTP.Router.call([])

    assert conn.status == 200
    {:ok, response} = ElixirDB.JSON.StrictDecoder.decode(conn.resp_body)
    assert response["data"]["body"]["ok"] == true
  end
end
