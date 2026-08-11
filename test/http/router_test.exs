defmodule ElixirDB.HTTP.RouterTest do
  alias ElixirDB.HTTP.Router
  alias ElixirDB.JSON.StrictDecoder
  alias ElixirDB.Runtime.DatabaseCatalog
  use ExUnit.Case, async: false

  @moduletag :integration

  test "database and document endpoints return versioned envelopes" do
    path = "http-#{System.unique_integer([:positive])}.elixirdb"
    body = IO.iodata_to_binary(JSON.encode_to_iodata!(%{"path" => path}))

    conn =
      Plug.Test.conn(:post, "/v1/databases", body)
      |> Plug.Conn.put_req_header("content-type", "application/json")
      |> Router.call([])

    assert conn.status == 201
    {:ok, created} = StrictDecoder.decode(conn.resp_body)
    uuid = created["data"]["database_uuid"]

    on_exit(fn ->
      _ = DatabaseCatalog.close(uuid)
      _ = DatabaseCatalog.unregister(uuid)
      ElixirDB.TempDatabase.cleanup(Path.join(ElixirDB.Config.database_root(), path))
    end)

    put = IO.iodata_to_binary(JSON.encode_to_iodata!(%{"id" => "doc", "body" => %{"ok" => true}}))

    conn =
      Plug.Test.conn(:post, "/v1/databases/#{uuid}/documents/put", put)
      |> Plug.Conn.put_req_header("content-type", "application/json")
      |> Router.call([])

    assert conn.status == 201

    get = IO.iodata_to_binary(JSON.encode_to_iodata!(%{"id" => "doc"}))

    conn =
      Plug.Test.conn(:post, "/v1/databases/#{uuid}/documents/get", get)
      |> Plug.Conn.put_req_header("content-type", "application/json")
      |> Router.call([])

    assert conn.status == 200
    {:ok, response} = StrictDecoder.decode(conn.resp_body)
    assert response["data"]["body"]["ok"] == true
  end
end
