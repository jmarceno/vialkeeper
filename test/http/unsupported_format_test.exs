defmodule VialKeeper.HTTP.UnsupportedFormatTest do
  @moduledoc "Covers HTTP 409 unsupported_format when a closed bundle is not V1."

  use ExUnit.Case, async: false

  @moduletag :integration

  alias Plug.Conn
  alias VialKeeper.HTTP.Router
  alias VialKeeper.JSON.StrictDecoder
  alias VialKeeper.Runtime.DatabaseCatalog

  test "document access returns unsupported_format after a future user_version bump" do
    path = "format-http-#{System.unique_integer([:positive])}.vialkeeper"
    created = request(:post, "/v1/databases", %{"path" => path})
    assert created.status == 201

    {:ok, %{"data" => %{"database_uuid" => uuid}}} = StrictDecoder.decode(created.resp_body)

    on_exit(fn ->
      _ = DatabaseCatalog.close(uuid)
      _ = DatabaseCatalog.unregister(uuid)
      VialKeeper.TempDatabase.cleanup(Path.join(VialKeeper.Config.database_root(), path))
    end)

    close = request(:post, "/v1/databases/#{uuid}/close", %{})
    assert close.status == 200

    sqlite =
      VialKeeper.TempDatabase.sqlite_path(Path.join(VialKeeper.Config.database_root(), path))

    {:ok, conn} = Exqlite.Sqlite3.open(sqlite)

    try do
      assert :ok = Exqlite.Sqlite3.execute(conn, "PRAGMA user_version = 2")
    after
      assert :ok = Exqlite.Sqlite3.close(conn)
    end

    get = request(:post, "/v1/databases/#{uuid}/documents/get", %{"id" => "doc"})
    assert get.status == 409

    assert {:ok, %{"error" => %{"code" => "unsupported_format", "retryable" => false}}} =
             StrictDecoder.decode(get.resp_body)
  end

  defp request(method, path, body) do
    payload = IO.iodata_to_binary(JSON.encode_to_iodata!(body))

    Plug.Test.conn(method, path, payload)
    |> Conn.put_req_header("content-type", "application/json")
    |> Router.call([])
  end
end
