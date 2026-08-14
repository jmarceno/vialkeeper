defmodule ElixirDB.HTTP.ReplicationAuthTokenRedactionTest do
  @moduledoc """
  Guards that the machine-API replication GET routes never echo a stored raw
  `auth_token` back in the response body. Redaction happens in JobManager; these
  routes delegate to `JobManager.list/1` and `JobManager.get/2`.
  """

  alias ElixirDB.HTTP.Router
  alias ElixirDB.JSON.StrictDecoder
  alias ElixirDB.Runtime.DatabaseCatalog
  use ExUnit.Case, async: false

  @moduletag :integration

  setup do
    path = "http-redact-%{System.unique_integer([:positive])}.elixirdb"
    conn = call(:post, "/v1/databases", %{"path" => path})
    assert conn.status == 201
    {:ok, %{"data" => %{"database_uuid" => uuid}}} = decode(conn.resp_body)

    on_exit(fn ->
      _ = DatabaseCatalog.close(uuid)
      _ = DatabaseCatalog.unregister(uuid)
      ElixirDB.TempDatabase.cleanup(Path.join(ElixirDB.Config.database_root(), path))
    end)

    {:ok, uuid: uuid}
  end

  test "replication GET routes never leak the raw auth token", %{uuid: uuid} do
    secret = "machine-secret-%{System.unique_integer([:positive])}"

    definition = %{
      "persist" => true,
      "mode" => "one_shot",
      "direction" => "push",
      "enabled" => false,
      "endpoint" => %{
        "kind" => "remote",
        "database_uuid" => "11111111-1111-4111-8111-111111111111",
        "base_url" => "http://127.0.0.1:9",
        "auth_token" => secret
      }
    }

    created = call(:post, "/v1/databases/#{uuid}/replications", definition)
    assert created.status == 201
    refute created.resp_body =~ secret
    {:ok, %{"data" => job}} = decode(created.resp_body)
    job_id = job["job_id"]

    listed = call(:get, "/v1/databases/#{uuid}/replications", nil)
    assert listed.status == 200
    refute listed.resp_body =~ secret

    shown = call(:get, "/v1/databases/#{uuid}/replications/#{URI.encode_www_form(job_id)}", nil)
    assert shown.status == 200
    refute shown.resp_body =~ secret
  end

  defp call(method, path, body) do
    conn = Plug.Test.conn(method, path, if(is_nil(body), do: "", else: encode(body)))
    conn = Plug.Conn.put_req_header(conn, "content-type", "application/json")
    Router.call(conn, [])
  end

  defp encode(term), do: IO.iodata_to_binary(JSON.encode_to_iodata!(term))
  defp decode(body), do: StrictDecoder.decode(body)
end
