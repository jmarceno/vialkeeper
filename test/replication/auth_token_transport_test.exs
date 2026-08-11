defmodule ElixirDB.Replication.AuthTokenTransportTest do
  @moduledoc """
  AUTH-003: a remote endpoint's `auth_token` is presented as an
  `Authorization: Bearer` header on outbound replication wire calls.

  Uses a tiny capture Plug + Bandit so the actual header sent over the wire is
  observed, rather than mocking the transport.
  """
  use ExUnit.Case, async: true

  @moduletag :integration

  defmodule CapturePlug do
    @moduledoc false
    use Plug.Router
    plug(:match)
    plug(:dispatch)

    get "/v1/databases/:_uuid/replication/identity" do
      send_capture(conn)
    end

    match _ do
      send_capture(conn)
    end

    defp send_capture(conn) do
      auth = Plug.Conn.get_req_header(conn, "authorization") |> List.first()

      body =
        JSON.encode!(%{
          "captured_authorization" => auth,
          "database_uuid" => "11111111-1111-4111-8111-111111111111",
          "current_sequence" => 0,
          "replication_protocol_major" => 1,
          "revision_algorithm_version" => 1,
          "canonicalization_version" => 1
        })

      conn
      |> Plug.Conn.put_resp_content_type("application/json")
      |> Plug.Conn.send_resp(200, body)
    end
  end

  alias ElixirDB.Replication.RemoteEndpoint

  @uuid "11111111-1111-4111-8111-111111111111"

  setup do
    {:ok, pid} =
      Bandit.start_link(
        plug: CapturePlug,
        scheme: :http,
        ip: {127, 0, 0, 1},
        port: 0
      )

    port = port_of(pid)

    on_exit(fn ->
      try do
        GenServer.stop(pid, :normal)
      catch
        # Bandit's supervised shutdown propagates an :EXIT; tolerate it.
        :exit, _ -> :ok
      end
    end)

    {:ok, pid: pid, port: port, base_url: "http://127.0.0.1:#{port}"}
  end

  defp port_of(pid) do
    {:ok, {_bound_ip, port}} = ThousandIsland.listener_info(pid)
    port
  end

  test "a remote endpoint with auth_token sends the bearer credential", %{base_url: base_url} do
    {:ok, endpoint} =
      RemoteEndpoint.new(%{
        "kind" => "remote",
        "base_url" => base_url,
        "database_uuid" => @uuid,
        "auth_token" => "secret-token-123"
      })

    assert {:ok, %{"captured_authorization" => auth}} = RemoteEndpoint.identity(endpoint)
    assert auth == "Bearer secret-token-123"
  end

  test "a remote endpoint without auth_token sends no authorization header", %{base_url: base_url} do
    {:ok, endpoint} =
      RemoteEndpoint.new(%{
        "kind" => "remote",
        "base_url" => base_url,
        "database_uuid" => @uuid
      })

    assert {:ok, %{"captured_authorization" => nil}} = RemoteEndpoint.identity(endpoint)
  end
end
