defmodule VialKeeper.Replication.RemoteIdentityHardeningTest do
  @moduledoc """
  A remote peer is untrusted: malformed identity fields are rejected and
  HTTP redirects are never followed (so credentials are never replayed).
  """
  use ExUnit.Case, async: false

  @moduletag :integration

  @malformed "22222222-2222-4222-8222-222222222222"
  @redirect "33333333-3333-4333-8333-333333333333"

  defmodule PeerPlug do
    @moduledoc false
    use Plug.Router

    alias Plug.Conn
    alias VialKeeper.Replication.WireCompression

    plug(:match)
    plug(:dispatch)

    get "/v1/databases/22222222-2222-4222-8222-222222222222/replication/identity" do
      send_json(conn, %{"current_sequence" => 0, "retention_floor" => "9"})
    end

    get "/v1/databases/33333333-3333-4333-8333-333333333333/replication/identity" do
      conn
      |> Conn.put_resp_header("location", "/redirected")
      |> Conn.send_resp(302, "")
    end

    post "/v1/databases/22222222-2222-4222-8222-222222222222/replication/revisions/get" do
      send_json(conn, %{"chains" => [], "retention_floor" => 1, "compaction_epoch" => "2"})
    end

    match _ do
      send(Application.get_env(:vial_keeper, :remote_hardening_test_pid), :redirect_followed)
      send_json(conn, %{"current_sequence" => 0})
    end

    defp send_json(conn, payload) do
      {:ok, encoded} = WireCompression.encode_json(payload, 65_536)

      conn
      |> Conn.put_resp_content_type("application/json")
      |> Conn.put_resp_header("content-encoding", "zstd")
      |> Conn.put_resp_header(
        "x-vialkeeper-uncompressed-length",
        Integer.to_string(encoded.uncompressed_length)
      )
      |> Conn.send_resp(200, encoded.body)
    end
  end

  alias VialKeeper.Replication.RemoteEndpoint

  setup do
    {:ok, pid} =
      Bandit.start_link(
        plug: PeerPlug,
        scheme: :http,
        ip: {127, 0, 0, 1},
        port: 0,
        http_options: [compress: false]
      )

    {:ok, {_ip, port}} = ThousandIsland.listener_info(pid)

    on_exit(fn ->
      try do
        GenServer.stop(pid, :normal)
      catch
        :exit, _ -> :ok
      end
    end)

    {:ok, base_url: "http://127.0.0.1:#{port}"}
  end

  defp endpoint(base_url, uuid) do
    {:ok, endpoint} =
      RemoteEndpoint.new(%{"base_url" => base_url, "database_uuid" => uuid, "auth_token" => "t"})

    endpoint
  end

  test "identity with a non-integer retention floor is rejected", %{base_url: base_url} do
    assert {:error, _} = RemoteEndpoint.identity(endpoint(base_url, @malformed))
  end

  test "redirect responses are not followed", %{base_url: base_url} do
    Application.put_env(:vial_keeper, :remote_hardening_test_pid, self())
    assert {:error, _} = RemoteEndpoint.identity(endpoint(base_url, @redirect))
    refute_received :redirect_followed
  end

  test "bootstrap revision page with a non-integer compaction epoch is rejected", %{
    base_url: base_url
  } do
    assert {:error, %VialKeeper.Error{}} =
             RemoteEndpoint.get_revision_chains(endpoint(base_url, @malformed), %{
               bootstrap: true
             })
  end
end
