defmodule VialKeeper.HTTP.ShadowControlTest do
  use ExUnit.Case, async: false

  alias VialKeeper.HTTP.Router
  alias VialKeeper.HTTP.Routes.ShadowControl
  alias VialKeeper.Replication.WireCompression

  test "control plane requires its own bearer token and compresses JSON" do
    token = "shadow-control-#{System.unique_integer([:positive])}"
    digest = :crypto.hash(:sha256, token) |> Base.encode16(case: :lower)
    previous = Application.get_env(:vial_keeper, :shadow_worker, [])

    Application.put_env(:vial_keeper, :shadow_worker,
      enabled: true,
      storage_root: "shadows",
      control_token_digests: [digest],
      allowed_attachment_roots: []
    )

    on_exit(fn -> Application.put_env(:vial_keeper, :shadow_worker, previous) end)

    unauthenticated = Plug.Test.conn(:get, "/v1/control-plane/capabilities", "") |> Router.call([])

    authenticated =
      Plug.Test.conn(:get, "/v1/control-plane/capabilities", "")
      |> Plug.Conn.put_req_header("authorization", "Bearer " <> token)
      |> Plug.Conn.put_req_header("accept-encoding", "zstd")
      |> Router.call([])

    assert unauthenticated.status == 401
    assert authenticated.status == 200
    assert Plug.Conn.get_resp_header(authenticated, "content-encoding") == ["zstd"]

    assert {:ok, %{"data" => %{"protocol_major" => 1}}} =
             WireCompression.decode_json(authenticated.resp_body,
               decoded_limit: 16_777_216,
               headers: authenticated.resp_headers,
               expect: :map
             )
  end

  describe "non-canonical control-plane request targets" do
    setup do
      control_token = "shadow-control-#{System.unique_integer([:positive])}"
      api_token = "api-#{System.unique_integer([:positive])}"
      previous_worker = Application.get_env(:vial_keeper, :shadow_worker, [])
      previous_auth = Application.get_env(:vial_keeper, :auth, [])

      Application.put_env(:vial_keeper, :shadow_worker,
        enabled: true,
        storage_root: "shadows",
        control_token_digests: [sha256(control_token)],
        allowed_attachment_roots: []
      )

      on_exit(fn ->
        Application.put_env(:vial_keeper, :shadow_worker, previous_worker)
        Application.put_env(:vial_keeper, :auth, previous_auth)
      end)

      %{control_token: control_token, api_token: api_token}
    end

    @bypass_paths [
      "/v1//control-plane/capabilities",
      "/v1/%63ontrol-plane/capabilities",
      "/v1/control%2Dplane/capabilities",
      "/v1///control-plane//capabilities"
    ]

    test "are rejected without a token when public auth is disabled" do
      Application.put_env(:vial_keeper, :auth, enabled: false, token_digests: [])

      for path <- @bypass_paths do
        assert (Plug.Test.conn(:get, path, "") |> Router.call([])).status == 401, path
      end
    end

    test "reject API tokens and accept only control tokens", context do
      Application.put_env(:vial_keeper, :auth,
        enabled: true,
        token_digests: [sha256(context.api_token)]
      )

      for path <- @bypass_paths do
        assert get_with_token(path, context.api_token).status == 401, path
        assert get_with_token(path, context.control_token).status == 200, path
      end
    end

    test "malformed escapes are rejected before routing" do
      Application.put_env(:vial_keeper, :auth, enabled: false, token_digests: [])

      conn = Plug.Test.conn(:get, "/v1/%zzcontrol-plane/capabilities", "") |> Router.call([])
      assert conn.status == 400
    end

    test "the sub-router enforces the control token on its own", context do
      Application.put_env(:vial_keeper, :auth, enabled: false, token_digests: [])

      unauthenticated =
        Plug.Test.conn(:get, "/capabilities", "")
        |> ShadowControl.call([])

      authenticated =
        Plug.Test.conn(:get, "/capabilities", "")
        |> Plug.Conn.put_req_header("authorization", "Bearer " <> context.control_token)
        |> ShadowControl.call([])

      assert unauthenticated.status == 401
      assert authenticated.status == 200
    end
  end

  defp get_with_token(path, token) do
    Plug.Test.conn(:get, path, "")
    |> Plug.Conn.put_req_header("authorization", "Bearer " <> token)
    |> Router.call([])
  end

  defp sha256(token), do: :crypto.hash(:sha256, token) |> Base.encode16(case: :lower)
end
