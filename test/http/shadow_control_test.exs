defmodule VialKeeper.HTTP.ShadowControlTest do
  use ExUnit.Case, async: false

  alias VialKeeper.HTTP.Router
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
end
