defmodule VialKeeper.Replication.ShadowRemoteTransportTest do
  use ExUnit.Case, async: false

  alias VialKeeper.Replication.RemoteTransport
  alias VialKeeper.TestServer

  test "control requests use the bounded compressed JSON contract" do
    token = "shadow-remote-#{System.unique_integer([:positive])}"
    digest = :crypto.hash(:sha256, token) |> Base.encode16(case: :lower)
    previous = Application.get_env(:vial_keeper, :shadow_worker, [])

    Application.put_env(:vial_keeper, :shadow_worker,
      enabled: true,
      control_token_digests: [digest]
    )

    server = TestServer.start_supervised!()

    on_exit(fn -> Application.put_env(:vial_keeper, :shadow_worker, previous) end)

    assert {:ok, %{"data" => %{"capabilities" => capabilities}}} =
             RemoteTransport.request(
               server.base_url,
               :get,
               "/v1/control-plane/capabilities",
               nil,
               token,
               1_000
             )

    assert "zstd_json_v1" in capabilities
  end
end
