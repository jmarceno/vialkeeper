defmodule VialKeeper.ApplicationStartupTest do
  @moduledoc """
  CONFIG-005 failsafe: the listener safety decision enforces that a
  non-loopback interface requires authentication, TLS, or the explicit
  allow_insecure_remote override.
  """
  use ExUnit.Case, async: false

  alias Elixir.Application, as: OTPApplication
  alias VialKeeper.Application

  @loopback [ip: {127, 0, 0, 1}, port: 4000]
  @loopback_v6 [ip: {0, 0, 0, 0, 0, 0, 0, 1}, port: 4000]
  @remote [ip: {10, 0, 0, 5}, port: 4000]

  setup do
    previous_auth = OTPApplication.get_env(:vial_keeper, :auth)
    previous_tls = OTPApplication.get_env(:vial_keeper, :tls)
    previous_security = OTPApplication.get_env(:vial_keeper, :security)

    on_exit(fn ->
      OTPApplication.put_env(:vial_keeper, :auth, previous_auth)
      OTPApplication.put_env(:vial_keeper, :tls, previous_tls)
      OTPApplication.put_env(:vial_keeper, :security, previous_security)
    end)

    :ok
  end

  defp configure(opts) do
    OTPApplication.put_env(
      :vial_keeper,
      :auth,
      Keyword.get(opts, :auth, enabled: false, token_digests: [])
    )

    OTPApplication.put_env(:vial_keeper, :tls, Keyword.get(opts, :tls, enabled: false))

    OTPApplication.put_env(
      :vial_keeper,
      :security,
      Keyword.get(opts, :security, allow_insecure_remote: false)
    )
  end

  test "loopback IPv4 with no auth and no tls is allowed" do
    configure([])
    assert :ok == Application.listener_safety_error(@loopback)
  end

  test "loopback IPv6 with no auth and no tls is allowed" do
    configure([])
    assert :ok == Application.listener_safety_error(@loopback_v6)
  end

  test "non-loopback with no auth and no tls is refused" do
    configure([])
    assert {:error, message} = Application.listener_safety_error(@remote)
    assert message =~ "non-loopback"
    assert message =~ "{10, 0, 0, 5}"
  end

  test "non-loopback with auth enabled is allowed" do
    configure(auth: [enabled: true, token_digests: ["a"]])
    assert :ok == Application.listener_safety_error(@remote)
  end

  test "non-loopback with tls enabled is allowed" do
    configure(tls: [enabled: true, certfile: "cert.pem", keyfile: "key.pem"])
    assert :ok == Application.listener_safety_error(@remote)
  end

  test "non-loopback with allow_insecure_remote override is allowed" do
    configure(security: [allow_insecure_remote: true])
    assert :ok == Application.listener_safety_error(@remote)
  end

  test "non-loopback with auth disabled and tls disabled and override false is refused" do
    configure(
      auth: [enabled: false],
      tls: [enabled: false],
      security: [allow_insecure_remote: false]
    )

    assert {:error, _} = Application.listener_safety_error(@remote)
  end
end
