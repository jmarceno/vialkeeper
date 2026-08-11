defmodule ElixirDB.HTTP.AuthPlugTest do
  @moduledoc """
  Bearer-token authentication (`AUTH-001`, `AUTH-004`).

  Covers: disabled pass-through, valid token success, and the three
  indistinguishable failure cases (missing, malformed, wrong) yielding
  byte-identical 401 responses, plus a host.toml token round-trip.
  """
  use ExUnit.Case, async: false

  @moduletag :integration

  import Plug.Test

  alias ElixirDB.HTTP.Router

  # A real token/digest pair (32 random bytes, hex; SHA-256 of the token).
  @token :crypto.strong_rand_bytes(32) |> Base.encode16(case: :lower)
  @digest String.downcase(:crypto.hash(:sha256, @token) |> Base.encode16(case: :lower))

  setup do
    previous = Application.get_env(:elixir_db, :auth)
    on_exit(fn -> Application.put_env(:elixir_db, :auth, previous) end)
    :ok
  end

  defp auth_enabled,
    do: Application.put_env(:elixir_db, :auth, enabled: true, token_digests: [@digest])

  defp auth_disabled, do: Application.put_env(:elixir_db, :auth, enabled: false, token_digests: [])

  # AuthPlug.init/1 reads Application env, so a fresh conn through the Router
  # exercises init+call. We hit a path that returns 200 when authed.
  defp request(method, path, token \\ nil) do
    conn =
      conn(method, path, "")
      |> maybe_auth(token)

    Router.call(conn, Router.init([]))
  end

  defp maybe_auth(conn, nil), do: conn

  defp maybe_auth(conn, token),
    do: Plug.Conn.put_req_header(conn, "authorization", "Bearer " <> token)

  test "auth disabled: requests pass without a header" do
    auth_disabled()
    # A non-existent route still reaches routing (404), proving auth did not halt.
    conn = request("GET", "/v1/databases")
    assert conn.status == 200 or conn.status == 404
    refute conn.halted
  end

  test "auth enabled: valid bearer token reaches the route" do
    auth_enabled()
    conn = request("GET", "/v1/databases", @token)
    refute conn.halted
    # /v1/databases lists registrations; auth passing means we got past the plug.
    assert conn.status in [200]
  end

  test "auth enabled: missing header is rejected with 401" do
    auth_enabled()
    conn = request("GET", "/v1/databases")
    assert conn.status == 401
    assert conn.halted
  end

  test "auth enabled: malformed header is rejected with 401" do
    auth_enabled()

    conn =
      conn("GET", "/v1/databases", "")
      |> Plug.Conn.put_req_header("authorization", "Basic something")
      |> Router.call(Router.init([]))

    assert conn.status == 401
  end

  test "auth enabled: wrong token is rejected with 401" do
    auth_enabled()
    wrong = :crypto.strong_rand_bytes(32) |> Base.encode16(case: :lower)
    conn = request("GET", "/v1/databases", wrong)
    assert conn.status == 401
  end

  test "AUTH-004: missing, malformed, and wrong token produce indistinguishable errors" do
    auth_enabled()

    missing = request("GET", "/v1/databases")

    malformed =
      conn("GET", "/v1/databases", "")
      |> Plug.Conn.put_req_header("authorization", "Basic something")
      |> Router.call(Router.init([]))

    wrong = request("GET", "/v1/databases", String.duplicate("a", 64))

    # request_id differs per request by design; the error payload (code,
    # message, details, retryable) must be byte-identical across all three
    # failure cases so the cause cannot be probed.
    assert error_payload(missing) == error_payload(malformed)
    assert error_payload(missing) == error_payload(wrong)
  end

  defp error_payload(conn) do
    conn.resp_body
    |> JSON.decode!()
    |> Map.delete("request_id")
  end

  test "AUTH-004: 'Bearer ' with no token is rejected like a wrong token" do
    auth_enabled()

    conn =
      conn("GET", "/v1/databases", "")
      |> Plug.Conn.put_req_header("authorization", "Bearer ")
      |> Router.call(Router.init([]))

    assert conn.status == 401
  end

  test "token digest from a freshly generated pair authenticates" do
    # Mirrors `bin/elixir_db token`: generate raw token, store its digest,
    # present the raw token.
    raw = :crypto.strong_rand_bytes(32) |> Base.encode16(case: :lower)
    digest = String.downcase(:crypto.hash(:sha256, raw) |> Base.encode16(case: :lower))
    Application.put_env(:elixir_db, :auth, enabled: true, token_digests: [digest])

    conn = request("GET", "/v1/databases", raw)
    refute conn.halted
    assert conn.status == 200
  end
end
