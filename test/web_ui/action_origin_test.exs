defmodule ElixirDB.WebUI.ActionOriginTest do
  @moduledoc """
  Proof that `/ui/actions/*` requests are rejected unless they carry the
  `hx-request: true` header that htmx supplies, so a plain cross-origin form
  cannot drive-by mutate the console.

  Requests are driven through the parent HTTP router at `/ui/...` so the
  forwarded router-local `conn.path_info` is exercised.
  """

  use ExUnit.Case, async: false

  @moduletag :integration
  import Plug.Test

  alias ElixirDB.HTTP.Router

  setup do
    previous_auth = Application.get_env(:elixir_db, :auth)
    previous_web_ui = Application.get_env(:elixir_db, :web_ui)

    on_exit(fn ->
      Application.put_env(:elixir_db, :auth, previous_auth)
      Application.put_env(:elixir_db, :web_ui, previous_web_ui)
    end)

    Application.put_env(:elixir_db, :web_ui, enabled: true)
    Application.put_env(:elixir_db, :auth, enabled: false, token_digests: [])

    :ok
  end

  defp request(method, path, opts \\ []) do
    body = Keyword.get(opts, :body, "")
    headers = Keyword.get(opts, :headers, [])

    conn =
      Enum.reduce(headers, conn(method, path, body), fn {name, value}, acc ->
        Plug.Conn.put_req_header(acc, name, value)
      end)

    Router.call(conn, Router.init([]))
  end

  test "POST action without the htmx header is rejected like an unknown route" do
    action =
      request("POST", "/ui/actions/databases/integrity-check",
        body: URI.encode_query(%{}),
        headers: [{"content-type", "application/x-www-form-urlencoded"}]
      )

    baseline = request("GET", "/ui/definitely-not-a-route")

    assert action.status == baseline.status
    assert action.status in [400, 404]
    assert action.resp_body =~ "route not found"
    assert action.resp_body =~ "invalid_request"
  end

  test "POST action with the htmx header reaches normal handling" do
    admitted =
      request("POST", "/ui/actions/federation/query",
        body: URI.encode_query(%{"databases" => "[{}]", "query" => ~s({"limit":1})}),
        headers: [
          {"content-type", "application/x-www-form-urlencoded"},
          {"hx-request", "true"}
        ]
      )

    refute admitted.resp_body =~ "route not found"
    assert admitted.resp_body =~ "databases must contain UUID strings"
  end

  test "GET fragment routes are unaffected without the header" do
    fragment = request("GET", "/ui/fragments/federation")
    assert fragment.status == 200
    assert fragment.resp_body =~ "Federation"
  end

  test "POST action with a non-true hx-request value is rejected" do
    action =
      request("POST", "/ui/actions/databases/integrity-check",
        body: URI.encode_query(%{}),
        headers: [
          {"content-type", "application/x-www-form-urlencoded"},
          {"hx-request", "false"}
        ]
      )

    baseline = request("GET", "/ui/definitely-not-a-route")

    assert action.status == baseline.status
    assert action.resp_body =~ "route not found"
    assert action.resp_body =~ "invalid_request"
  end
end
