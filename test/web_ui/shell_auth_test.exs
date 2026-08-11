defmodule ElixirDB.WebUI.ShellAuthTest do
  @moduledoc """
  Host config, embedded assets, anonymous shell, and auth-boundary proofs for
  the embedded administration console.
  """
  use ExUnit.Case, async: false
  import Plug.Test

  alias ElixirDB.HostConfig
  alias ElixirDB.HTTP.{AuthPlug, Router}
  alias ElixirDB.WebUI
  alias ElixirDB.WebUI.{Assets, HTML, Layout}

  @token :crypto.strong_rand_bytes(32) |> Base.encode16(case: :lower)
  @digest String.downcase(:crypto.hash(:sha256, @token) |> Base.encode16(case: :lower))

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
    token = Keyword.get(opts, :token)

    conn =
      conn(method, path, body)
      |> put_req_headers(headers)
      |> maybe_auth(token)

    Router.call(conn, Router.init([]))
  end

  defp put_req_headers(conn, headers) do
    Enum.reduce(headers, conn, fn {name, value}, acc ->
      Plug.Conn.put_req_header(acc, name, value)
    end)
  end

  defp maybe_auth(conn, nil), do: conn

  defp maybe_auth(conn, token),
    do: Plug.Conn.put_req_header(conn, "authorization", "Bearer " <> token)

  defp enable_auth,
    do: Application.put_env(:elixir_db, :auth, enabled: true, token_digests: [@digest])

  defp disable_ui, do: Application.put_env(:elixir_db, :web_ui, enabled: false)

  test "defaults and shipped template enable the web UI without drift" do
    assert HostConfig.defaults()["web_ui"] == %{"enabled" => true}

    template =
      :code.priv_dir(:elixir_db)
      |> Path.join("host.toml")
      |> File.read!()

    {:ok, parsed} = Toml.decode(template)
    assert parsed == HostConfig.defaults()
    assert parsed["web_ui"]["enabled"] == true
  end

  test "HostConfig loads web_ui.enabled overrides" do
    dir = Path.join(System.tmp_dir!(), "elixirdb-webui-cfg-#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)
    on_exit(fn -> File.rm_rf(dir) end)

    File.write!(Path.join(dir, "host.toml"), "[web_ui]\nenabled = false\n")
    assert {:ok, config} = HostConfig.load_from(dir)
    assert Keyword.get(config, :web_ui) == [enabled: false]
  end

  test "disabled UI exposes no /ui routes while /v1 remains available" do
    disable_ui()
    ui = request("GET", "/ui")
    assert ui.status == 400
    assert error_payload(ui)["error"]["code"] == "invalid_request"

    asset = request("GET", "/ui/assets/htmx.min.js")
    assert asset.status == 400

    list = request("GET", "/v1/databases")
    assert list.status == 200
  end

  test "auth disabled: shell, assets, and home fragment succeed without credentials" do
    shell = request("GET", "/ui")
    assert shell.status == 200
    assert content_type(shell) =~ "text/html"
    assert shell.resp_body =~ "ElixirDB"
    assert shell.resp_body =~ ~s(name="htmx-config")

    assert shell.resp_body =~
             ~s(content='{"allowEval":false,"allowScriptTags":false,"historyCacheSize":0,"includeIndicatorStyles":false,"selfRequestsOnly":true}')

    assert shell.resp_body =~ ~s(hx-disinherit="hx-target hx-swap")
    assert shell.resp_body =~ ~s(id="app")

    refute shell.resp_body =~ "database_uuid"
    refute shell.resp_body =~ "token_digests"
    refute shell.resp_body =~ "127.0.0.1"

    asset = request("GET", "/ui/assets/htmx.min.js")
    assert asset.status == 200
    assert content_type(asset) =~ "javascript"
    assert byte_size(asset.resp_body) == Assets.htmx_expected_size()

    home = request("GET", "/ui/fragments/home")
    assert home.status == 200
    assert home.resp_body =~ "Console"
  end

  test "auth enabled: shell and assets are anonymous; fragments require bearer" do
    enable_auth()

    shell = request("GET", "/ui")
    assert shell.status == 200
    assert shell.resp_body =~ "auth-form"

    asset = request("GET", "/ui/assets/app.css")
    assert asset.status == 200

    missing = request("GET", "/ui/fragments/home")
    assert missing.status == 401

    malformed =
      conn("GET", "/ui/fragments/home", "")
      |> Plug.Conn.put_req_header("authorization", "Basic nope")
      |> Router.call(Router.init([]))

    wrong = request("GET", "/ui/fragments/home", token: String.duplicate("ab", 32))
    ok = request("GET", "/ui/fragments/home", token: @token)

    assert malformed.status == 401
    assert wrong.status == 401
    assert ok.status == 200
    assert error_payload(missing) == error_payload(malformed)
    assert error_payload(missing) == error_payload(wrong)
  end

  test "public_web_ui_request? rejects fragments, actions, and /v1" do
    assert AuthPlug.public_web_ui_request?(conn("GET", "/ui"))
    assert AuthPlug.public_web_ui_request?(conn("GET", "/ui/assets/htmx.min.js"))
    refute AuthPlug.public_web_ui_request?(conn("GET", "/ui/fragments/home"))
    refute AuthPlug.public_web_ui_request?(conn("POST", "/ui/actions/databases"))
    refute AuthPlug.public_web_ui_request?(conn("GET", "/v1/databases"))
    refute AuthPlug.public_web_ui_request?(conn("GET", "/ui/assets/../htmx.min.js"))
    refute AuthPlug.public_web_ui_request?(conn("GET", "/ui/assets/missing.js"))
  end

  test "asset allow-list rejects unknown and traversal names" do
    assert :error = Assets.fetch("../htmx.min.js")
    assert :error = Assets.fetch("missing.js")
    assert :error = Assets.fetch("vendor/htmx.min.js")

    unknown = request("GET", "/ui/assets/missing.js")
    assert unknown.status == 400
    assert error_payload(unknown)["error"]["code"] == "invalid_request"
  end

  test "assets serve etag, 304, gzip, cache, and content-type headers" do
    {:ok, asset} = Assets.fetch("htmx.min.js")
    etag = "\"" <> asset.etag <> "\""

    first = request("GET", "/ui/assets/htmx.min.js")
    assert first.status == 200
    assert get_header(first, "etag") == etag
    assert get_header(first, "cache-control") == "public, max-age=31536000, immutable"
    assert get_header(first, "x-content-type-options") == "nosniff"
    assert content_type(first) == "text/javascript; charset=utf-8"

    cached =
      request("GET", "/ui/assets/htmx.min.js", headers: [{"if-none-match", etag}])

    assert cached.status == 304
    assert cached.resp_body == ""

    gzipped =
      request("GET", "/ui/assets/htmx.min.js", headers: [{"accept-encoding", "gzip"}])

    assert gzipped.status == 200
    assert get_header(gzipped, "content-encoding") == "gzip"
    assert get_header(gzipped, "vary") == "accept-encoding"
    assert gzipped.resp_body == asset.gzip_body
  end

  test "HTML responses include CSP without unsafe-inline or unsafe-eval" do
    shell = request("GET", "/ui")
    csp = get_header(shell, "content-security-policy")

    assert csp =~ "default-src 'self'"
    assert csp =~ "script-src 'self'"
    assert csp =~ "style-src 'self'"
    refute csp =~ "unsafe-inline"
    refute csp =~ "unsafe-eval"
    assert get_header(shell, "referrer-policy") == "no-referrer"
    assert get_header(shell, "x-frame-options") == "DENY"
    assert get_header(shell, "cache-control") == "no-store"

    home = request("GET", "/ui/fragments/home")
    assert get_header(home, "cache-control") == "no-store"
    assert get_header(home, "content-security-policy") == csp
  end

  test "hostile values escape in HTML helpers and shell fixtures" do
    hostile = "<img src=x onerror=\"alert(1)\">&\""
    assert HTML.escape(hostile) == "&lt;img src=x onerror=&quot;alert(1)&quot;&gt;&amp;&quot;"
    assert HTML.attr(hostile) == HTML.escape(hostile)

    shell = Layout.shell() |> IO.iodata_to_binary()
    assert shell =~ "ElixirDB"
    refute shell =~ "<script>alert"
  end

  test "HTMX vendor version and digest constants match committed bytes" do
    assert Assets.htmx_version() == "2.0.7"
    assert Assets.htmx_expected_size() == 51_076

    assert Assets.htmx_expected_sha256() ==
             "60231ae6ba9db3825eb15a261122d5f55921c4d53b66bf637dc18b4ee27c79f9"

    {:ok, asset} = Assets.fetch("htmx.min.js")
    assert byte_size(asset.body) == Assets.htmx_expected_size()
    assert asset.etag == Assets.htmx_expected_sha256()
    assert Assets.htmx_license() =~ "Zero-Clause BSD"
  end

  test "embedded asset bytes remain available after source assets are removed" do
    root =
      Path.join(System.tmp_dir!(), "elixirdb-webui-embed-#{System.unique_integer([:positive])}")

    File.mkdir_p!(root)
    on_exit(fn -> File.rm_rf(root) end)

    source = Path.expand("assets/web_ui")
    staging = Path.join(root, "assets/web_ui")
    File.mkdir_p!(Path.dirname(staging))
    File.cp_r!(source, staging)
    File.rm_rf!(staging)

    {:ok, asset} = Assets.fetch("app.css")
    assert is_binary(asset.body)
    assert asset.body != ""
    refute File.exists?(Path.expand("assets/web_ui/app.css") |> then(fn _ -> staging end))
    assert WebUI.enabled?()
  end

  defp content_type(conn), do: get_header(conn, "content-type")

  defp get_header(conn, name) do
    conn
    |> Plug.Conn.get_resp_header(name)
    |> List.first()
  end

  defp error_payload(conn) do
    conn.resp_body
    |> JSON.decode!()
    |> Map.delete("request_id")
  end
end
