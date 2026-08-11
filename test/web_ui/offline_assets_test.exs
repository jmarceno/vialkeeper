defmodule ElixirDB.WebUI.OfflineAssetsTest do
  @moduledoc """
  Offline embedding and dependency proofs for the administration console.

  Validates that rendered shell/layout markup has no external network asset
  references, that vendored HTMX bytes are frozen, and that the application
  dependency graph does not introduce Phoenix/LiveView or frontend runtimes.
  """
  use ExUnit.Case, async: false

  @moduletag :integration
  import Plug.Test

  alias ElixirDB.HTTP.Router
  alias ElixirDB.WebUI.{Assets, Layout}

  setup do
    previous_web_ui = Application.get_env(:elixir_db, :web_ui)
    previous_auth = Application.get_env(:elixir_db, :auth)

    on_exit(fn ->
      Application.put_env(:elixir_db, :web_ui, previous_web_ui)
      Application.put_env(:elixir_db, :auth, previous_auth)
    end)

    Application.put_env(:elixir_db, :web_ui, enabled: true)
    Application.put_env(:elixir_db, :auth, enabled: false, token_digests: [])
    :ok
  end

  test "shell and layout asset references stay same-origin" do
    shell = Layout.shell() |> IO.iodata_to_binary()
    refute_external_asset_refs(shell)

    conn =
      conn(:get, "/ui")
      |> Router.call(Router.init([]))

    assert conn.status == 200
    refute_external_asset_refs(conn.resp_body)

    for name <- Assets.names() do
      assert shell =~ "/ui/assets/#{name}"
    end
  end

  test "application dependency list has no Phoenix or LiveView runtime" do
    deps =
      Mix.Project.config()
      |> Keyword.get(:deps, [])
      |> Enum.map(fn
        {name, _} -> to_string(name)
        {name, _, _} -> to_string(name)
      end)

    refute "phoenix" in deps
    refute "phoenix_live_view" in deps
    refute "phoenix_html" in deps
    refute "esbuild" in deps
    refute "tailwind" in deps
  end

  test "embedded assets remain available without reading the source tree at runtime" do
    root =
      Path.join(System.tmp_dir!(), "elixirdb-offline-assets-#{System.unique_integer([:positive])}")

    File.mkdir_p!(root)
    on_exit(fn -> File.rm_rf(root) end)

    source = Path.expand("assets/web_ui")
    staging = Path.join(root, "assets_web_ui_copy")
    File.cp_r!(source, staging)

    for name <- Assets.names() do
      {:ok, asset} = Assets.fetch(name)
      assert is_binary(asset.body)
      assert asset.body != ""
    end

    # Removing a copy does not affect the already-compiled module literals.
    File.rm_rf!(staging)

    for name <- Assets.names() do
      assert {:ok, asset} = Assets.fetch(name)
      assert byte_size(asset.body) > 0
    end
  end

  test "HTMX constants match the committed vendor bytes" do
    {:ok, asset} = Assets.fetch("htmx.min.js")
    assert Assets.htmx_version() == "2.0.7"
    assert byte_size(asset.body) == Assets.htmx_expected_size()
    assert asset.etag == Assets.htmx_expected_sha256()
  end

  defp refute_external_asset_refs(html) when is_binary(html) do
    # Attribute-oriented checks: ignore escaped plain-text URL bodies in <pre>.
    for attr <- ["src=", "href=", "action="] do
      Regex.scan(~r/#{attr}"([^"]*)"/, html)
      |> Enum.each(fn [_, value] ->
        refute String.starts_with?(value, "http://"), "external #{attr}#{value}"
        refute String.starts_with?(value, "https://"), "external #{attr}#{value}"
        refute String.starts_with?(value, "//"), "protocol-relative #{attr}#{value}"
      end)
    end

    refute html =~ ~r/@import\s+url\(/i
    refute html =~ "cdn.jsdelivr"
    refute html =~ "unpkg.com"
    refute html =~ "fonts.googleapis"
    refute html =~ "fonts.gstatic"
    refute html =~ "googletagmanager"
  end
end
