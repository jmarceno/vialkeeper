defmodule ElixirDB.WebUI.Router do
  @moduledoc """
  Plug router for the embedded `/ui` administration console.

  Asset and shell routes are fixed allow-lists. When `[web_ui] enabled = false`,
  every path under `/ui` returns the ordinary route-not-found response.
  """

  use Plug.Router

  alias ElixirDB.HTTP.Response, as: JSONResponse
  alias ElixirDB.WebUI
  alias ElixirDB.WebUI.Assets
  alias ElixirDB.WebUI.Routes.{Home, Shell}

  plug(:match)
  plug(:dispatch)

  get "/" do
    serve_when_enabled(conn, fn conn -> Shell.call(conn) end)
  end

  head "/" do
    serve_when_enabled(conn, fn conn ->
      conn
      |> Shell.call()
      |> Map.put(:resp_body, "")
    end)
  end

  get "/assets/:name" do
    serve_when_enabled(conn, fn conn -> serve_asset(conn, name) end)
  end

  head "/assets/:name" do
    serve_when_enabled(conn, fn conn ->
      conn
      |> serve_asset(name)
      |> Map.put(:resp_body, "")
    end)
  end

  get "/fragments/home" do
    serve_when_enabled(conn, fn conn -> Home.call(conn) end)
  end

  head "/fragments/home" do
    serve_when_enabled(conn, fn conn ->
      conn
      |> Home.call()
      |> Map.put(:resp_body, "")
    end)
  end

  match _ do
    not_found(conn)
  end

  defp serve_when_enabled(conn, fun) do
    if WebUI.enabled?() do
      fun.(conn)
    else
      not_found(conn)
    end
  end

  defp serve_asset(conn, name) do
    case Assets.fetch(name) do
      {:ok, asset} ->
        etag = quoted_etag(asset.etag)

        cond do
          if_none_match?(conn, etag) ->
            conn
            |> put_asset_headers(asset, etag)
            |> send_resp(304, "")

          gzip_accepted?(conn) ->
            conn
            |> put_asset_headers(asset, etag)
            |> put_resp_header("content-encoding", "gzip")
            |> put_resp_header("vary", "accept-encoding")
            |> send_resp(200, asset.gzip_body)

          true ->
            conn
            |> put_asset_headers(asset, etag)
            |> put_resp_header("vary", "accept-encoding")
            |> send_resp(200, asset.body)
        end

      :error ->
        not_found(conn)
    end
  end

  defp put_asset_headers(conn, asset, etag) do
    conn
    |> put_resp_header("content-type", asset.content_type)
    |> put_resp_header("x-content-type-options", "nosniff")
    |> put_resp_header("etag", etag)
    |> put_resp_header("cache-control", "public, max-age=31536000, immutable")
  end

  defp quoted_etag(digest) when is_binary(digest), do: "\"" <> digest <> "\""

  defp if_none_match?(conn, etag) do
    conn
    |> get_req_header("if-none-match")
    |> Enum.any?(fn header ->
      header
      |> String.split(",")
      |> Enum.map(&String.trim/1)
      |> Enum.member?(etag)
    end)
  end

  defp gzip_accepted?(conn) do
    conn
    |> get_req_header("accept-encoding")
    |> Enum.any?(fn header ->
      header
      |> String.downcase()
      |> String.contains?("gzip")
    end)
  end

  defp not_found(conn) do
    JSONResponse.error(
      conn,
      ElixirDB.Error.invalid_request("route not found", %{path: conn.request_path})
    )
  end
end
