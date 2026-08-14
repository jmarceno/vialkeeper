defmodule ElixirDB.WebUI.Router do
  @moduledoc """
  Plug router for the embedded `/ui` administration console.

  Asset and shell routes are fixed allow-lists. When `[web_ui] enabled = false`,
  every path under `/ui` returns the ordinary route-not-found response.
  Fragment and action routes remain behind the same bearer authentication as `/v1`.
  """

  use Plug.Router

  alias ElixirDB.HTTP.Response, as: JSONResponse
  alias ElixirDB.WebUI
  alias ElixirDB.WebUI.Assets

  alias ElixirDB.WebUI.Routes.{
    Databases,
    Documents,
    Federation,
    Home,
    Maintenance,
    MaterializedViews,
    Observability,
    Queries,
    Replications,
    Shell,
    Views
  }

  plug(:reject_foreign_actions)
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

  get "/fragments/databases" do
    serve_when_enabled(conn, fn conn -> Databases.list(conn) end)
  end

  get "/fragments/databases/:uuid/documents/new" do
    serve_when_enabled(conn, fn conn -> Documents.new(conn) end)
  end

  get "/fragments/databases/:uuid/documents/show" do
    serve_when_enabled(conn, fn conn -> Documents.show(conn) end)
  end

  get "/fragments/databases/:uuid/documents" do
    serve_when_enabled(conn, fn conn -> Documents.browse(conn) end)
  end

  get "/fragments/databases/:uuid/queries" do
    serve_when_enabled(conn, fn conn -> Queries.show(conn) end)
  end

  get "/fragments/databases/:uuid/views/:view_id/status" do
    serve_when_enabled(conn, fn conn -> Views.status(conn) end)
  end

  get "/fragments/databases/:uuid/views" do
    serve_when_enabled(conn, fn conn -> Views.list(conn) end)
  end

  get "/fragments/databases/:uuid/replications/:job_id/status" do
    serve_when_enabled(conn, fn conn -> Replications.status(conn) end)
  end

  get "/fragments/databases/:uuid/replications" do
    serve_when_enabled(conn, fn conn -> Replications.list(conn) end)
  end

  get "/fragments/databases/:uuid/maintenance" do
    serve_when_enabled(conn, fn conn -> Maintenance.show(conn) end)
  end

  get "/fragments/databases/:uuid" do
    serve_when_enabled(conn, fn conn -> Databases.show(conn) end)
  end

  get "/fragments/federation" do
    serve_when_enabled(conn, fn conn -> Federation.show(conn) end)
  end

  get "/fragments/materialized-views/:uuid/status" do
    serve_when_enabled(conn, fn conn -> MaterializedViews.status(conn) end)
  end

  get "/fragments/materialized-views/:uuid" do
    serve_when_enabled(conn, fn conn -> MaterializedViews.show(conn) end)
  end

  get "/fragments/materialized-views" do
    serve_when_enabled(conn, fn conn -> MaterializedViews.list(conn) end)
  end

  get "/fragments/observability" do
    serve_when_enabled(conn, fn conn -> Observability.show(conn) end)
  end

  post "/actions/databases" do
    serve_when_enabled(conn, fn conn -> Databases.create(conn) end)
  end

  post "/actions/databases/register" do
    serve_when_enabled(conn, fn conn -> Databases.register(conn) end)
  end

  post "/actions/databases/:uuid/config" do
    serve_when_enabled(conn, fn conn -> Databases.update_config(conn) end)
  end

  post "/actions/databases/:uuid/close" do
    serve_when_enabled(conn, fn conn -> Databases.close(conn) end)
  end

  post "/actions/databases/:uuid/unregister" do
    serve_when_enabled(conn, fn conn -> Databases.unregister(conn) end)
  end

  post "/actions/databases/:uuid/documents/put" do
    serve_when_enabled(conn, fn conn -> Documents.put(conn) end)
  end

  post "/actions/databases/:uuid/documents/delete" do
    serve_when_enabled(conn, fn conn -> Documents.delete(conn) end)
  end

  post "/actions/databases/:uuid/queries/execute" do
    serve_when_enabled(conn, fn conn -> Queries.execute(conn) end)
  end

  post "/actions/databases/:uuid/queries/explain" do
    serve_when_enabled(conn, fn conn -> Queries.explain(conn) end)
  end

  post "/actions/databases/:uuid/indexes" do
    serve_when_enabled(conn, fn conn -> Queries.create_index(conn) end)
  end

  post "/actions/databases/:uuid/indexes/:index_id/delete" do
    serve_when_enabled(conn, fn conn -> Queries.delete_index(conn) end)
  end

  post "/actions/databases/:uuid/indexes/:index_id/rebuild" do
    serve_when_enabled(conn, fn conn -> Queries.rebuild_index(conn) end)
  end

  post "/actions/databases/:uuid/views" do
    serve_when_enabled(conn, fn conn -> Views.create(conn) end)
  end

  post "/actions/databases/:uuid/views/:view_id/query" do
    serve_when_enabled(conn, fn conn -> Views.query(conn) end)
  end

  post "/actions/databases/:uuid/views/:view_id/rebuild" do
    serve_when_enabled(conn, fn conn -> Views.rebuild(conn) end)
  end

  post "/actions/databases/:uuid/views/:view_id/delete" do
    serve_when_enabled(conn, fn conn -> Views.delete(conn) end)
  end

  post "/actions/databases/:uuid/replications" do
    serve_when_enabled(conn, fn conn -> Replications.create(conn) end)
  end

  post "/actions/databases/:uuid/replications/:job_id/start" do
    serve_when_enabled(conn, fn conn -> Replications.start(conn) end)
  end

  post "/actions/databases/:uuid/replications/:job_id/cancel" do
    serve_when_enabled(conn, fn conn -> Replications.cancel(conn) end)
  end

  post "/actions/databases/:uuid/replications/:job_id/enable" do
    serve_when_enabled(conn, fn conn -> Replications.enable(conn) end)
  end

  post "/actions/databases/:uuid/replications/:job_id/disable" do
    serve_when_enabled(conn, fn conn -> Replications.disable(conn) end)
  end

  post "/actions/databases/:uuid/replications/:job_id/delete" do
    serve_when_enabled(conn, fn conn -> Replications.delete(conn) end)
  end

  post "/actions/databases/:uuid/integrity-check" do
    serve_when_enabled(conn, fn conn -> Maintenance.integrity_check(conn) end)
  end

  post "/actions/databases/:uuid/compact" do
    serve_when_enabled(conn, fn conn -> Maintenance.compact(conn) end)
  end

  post "/actions/databases/:uuid/attachments/gc" do
    serve_when_enabled(conn, fn conn -> Maintenance.attachment_gc(conn) end)
  end

  post "/actions/federation/query" do
    serve_when_enabled(conn, fn conn -> Federation.query(conn) end)
  end

  post "/actions/federation/saved-queries/execute" do
    serve_when_enabled(conn, fn conn -> Federation.execute_saved(conn) end)
  end

  post "/actions/materialized-views" do
    serve_when_enabled(conn, fn conn -> MaterializedViews.create(conn) end)
  end

  post "/actions/materialized-views/:uuid/enable" do
    serve_when_enabled(conn, fn conn -> MaterializedViews.enable(conn) end)
  end

  post "/actions/materialized-views/:uuid/disable" do
    serve_when_enabled(conn, fn conn -> MaterializedViews.disable(conn) end)
  end

  post "/actions/materialized-views/:uuid/refresh" do
    serve_when_enabled(conn, fn conn -> MaterializedViews.refresh(conn) end)
  end

  post "/actions/materialized-views/:uuid/rebuild" do
    serve_when_enabled(conn, fn conn -> MaterializedViews.rebuild(conn) end)
  end

  match _ do
    not_found(conn)
  end

  @state_changing_methods ["POST", "PUT", "PATCH", "DELETE"]

  defp reject_foreign_actions(conn, _opts) do
    if foreign_action?(conn) do
      conn
      |> not_found()
      |> Plug.Conn.halt()
    else
      conn
    end
  end

  defp foreign_action?(conn) do
    conn.method in @state_changing_methods and match?(["actions" | _], conn.path_info) and
      not htmx_request?(conn)
  end

  defp htmx_request?(conn) do
    conn
    |> Plug.Conn.get_req_header("hx-request")
    |> Enum.any?(&(&1 == "true"))
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
