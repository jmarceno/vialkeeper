defmodule VialKeeper.HTTP.Routes.MaterializedViews do
  @moduledoc "HTTP lifecycle and status routes for materialized federated views."
  use Plug.Router

  alias VialKeeper.HTTP.{Request, Response, Schemas}
  alias VialKeeper.MaterializedViews

  plug(:match)
  plug(:dispatch)

  post "/" do
    Request.call(
      conn,
      Schemas.opts(
        :materialized_view_create,
        "materialized view creation contains an unknown field"
      ),
      fn body, conn ->
        Response.result(conn, MaterializedViews.create(body), 201)
      end
    )
  end

  get "/" do
    Response.result(conn, MaterializedViews.list())
  end

  get "/:uuid" do
    with_uuid(conn, &Response.result(&1, MaterializedViews.get(&2)))
  end

  post "/:uuid/refresh" do
    action(conn, &MaterializedViews.refresh/1)
  end

  post "/:uuid/rebuild" do
    action(conn, &MaterializedViews.rebuild/1)
  end

  post "/:uuid/enable" do
    action(conn, &MaterializedViews.enable/1)
  end

  post "/:uuid/disable" do
    action(conn, &MaterializedViews.disable/1)
  end

  match _ do
    Response.error(
      conn,
      VialKeeper.Error.invalid_request("route not found", %{path: conn.request_path})
    )
  end

  defp action(conn, fun) do
    with_uuid(conn, fn conn, uuid ->
      Request.call(
        conn,
        Schemas.opts(
          :materialized_view_action,
          "materialized view action contains an unknown field"
        ),
        fn _body, conn -> Response.result(conn, fun.(uuid)) end
      )
    end)
  end

  defp with_uuid(conn, fun) do
    case Request.validate_path_id(conn.path_params["uuid"]) do
      :ok -> fun.(conn, conn.path_params["uuid"])
      {:error, error} -> Response.error(conn, error)
    end
  end
end
