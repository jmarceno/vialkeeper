defmodule VialKeeper.HTTP.Routes.Views do
  @moduledoc "HTTP lifecycle and query routes for local declarative views."
  use Plug.Router
  use VialKeeper.HTTP.RouterSpecs

  alias VialKeeper.HTTP.{Request, Response, Schemas}

  plug(:match)
  plug(:dispatch)

  post "/" do
    Request.call(
      conn,
      Schemas.opts(:view_create, "view creation contains an unknown field"),
      fn body, conn ->
        Response.result(conn, VialKeeper.Views.create(Request.uuid(conn), body), 201)
      end
    )
  end

  get "/" do
    Response.result(conn, VialKeeper.Views.list(Request.uuid(conn)))
  end

  delete "/:view_id" do
    with_path_id(conn, fn conn, view_id ->
      Response.result(conn, VialKeeper.Views.delete(Request.uuid(conn), view_id))
    end)
  end

  post "/:view_id/rebuild" do
    with_path_id(conn, fn conn, view_id ->
      Request.call(
        conn,
        Schemas.opts(:view_rebuild, "view rebuild contains an unknown field"),
        fn _body, conn ->
          uuid = Request.uuid(conn)

          case VialKeeper.Views.rebuild(uuid, view_id) do
            :ok ->
              case VialKeeper.Views.state(uuid, view_id) do
                {:ok, state} -> Response.ok(conn, %{"accepted" => true, "state" => state})
                {:error, error} -> Response.error(conn, error)
              end

            {:error, error} ->
              Response.error(conn, error)
          end
        end
      )
    end)
  end

  post "/:view_id/query" do
    with_path_id(conn, fn conn, view_id ->
      Request.call(
        conn,
        Schemas.opts(:view_query, "view query contains an unknown field"),
        fn body, conn ->
          Response.result(conn, VialKeeper.Views.query(Request.uuid(conn), view_id, body))
        end
      )
    end)
  end

  match _ do
    Response.error(
      conn,
      VialKeeper.Error.invalid_request("route not found", %{path: conn.request_path})
    )
  end

  defp with_path_id(conn, fun) do
    case Request.validate_path_id(conn.path_params["view_id"]) do
      :ok -> fun.(conn, conn.path_params["view_id"])
      {:error, error} -> Response.error(conn, error)
    end
  end
end
