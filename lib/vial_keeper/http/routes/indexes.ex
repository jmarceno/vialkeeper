defmodule VialKeeper.HTTP.Routes.Indexes do
  @moduledoc "HTTP routes for logical index management."
  use Plug.Router
  alias VialKeeper.HTTP.{Request, Response, Schemas}

  plug(:match)
  plug(:dispatch)

  post "/" do
    Request.call(
      conn,
      Schemas.opts(:index_create, "index create contains an unknown field"),
      fn body, conn ->
        Response.result(conn, VialKeeper.Query.create_index(Request.uuid(conn), body), 201)
      end
    )
  end

  get "/" do
    Response.result(conn, VialKeeper.Query.list_indexes(Request.uuid(conn)))
  end

  delete "/:index_id" do
    with_path_id(conn, fn conn, index_id ->
      Response.result(conn, VialKeeper.Query.delete_index(Request.uuid(conn), index_id))
    end)
  end

  post "/:index_id/rebuild" do
    with_path_id(conn, fn conn, index_id ->
      Response.result(conn, VialKeeper.Query.rebuild_index(Request.uuid(conn), index_id))
    end)
  end

  match _ do
    Response.error(
      conn,
      VialKeeper.Error.invalid_request("route not found", %{path: conn.request_path})
    )
  end

  # SAFETY: bounds-check the :index_id path parameter before it reaches storage. See
  # Request.validate_path_id/1.
  defp with_path_id(conn, fun) do
    case Request.validate_path_id(conn.path_params["index_id"]) do
      :ok -> fun.(conn, conn.path_params["index_id"])
      {:error, error} -> Response.error(conn, error)
    end
  end

  @doc false
  def query(conn) do
    Request.call(conn, Schemas.opts(:query, "query contains an unknown field"), fn body, conn ->
      Response.result(conn, VialKeeper.Query.execute(Request.uuid(conn), body))
    end)
  end

  @doc false
  def explain(conn) do
    Request.call(conn, Schemas.opts(:query, "query explain contains an unknown field"), fn body,
                                                                                           conn ->
      Response.result(conn, VialKeeper.Query.explain(Request.uuid(conn), body))
    end)
  end
end
