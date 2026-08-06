defmodule ElixirDB.HTTP.Routes.Indexes do
  @moduledoc false
  use Plug.Router
  alias ElixirDB.HTTP.{Request, Response, Schemas}

  plug(:match)
  plug(:dispatch)

  post "/" do
    Request.call(
      conn,
      Schemas.opts(:index_create, "index create contains an unknown field"),
      fn body, conn ->
        Response.result(conn, ElixirDB.Query.create_index(Request.uuid(conn), body), 201)
      end
    )
  end

  get "/" do
    Response.result(conn, ElixirDB.Query.list_indexes(Request.uuid(conn)))
  end

  delete "/:index_id" do
    Response.result(
      conn,
      ElixirDB.Query.delete_index(Request.uuid(conn), conn.path_params["index_id"])
    )
  end

  post "/:index_id/rebuild" do
    Response.result(
      conn,
      ElixirDB.Query.rebuild_index(Request.uuid(conn), conn.path_params["index_id"])
    )
  end

  match _ do
    Response.error(
      conn,
      ElixirDB.Error.invalid_request("route not found", %{path: conn.request_path})
    )
  end

  @doc false
  def query(conn) do
    Request.call(conn, Schemas.opts(:query, "query contains an unknown field"), fn body, conn ->
      Response.result(conn, ElixirDB.Query.execute(Request.uuid(conn), body))
    end)
  end

  @doc false
  def explain(conn) do
    Request.call(conn, Schemas.opts(:query, "query explain contains an unknown field"), fn body,
                                                                                           conn ->
      Response.result(conn, ElixirDB.Query.explain(Request.uuid(conn), body))
    end)
  end
end
