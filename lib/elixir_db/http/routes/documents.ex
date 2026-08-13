defmodule ElixirDB.HTTP.Routes.Documents do
  @moduledoc "HTTP routes for document reads, writes, conflicts, and imports."
  use Plug.Router
  alias ElixirDB.HTTP.{Request, Response, Schemas}

  plug(:match)
  plug(:dispatch)

  post "/get" do
    Request.call(
      conn,
      Schemas.opts(:document_get, "document get contains an unknown field"),
      fn body, conn ->
        case Request.read_consistency(conn) do
          {:ok, consistency} ->
            Response.result_with_read_meta(
              conn,
              ElixirDB.Documents.get_with_meta(Request.uuid(conn), body,
                read_consistency: consistency
              )
            )

          {:error, error} ->
            Response.error(conn, error)
        end
      end
    )
  end

  post "/put" do
    Request.call(
      conn,
      Schemas.opts(:document_put, "document put contains an unknown field"),
      fn body, conn ->
        Response.result(conn, ElixirDB.Documents.put(Request.uuid(conn), body), 201)
      end
    )
  end

  post "/delete" do
    Request.call(
      conn,
      Schemas.opts(:document_delete, "document delete contains an unknown field"),
      fn body, conn ->
        Response.result(conn, ElixirDB.Documents.delete(Request.uuid(conn), body))
      end
    )
  end

  post "/resolve" do
    Request.call(
      conn,
      Schemas.opts(:document_resolve, "document resolve contains an unknown field"),
      fn body, conn ->
        Response.result(conn, ElixirDB.Documents.resolve(Request.uuid(conn), body))
      end
    )
  end

  post "/bulk-get" do
    # Bulk get is an array of get requests — BodyReader allows arrays; per-item validation
    # remains in Documents.bulk_get/2.
    Request.call(conn, fn body, conn ->
      case Request.read_consistency(conn) do
        {:ok, consistency} ->
          Response.result_with_read_meta(
            conn,
            ElixirDB.Documents.bulk_get_with_meta(
              Request.uuid(conn),
              body,
              read_consistency: consistency
            )
          )

        {:error, error} ->
          Response.error(conn, error)
      end
    end)
  end

  post "/bulk-write" do
    Request.call(conn, fn body, conn ->
      Response.result(conn, ElixirDB.Documents.bulk_write(Request.uuid(conn), body))
    end)
  end

  match _ do
    Response.error(
      conn,
      ElixirDB.Error.invalid_request("route not found", %{path: conn.request_path})
    )
  end
end
