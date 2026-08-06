defmodule ElixirDB.HTTP.Routes.Documents do
  @moduledoc false
  use Plug.Router
  alias ElixirDB.HTTP.{Request, Response}

  plug(:match)
  plug(:dispatch)

  post "/get" do
    Request.call(
      conn,
      [
        allowed_fields: ["id", "revision", "include_conflicts"],
        unknown_message: "document get contains an unknown field"
      ],
      fn body, conn ->
        Response.result(conn, ElixirDB.Documents.get(Request.uuid(conn), body))
      end
    )
  end

  post "/put" do
    Request.call(
      conn,
      [
        allowed_fields: ["id", "if_revision", "body"],
        unknown_message: "document put contains an unknown field"
      ],
      fn body, conn ->
        Response.result(conn, ElixirDB.Documents.put(Request.uuid(conn), body), 201)
      end
    )
  end

  post "/delete" do
    Request.call(
      conn,
      [
        allowed_fields: ["id", "if_revision"],
        unknown_message: "document delete contains an unknown field"
      ],
      fn body, conn ->
        Response.result(conn, ElixirDB.Documents.delete(Request.uuid(conn), body))
      end
    )
  end

  post "/resolve" do
    Request.call(
      conn,
      [
        allowed_fields: [
          "id",
          "document_id",
          "expected_live_revisions",
          "chosen_parent_revision",
          "body",
          "delete_all"
        ],
        unknown_message: "document resolve contains an unknown field"
      ],
      fn body, conn ->
        Response.result(conn, ElixirDB.Documents.resolve(Request.uuid(conn), body))
      end
    )
  end

  post "/bulk-get" do
    Request.call(conn, fn body, conn ->
      Response.result(conn, ElixirDB.Documents.bulk_get(Request.uuid(conn), body))
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
