defmodule VialKeeper.HTTP.Routes.Attachments do
  @moduledoc "HTTP routes for attachment upload, metadata, and download operations."
  use Plug.Router
  use VialKeeper.HTTP.RouterSpecs

  alias Plug.Conn
  alias VialKeeper.Attachments
  alias VialKeeper.HTTP.{Request, Response, Schemas}

  plug(:match)
  plug(:dispatch)

  post "/upload" do
    uuid = Request.uuid(conn)

    case require_octet_stream(conn) do
      :ok ->
        case Attachments.upload_stream(uuid, conn) do
          {:ok, data, conn} ->
            Response.ok(conn, data, 201)

          {:error, error} ->
            # The request body may be partially consumed; do not reuse the
            # connection with stale body accounting.
            conn
            |> Conn.put_resp_header("connection", "close")
            |> Response.error(error)
        end

      {:error, error} ->
        Response.error(conn, error)
    end
  end

  post "/get" do
    Request.call(
      conn,
      Schemas.opts(:attachment_get, "attachment get contains an unknown field"),
      fn body, conn ->
        with {:ok, consistency} <- Request.read_consistency(conn),
             {:ok, stream, meta} <-
               Attachments.open_stream_with_meta(
                 Request.uuid(conn),
                 body,
                 read_consistency: consistency
               ) do
          send_attachment(Response.put_read_headers(conn, meta), stream)
        else
          {:error, error} -> Response.error(conn, error)
        end
      end
    )
  end

  match _ do
    Response.error(
      conn,
      VialKeeper.Error.invalid_request("route not found", %{path: conn.request_path})
    )
  end

  defp require_octet_stream(conn) do
    content_type = conn |> Conn.get_req_header("content-type") |> List.first()

    cond do
      is_nil(content_type) ->
        {:error,
         VialKeeper.Error.invalid_request(
           "attachment upload content type must be application/octet-stream"
         )}

      String.starts_with?(content_type, "application/octet-stream") ->
        :ok

      true ->
        {:error,
         VialKeeper.Error.invalid_request(
           "attachment upload content type must be application/octet-stream"
         )}
    end
  end

  defp send_attachment(conn, stream) do
    request_id = Response.request_id(conn)

    try do
      conn =
        conn
        |> Conn.put_resp_header("x-request-id", request_id)
        |> Conn.put_resp_header("content-type", stream.content_type)
        |> Conn.put_resp_header("content-length", Integer.to_string(stream.content_length))
        |> Conn.put_resp_header("etag", stream.etag)
        |> Conn.send_chunked(200)

      Response.stream_chunks(conn, stream.body)
    after
      stream.close.()
    end
  end
end
