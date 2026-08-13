defmodule ElixirDB.HTTP.Routes.ShadowControl do
  @moduledoc "Authenticated compressed control-plane routes for shadow workers."
  use Plug.Router

  alias ElixirDB.HTTP.{Request, Response}
  alias ElixirDB.Replication.BlobRepresentationStream
  alias ElixirDB.Shadow.{Protocol, Worker}

  @provision_fields ~w(source_uuid shadow_uuid generation operation_id attachment_store_type attachment_location specification_digest source_base_url source_bearer_token)
  @read_fields ~w(source_uuid shadow_uuid generation operation_id id document_id revision include_conflicts requests name)

  plug(:match)
  plug(:dispatch)

  get "/capabilities" do
    Response.result(conn, Worker.capabilities())
  end

  put "/shadows/:source_uuid/generations/:generation" do
    with_path(conn, fn conn, source_uuid, generation ->
      Request.call(
        conn,
        [
          allowed_fields: @provision_fields,
          unknown_message: "shadow provision contains an unknown field"
        ],
        fn body, conn ->
          body = Protocol.string_keys(body)

          if body["source_uuid"] == source_uuid and body["generation"] == generation do
            Response.result(conn, Worker.provision(body), 202)
          else
            Response.error(
              conn,
              ElixirDB.Error.shadow_generation_conflict("shadow path and request identity differ")
            )
          end
        end
      )
    end)
  end

  get "/shadows/:source_uuid/generations/:generation" do
    with_path(conn, fn conn, source_uuid, generation ->
      Response.result(
        conn,
        Worker.inspect(%{"source_uuid" => source_uuid, "generation" => generation})
      )
    end)
  end

  delete "/shadows/:source_uuid/generations/:generation" do
    with_path(conn, fn conn, source_uuid, generation ->
      Response.result(
        conn,
        Worker.destroy(%{"source_uuid" => source_uuid, "generation" => generation})
      )
    end)
  end

  post "/shadows/:source_uuid/generations/:generation/reads/document" do
    read_request(conn, @read_fields, fn conn, body ->
      Response.result(conn, Worker.read_document(body, [], []))
    end)
  end

  post "/shadows/:source_uuid/generations/:generation/reads/documents/bulk" do
    read_request(conn, @read_fields, fn conn, body ->
      Response.result(conn, Worker.bulk_read_documents(body, [], []))
    end)
  end

  post "/shadows/:source_uuid/generations/:generation/reads/attachment" do
    read_request(conn, @read_fields, fn conn, body ->
      case Worker.open_attachment_representation(body, [], []) do
        {:ok, %{"stream" => stream, "source_watermark" => watermark} = result}
        when is_integer(watermark) and watermark >= 0 ->
          attachment = result["attachment"] || %{}
          content_type = attachment["content_type"] || "application/octet-stream"

          conn
          |> Plug.Conn.put_resp_header("x-elixirdb-source-watermark", Integer.to_string(watermark))
          |> Plug.Conn.put_resp_header("x-elixirdb-attachment-content-type", content_type)
          |> send_representation(stream)

        {:ok, _} ->
          Response.error(
            conn,
            ElixirDB.Error.shadow_incompatible("shadow attachment response is invalid")
          )

        {:error, error} ->
          Response.error(conn, error)
      end
    end)
  end

  match _ do
    Response.error(
      conn,
      ElixirDB.Error.invalid_request("route not found", %{path: conn.request_path})
    )
  end

  defp read_request(conn, fields, fun) do
    with_path(conn, fn conn, source_uuid, generation ->
      read_request_body(conn, fields, source_uuid, generation, fun)
    end)
  end

  defp read_request_body(conn, fields, source_uuid, generation, fun) do
    Request.call(
      conn,
      [allowed_fields: fields, unknown_message: "shadow read contains an unknown field"],
      fn body, conn ->
        dispatch_read(conn, Protocol.string_keys(body), source_uuid, generation, fun)
      end
    )
  end

  defp dispatch_read(conn, body, source_uuid, generation, fun) do
    if body["source_uuid"] == source_uuid and body["generation"] == generation do
      fun.(conn, body)
    else
      Response.error(
        conn,
        ElixirDB.Error.shadow_generation_conflict("shadow path and request identity differ")
      )
    end
  end

  defp with_path(conn, fun) do
    source_uuid = conn.path_params["source_uuid"]
    generation = conn.path_params["generation"]

    with :ok <- Request.validate_path_id(source_uuid),
         {:ok, generation} <- parse_generation(generation) do
      fun.(conn, source_uuid, generation)
    else
      {:error, error} -> Response.error(conn, error)
    end
  end

  defp parse_generation(value) when is_binary(value) do
    case Integer.parse(value) do
      {generation, ""} when generation > 0 -> {:ok, generation}
      _ -> {:error, ElixirDB.Error.invalid_request("shadow generation must be positive")}
    end
  end

  defp parse_generation(_),
    do: {:error, ElixirDB.Error.invalid_request("shadow generation is missing")}

  defp send_representation(conn, stream) do
    conn =
      conn
      |> put_representation_headers(BlobRepresentationStream.response_headers(stream))
      |> Plug.Conn.send_chunked(200)

    Response.stream_chunks(conn, stream.body)
  end

  defp put_representation_headers(conn, headers) do
    Enum.reduce(headers, conn, fn {name, value}, acc ->
      Plug.Conn.put_resp_header(acc, name, value)
    end)
  end
end
