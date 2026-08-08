defmodule ElixirDB.HTTP.Routes.ReplicationWire do
  @moduledoc false
  use Plug.Router
  alias ElixirDB.Attachments.Manifest
  alias ElixirDB.Domain.Checkpoint
  alias ElixirDB.HTTP.{Request, Response}
  alias ElixirDB.HTTP.Schemas
  alias ElixirDB.Replication.Wire
  alias ElixirDB.Runtime.DatabaseCatalog
  plug(:match)
  plug(:dispatch)

  get "/identity" do
    case DatabaseCatalog.command(
           Request.uuid(conn),
           {:command, :identity, %{}}
         ) do
      {:ok, identity} ->
        Response.ok(conn, Wire.identity(identity))

      {:error, error} ->
        Response.error(conn, error)
    end
  end

  post "/boundaries" do
    Request.call(
      conn,
      Schemas.opts(:wire_boundaries, "boundary page request contains an unknown field"),
      fn body, conn ->
        request =
          body
          |> Map.new(fn {k, v} -> {to_string(k), v} end)
          |> boundary_request()

        Response.result(
          conn,
          boundary_page_result(Request.uuid(conn), request)
        )
      end
    )
  end

  post "/boundaries/install" do
    Request.call(
      conn,
      Schemas.opts(:wire_boundary_install, "boundary install request contains an unknown field"),
      fn body, conn ->
        Response.result(
          conn,
          DatabaseCatalog.command(
            Request.uuid(conn),
            {:command, :install_boundary_pages, body}
          )
        )
      end
    )
  end

  get "/peers" do
    Response.result(
      conn,
      DatabaseCatalog.command(Request.uuid(conn), {:command, :list_peer_positions, %{}})
    )
  end

  get "/local-origin" do
    conn = Plug.Conn.fetch_query_params(conn)
    peer_database_uuid = conn.query_params["peer_database_uuid"]

    command =
      if is_binary(peer_database_uuid) and peer_database_uuid != "",
        do: {:command, :has_local_origin_changes, peer_database_uuid},
        else: {:command, :has_local_origin_changes}

    case DatabaseCatalog.command(Request.uuid(conn), command) do
      {:ok, has_local?} when is_boolean(has_local?) ->
        Response.ok(conn, %{"has_local_origin_changes" => has_local?})

      {:error, error} ->
        Response.error(conn, error)
    end
  end

  post "/local-origin/clear" do
    Request.call(conn, fn body, conn ->
      peer_database_uuid = body["peer_database_uuid"]

      command =
        if is_binary(peer_database_uuid) and peer_database_uuid != "",
          do: {:command, :clear_pending_local_causal, peer_database_uuid},
          else: {:command, :clear_pending_local_causal}

      Response.result(conn, DatabaseCatalog.command(Request.uuid(conn), command))
    end)
  end

  get "/peers/:peer_database_uuid" do
    with_peer_path_id(conn, fn conn, peer_id ->
      Response.result(
        conn,
        DatabaseCatalog.command(
          Request.uuid(conn),
          {:command, :get_local_record, "peer_ledger", peer_id}
        )
      )
    end)
  end

  get "/local-records/:namespace/:key" do
    with_record_path(conn, fn conn, namespace, key ->
      if namespace == "retention_boundary_state" do
        Response.result(
          conn,
          DatabaseCatalog.command(
            Request.uuid(conn),
            {:command, :get_local_record, namespace, key}
          )
        )
      else
        Response.error(
          conn,
          ElixirDB.Error.invalid_request("local record namespace is not readable")
        )
      end
    end)
  end

  put "/peers/:peer_database_uuid" do
    Request.call(conn, fn body, conn ->
      with_peer_path_id(conn, fn conn, peer_id ->
        peer_fields =
          body
          |> Map.new(fn {k, v} -> {to_string(k), v} end)
          |> Map.delete("expected_version")
          |> Map.delete("bootstrap_completed")
          |> Map.put("peer_database_uuid", peer_id)

        request = %{
          "expected_version" => Map.get(body, "expected_version", 0),
          "bootstrap_completed" => Map.get(body, "bootstrap_completed", false),
          "peer_database_uuid" => peer_id,
          "value" => peer_fields
        }

        Response.result(
          conn,
          DatabaseCatalog.command(
            Request.uuid(conn),
            {:command, :put_peer_position_cas, request}
          )
        )
      end)
    end)
  end

  post "/changes" do
    Request.call(
      conn,
      Schemas.opts(:wire_changes, "replication changes contains an unknown field"),
      fn body, conn ->
        Response.result(conn, ElixirDB.Changes.wait(Request.uuid(conn), body))
      end
    )
  end

  post "/revisions/diff" do
    Request.call(
      conn,
      Schemas.opts(:wire_diff, "revision diff contains an unknown field"),
      fn body, conn ->
        Response.result(
          conn,
          DatabaseCatalog.command(
            Request.uuid(conn),
            {:command, :diff_revisions, body}
          )
        )
      end
    )
  end

  post "/revisions/get" do
    Request.call(
      conn,
      Schemas.opts(:wire_get_chains, "revision get contains an unknown field"),
      fn body, conn ->
        Response.result(
          conn,
          DatabaseCatalog.command(
            Request.uuid(conn),
            {:command, :get_revision_chains, body}
          )
        )
      end
    )
  end

  post "/revisions/put" do
    Request.call(
      conn,
      Schemas.opts(:wire_put_chains, "revision put contains an unknown field"),
      fn body, conn ->
        Response.result(
          conn,
          DatabaseCatalog.command(
            Request.uuid(conn),
            {:command, :import_revision_chains, body}
          )
        )
      end
    )
  end

  post "/blobs/diff" do
    Request.call(
      conn,
      Schemas.opts(:wire_blob_diff, "blob diff contains an unknown field"),
      fn body, conn ->
        digests = body["digests"]

        if is_list(digests) do
          Response.result(conn, ElixirDB.Attachments.diff_blobs(Request.uuid(conn), digests))
        else
          Response.error(
            conn,
            ElixirDB.Error.invalid_request("blob digests must be a list")
          )
        end
      end
    )
  end

  get "/blobs/:digest" do
    with_blob_digest(conn, fn conn, digest ->
      case ElixirDB.Attachments.open_blob(Request.uuid(conn), digest) do
        {:ok, stream} -> send_blob(conn, stream)
        {:error, error} -> Response.error(conn, error)
      end
    end)
  end

  put "/blobs/:digest" do
    with_blob_digest(conn, fn conn, digest ->
      case require_octet_stream(conn) do
        :ok ->
          case content_length(conn) do
            {:ok, length} ->
              Response.result(
                conn,
                ElixirDB.Attachments.put_blob(Request.uuid(conn), digest, length, conn)
              )

            {:error, error} ->
              Response.error(conn, error)
          end

        {:error, error} ->
          Response.error(conn, error)
      end
    end)
  end

  get "/checkpoints/:replication_id" do
    with_path_id(conn, fn conn, replication_id ->
      Response.result(
        conn,
        DatabaseCatalog.command(
          Request.uuid(conn),
          {:command, :get_local_record, "checkpoints", replication_id}
        )
      )
    end)
  end

  put "/checkpoints/:replication_id" do
    Request.call(
      conn,
      Schemas.opts(:wire_checkpoint, "checkpoint PUT has an invalid replacement"),
      fn body, conn ->
        if valid_checkpoint_put?(body),
          do:
            with_path_id(conn, fn conn, replication_id ->
              Response.result(
                conn,
                DatabaseCatalog.command(
                  Request.uuid(conn),
                  {:command, :put_local_record,
                   %{
                     namespace: "checkpoints",
                     key: replication_id,
                     expected_version: body["expected_checkpoint_version"],
                     value: Map.delete(body, "expected_checkpoint_version")
                   }}
                )
              )
            end),
          else:
            Response.error(
              conn,
              ElixirDB.Error.invalid_request("checkpoint PUT has an invalid replacement")
            )
      end
    )
  end

  match _ do
    Response.error(
      conn,
      ElixirDB.Error.invalid_request("route not found", %{path: conn.request_path})
    )
  end

  defp valid_checkpoint_put?(body), do: Checkpoint.valid_wire_put?(body)

  defp boundary_request(body) when is_map(body) do
    cursor = body["page_cursor"] || body["cursor"]

    %{
      "source_history_epoch" => body["source_history_epoch"],
      "compaction_epoch" => body["compaction_epoch"],
      "cursor" => cursor,
      "limit" => body["limit"]
    }
    |> Enum.reject(fn {_key, value} -> is_nil(value) end)
    |> Map.new()
  end

  defp boundary_page_result(uuid, request) do
    case DatabaseCatalog.command(uuid, {:command, :read_boundary_pages, request}) do
      {:ok, page} -> {:ok, Wire.boundary_page(page)}
      error -> error
    end
  end

  # SAFETY: bounds-check the :replication_id path parameter before it is used as a storage
  # key. See Request.validate_path_id/1.
  defp with_path_id(conn, fun) do
    case Request.validate_path_id(conn.path_params["replication_id"]) do
      :ok -> fun.(conn, conn.path_params["replication_id"])
      {:error, error} -> Response.error(conn, error)
    end
  end

  defp with_peer_path_id(conn, fun) do
    case Request.validate_path_id(conn.path_params["peer_database_uuid"]) do
      :ok -> fun.(conn, conn.path_params["peer_database_uuid"])
      {:error, error} -> Response.error(conn, error)
    end
  end

  defp with_record_path(conn, fun) do
    with :ok <- Request.validate_path_id(conn.path_params["namespace"]),
         :ok <- Request.validate_path_id(conn.path_params["key"]) do
      fun.(conn, conn.path_params["namespace"], conn.path_params["key"])
    else
      {:error, error} -> Response.error(conn, error)
    end
  end

  defp with_blob_digest(conn, fun) do
    case Manifest.validate_digest(conn.path_params["digest"]) do
      {:ok, digest} -> fun.(conn, digest)
      {:error, error} -> Response.error(conn, error)
    end
  end

  defp require_octet_stream(conn) do
    content_type = conn |> Plug.Conn.get_req_header("content-type") |> List.first()

    cond do
      is_nil(content_type) ->
        {:error,
         ElixirDB.Error.invalid_request(
           "replication blob content type must be application/octet-stream"
         )}

      String.starts_with?(content_type, "application/octet-stream") ->
        :ok

      true ->
        {:error,
         ElixirDB.Error.invalid_request(
           "replication blob content type must be application/octet-stream"
         )}
    end
  end

  defp content_length(conn) do
    case Plug.Conn.get_req_header(conn, "content-length") do
      [value | _] ->
        case Integer.parse(value) do
          {length, ""} when length >= 0 ->
            {:ok, length}

          _ ->
            {:error,
             ElixirDB.Error.invalid_request("content-length must be a non-negative integer")}
        end

      _ ->
        {:error, ElixirDB.Error.invalid_request("content-length is required")}
    end
  end

  defp send_blob(conn, stream) do
    request_id = Response.request_id(conn)

    conn =
      conn
      |> Plug.Conn.put_resp_header("x-request-id", request_id)
      |> Plug.Conn.put_resp_header("content-type", "application/octet-stream")
      |> Plug.Conn.put_resp_header("content-length", Integer.to_string(stream.length))
      |> Plug.Conn.send_chunked(200)

    Response.stream_chunks(conn, stream.body)
  end
end
