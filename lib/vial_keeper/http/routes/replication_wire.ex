defmodule VialKeeper.HTTP.Routes.ReplicationWire do
  @moduledoc "HTTP routes for the replication wire protocol."
  use Plug.Router
  use VialKeeper.HTTP.RouterSpecs
  alias Plug.Conn
  alias Plug.Conn.Utils
  alias VialKeeper.Attachments.Manifest
  alias VialKeeper.Domain.Checkpoint
  alias VialKeeper.HTTP.{BodyReader, Request, Response}
  alias VialKeeper.HTTP.Schemas
  alias VialKeeper.Observability.Instrumentation.Replication, as: ReplicationInstr
  alias VialKeeper.Replication.BlobRepresentationStream
  alias VialKeeper.Replication.Wire
  alias VialKeeper.Runtime.DatabaseCatalog

  plug(:require_zstd_accept_encoding)
  plug(:reject_unexpected_get_delete_body)
  plug(:match)
  plug(:dispatch)

  get "/identity" do
    case replication_command(
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
          replication_command(
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
      replication_command(Request.uuid(conn), {:command, :list_peer_positions, %{}})
    )
  end

  get "/local-origin" do
    conn = Conn.fetch_query_params(conn)
    peer_database_uuid = conn.query_params["peer_database_uuid"]

    command =
      if is_binary(peer_database_uuid) and peer_database_uuid != "",
        do: {:command, :has_local_origin_changes, peer_database_uuid},
        else: {:command, :has_local_origin_changes}

    case replication_command(Request.uuid(conn), command) do
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

      Response.result(conn, replication_command(Request.uuid(conn), command))
    end)
  end

  get "/peers/:peer_database_uuid" do
    with_peer_path_id(conn, fn conn, peer_id ->
      Response.result(
        conn,
        replication_command(
          Request.uuid(conn),
          {:command, :get_local_record, "peer_ledger", peer_id}
        )
      )
    end)
  end

  get "/local-records/:namespace/:key" do
    with_record_path(conn, fn conn, namespace, key ->
      if namespace in ["retention_boundary_state", "shadow_checkpoints"] do
        Response.result(
          conn,
          replication_command(
            Request.uuid(conn),
            {:command, :get_local_record, namespace, key}
          )
        )
      else
        Response.error(
          conn,
          VialKeeper.Error.invalid_request("local record namespace is not readable")
        )
      end
    end)
  end

  put "/local-records/:namespace/:key" do
    Request.call(
      conn,
      Schemas.opts(:wire_checkpoint, "shadow checkpoint PUT has an invalid replacement"),
      fn body, conn ->
        with_record_path(conn, fn conn, namespace, key ->
          cond do
            namespace != "shadow_checkpoints" ->
              Response.error(
                conn,
                VialKeeper.Error.invalid_request("local record namespace is not writable")
              )

            not valid_checkpoint_put?(body) ->
              Response.error(
                conn,
                VialKeeper.Error.invalid_request("shadow checkpoint PUT has an invalid replacement")
              )

            true ->
              Response.result(
                conn,
                replication_command(
                  Request.uuid(conn),
                  {:command, :put_local_record,
                   %{
                     namespace: namespace,
                     key: key,
                     expected_version: body["expected_checkpoint_version"],
                     value: Map.delete(body, "expected_checkpoint_version")
                   }}
                )
              )
          end
        end)
      end
    )
  end

  put "/peers/:peer_database_uuid" do
    Request.call(conn, fn body, conn ->
      with_peer_path_id(conn, fn conn, peer_id ->
        body = Map.new(body, fn {k, v} -> {to_string(k), v} end)

        peer_fields =
          case Map.get(body, "value") do
            value when is_map(value) ->
              Map.new(value, fn {k, v} -> {to_string(k), v} end)

            _ ->
              body
              |> Map.delete("expected_version")
              |> Map.delete("bootstrap_completed")
          end
          |> Map.put("peer_database_uuid", peer_id)

        request = %{
          "expected_version" => Map.get(body, "expected_version", 0),
          "bootstrap_completed" => Map.get(body, "bootstrap_completed", false),
          "peer_database_uuid" => peer_id,
          "value" => peer_fields
        }

        Response.result(
          conn,
          replication_command(
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
        Response.result(
          conn,
          VialKeeper.Changes.wait(Request.uuid(conn), body, admission_class: :replication)
        )
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
          replication_command(
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
          replication_command(
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
          replication_command(
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
          Response.result(
            conn,
            VialKeeper.Attachments.diff_blobs(Request.uuid(conn), digests,
              admission_class: :replication
            )
          )
        else
          Response.error(
            conn,
            VialKeeper.Error.invalid_request("blob digests must be a list")
          )
        end
      end
    )
  end

  get "/blobs/:digest" do
    with_blob_digest(conn, fn conn, digest ->
      case VialKeeper.Attachments.open_blob_representation(Request.uuid(conn), digest,
             admission_class: :replication
           ) do
        {:ok, stream} -> send_blob_representation(conn, stream)
        {:error, error} -> Response.error(conn, error)
      end
    end)
  end

  put "/blobs/:digest" do
    with_blob_digest(conn, fn conn, digest ->
      case BlobRepresentationStream.parse_http_headers(conn.req_headers, digest) do
        {:ok, descriptor} ->
          case VialKeeper.Attachments.put_blob_representation(
                 Request.uuid(conn),
                 descriptor,
                 conn,
                 admission_class: :replication
               ) do
            {:ok, %Conn{} = conn} ->
              ReplicationInstr.wire_bytes(
                :ingress,
                :blob,
                descriptor.encoding,
                descriptor.payload_length
              )

              Response.result(conn, :ok)

            {:error, error} ->
              # The request body may be partially consumed; do not reuse the
              # connection with stale body accounting.
              conn
              |> Conn.put_resp_header("connection", "close")
              |> Response.error(error)
          end

        {:error, error} ->
          conn = discard_request_body(conn)
          Response.error(conn, error)
      end
    end)
  end

  get "/checkpoints/:replication_id" do
    with_path_id(conn, fn conn, replication_id ->
      Response.result(
        conn,
        replication_command(
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
                replication_command(
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
              VialKeeper.Error.invalid_request("checkpoint PUT has an invalid replacement")
            )
      end
    )
  end

  match _ do
    Response.error(
      conn,
      VialKeeper.Error.invalid_request("route not found", %{path: conn.request_path})
    )
  end

  defp replication_command(uuid, command),
    do: DatabaseCatalog.command_as(uuid, :replication, command)

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
    case replication_command(uuid, {:command, :read_boundary_pages, request}) do
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

  defp discard_request_body(conn) do
    case Conn.read_body(conn, length: 65_536, read_length: 65_536) do
      {:ok, _, conn} -> conn
      {:more, _, conn} -> discard_request_body(conn)
      {:error, _} -> conn
    end
  end

  defp require_zstd_accept_encoding(conn, _opts) do
    if accepts_zstd?(conn) do
      conn
    else
      conn
      |> Response.error(VialKeeper.Error.invalid_request("accept-encoding must include zstd"))
      |> Conn.halt()
    end
  end

  defp reject_unexpected_get_delete_body(conn, _opts) do
    if conn.method in ["GET", "DELETE"] do
      case BodyReader.reject_bodyless_payload(conn) do
        {:ok, conn} ->
          conn

        {:error, error} ->
          conn
          |> Response.error(error)
          |> Conn.halt()
      end
    else
      conn
    end
  end

  defp accepts_zstd?(conn) do
    conn
    |> Conn.get_req_header("accept-encoding")
    |> Enum.any?(&header_accepts_zstd?/1)
  end

  defp header_accepts_zstd?(header) when is_binary(header) do
    header
    |> Utils.list()
    |> Enum.any?(&coding_accepts_zstd?/1)
  end

  defp coding_accepts_zstd?(token) when is_binary(token) do
    case String.split(token, ";", parts: 2) do
      [coding] ->
        String.downcase(String.trim(coding)) == "zstd"

      [coding, params] ->
        String.downcase(String.trim(coding)) == "zstd" and quality(params) > 0
    end
  end

  defp quality(params) do
    params
    |> String.split(";")
    |> Enum.find_value(1.0, fn part ->
      case part |> String.trim() |> String.downcase() |> String.split("=", parts: 2) do
        ["q", value] -> parse_q(value)
        _ -> nil
      end
    end)
  end

  defp parse_q(value) do
    case Float.parse(String.trim(value)) do
      {q, ""} when q > 0 and q <= 1 -> q
      _ -> 0.0
    end
  end

  defp send_blob_representation(conn, stream) do
    ReplicationInstr.wire_bytes(:egress, :blob, stream.encoding, stream.payload_length)
    request_id = Response.request_id(conn)

    conn =
      Enum.reduce(BlobRepresentationStream.response_headers(stream), conn, fn {key, value}, conn ->
        Conn.put_resp_header(conn, key, value)
      end)

    conn =
      conn
      |> Conn.put_resp_header("x-request-id", request_id)
      |> Conn.send_chunked(200)

    Response.stream_chunks(conn, stream.body)
  end
end
