defmodule VialKeeper.HTTP.ReplicationWirePlug do
  @moduledoc """
  Registers response compression for remote replication and shadow control JSON.

  Path-scoped to `/v1/databases/<segment>/replication` without validating the
  database UUID. Does not read or decompress the request body; authorization
  remains `VialKeeper.HTTP.AuthPlug`. Successful blob-representation bodies are
  left uncompressed.
  """

  @behaviour Plug

  import Plug.Conn

  alias VialKeeper.Error
  alias VialKeeper.HTTP.Response
  alias VialKeeper.Observability.Instrumentation.Replication, as: ReplicationInstr
  alias VialKeeper.Replication.BlobRepresentationStream
  alias VialKeeper.Replication.WireCompression

  @blob_media_type BlobRepresentationStream.media_type()
  @json_type "application/json"
  @content_encoding "zstd"
  @uncompressed_length_header "x-vialkeeper-uncompressed-length"
  @compress_private :vial_keeper_replication_wire_compress

  @impl true
  def init(opts), do: opts

  @impl true
  def call(conn, _opts) do
    if wire_path?(conn.request_path) do
      register_before_send(conn, &compress_json_response/1)
    else
      conn
    end
  end

  @doc """
  Returns true when `path` is under the remote replication wire prefix.

  Matches `/v1/databases/<segment>/replication` and nested paths. Does not
  match `/v1/databases/<segment>/replications` job-management routes.
  """
  @spec wire_path?(binary()) :: boolean()
  def wire_path?(path) when is_binary(path) do
    case String.split(path, "/", parts: 6) do
      ["", "v1", "control-plane" | _rest] ->
        true

      ["", "v1", "databases", segment, "replication"] when segment != "" ->
        true

      ["", "v1", "databases", segment, "replication", _rest] when segment != "" ->
        true

      _ ->
        false
    end
  end

  def wire_path?(_), do: false

  defp compress_json_response(conn) do
    cond do
      conn.state not in [:set, :unset, :file] ->
        conn

      blob_representation?(conn) ->
        conn

      not json_response?(conn) ->
        conn

      empty_body?(conn) ->
        conn

      true ->
        encode_response(conn)
    end
  end

  # The response body was already JSON-encoded by VialKeeper.HTTP.Response, so
  # it is compressed as-is instead of decoding and re-encoding the envelope.
  defp encode_response(conn) do
    limit = decoded_limit()

    case ReplicationInstr.wire_codec(:egress, :compress, fn ->
           WireCompression.compress_encoded_json(conn.resp_body, limit)
         end) do
      {:ok, encoded} ->
        ReplicationInstr.wire_bytes(:egress, :json, :zstd, encoded.compressed_length)
        put_compressed(conn, encoded)

      {:error, _} ->
        fallback_internal_error(conn)
    end
  end

  defp fallback_internal_error(conn) do
    if conn.private[@compress_private] == :failed do
      conn
    else
      conn = put_private(conn, @compress_private, :failed)
      request_id = Response.request_id(conn)
      error = Error.internal_error()

      envelope = %{
        "request_id" => request_id,
        "error" => Error.public(error)
      }

      conn =
        conn
        |> put_resp_header("x-request-id", request_id)
        |> Map.put(:status, error.http_status)
        |> Map.put(:resp_body, JSON.encode_to_iodata!(envelope))
        |> put_resp_content_type(@json_type)

      case WireCompression.encode_json(envelope, decoded_limit()) do
        {:ok, encoded} -> put_compressed(conn, encoded)
        {:error, _} -> conn
      end
    end
  end

  defp put_compressed(conn, encoded) do
    conn
    |> put_resp_content_type(@json_type)
    |> put_resp_header("content-encoding", @content_encoding)
    |> put_resp_header(@uncompressed_length_header, Integer.to_string(encoded.uncompressed_length))
    |> put_resp_header("vary", "accept-encoding")
    |> put_resp_header("content-length", Integer.to_string(encoded.compressed_length))
    |> Map.put(:resp_body, encoded.body)
  end

  defp json_response?(conn) do
    case content_type(conn) do
      type when is_binary(type) -> String.starts_with?(String.downcase(type), @json_type)
      _ -> false
    end
  end

  defp blob_representation?(conn) do
    case content_type(conn) do
      type when is_binary(type) -> String.starts_with?(String.downcase(type), @blob_media_type)
      _ -> false
    end
  end

  defp content_type(conn) do
    case get_resp_header(conn, "content-type") do
      [value | _] -> value
      _ -> nil
    end
  end

  defp empty_body?(conn) do
    case conn.resp_body do
      nil -> true
      body -> IO.iodata_length(body) == 0
    end
  rescue
    _error in [ArgumentError, ErlangError, Protocol.UndefinedError] ->
      true
  end

  defp decoded_limit do
    VialKeeper.Config.host_limits()[:max_replication_batch_bytes] || 16_777_216
  end
end
