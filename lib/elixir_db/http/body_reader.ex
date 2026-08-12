defmodule ElixirDB.HTTP.BodyReader do
  @moduledoc "Reads and validates bounded JSON request bodies at the HTTP boundary."

  alias ElixirDB.HTTP.ReplicationWirePlug
  alias ElixirDB.JSON.StrictDecoder
  alias ElixirDB.Replication.WireCompression

  @doc """
  Reads and decodes a Version 1 JSON request body.

  Options:

    * `:allowed_fields` — when set, reject unknown top-level object fields (`API-009`)
    * `:unknown_message` — error message used when an unknown field is present

  Paths under `/v1/databases/<segment>/replication` use the bounded Zstandard
  JSON codec. Public routes remain uncompressed JSON.
  """
  def read(conn, opts \\ []) do
    if ReplicationWirePlug.wire_path?(conn.request_path) do
      read_replication(conn, opts)
    else
      read_public(conn, opts)
    end
  end

  @doc """
  Rejects a non-empty body on bodyless GET/DELETE replication requests.
  """
  @spec reject_bodyless_payload(Plug.Conn.t()) ::
          {:ok, Plug.Conn.t()} | {:error, ElixirDB.Error.t()}
  def reject_bodyless_payload(conn) do
    cond do
      content_length_positive?(conn) ->
        {:error, ElixirDB.Error.invalid_request("request body is not allowed")}

      transfer_encoded?(conn) ->
        {:error, ElixirDB.Error.invalid_request("request body is not allowed")}

      true ->
        case Plug.Conn.read_body(conn, length: 1, read_length: 1) do
          {:ok, "", conn} ->
            {:ok, conn}

          {:ok, _body, _conn} ->
            {:error, ElixirDB.Error.invalid_request("request body is not allowed")}

          {:more, _body, _conn} ->
            {:error, ElixirDB.Error.invalid_request("request body is not allowed")}

          {:error, reason} ->
            {:error,
             ElixirDB.Error.invalid_request("request body could not be read", %{
               cause: inspect(reason)
             })}
        end
    end
  end

  @doc """
  Rejects maps that contain keys outside `allowed`.

  Non-maps are always rejected when an allow-list is enforced, matching the
  previous per-route `unknown_fields?/2` behavior.
  """
  def reject_unknown_fields(body, allowed, message)
      when is_list(allowed) and is_binary(message) do
    if unknown_fields?(body, allowed) do
      {:error, ElixirDB.Error.invalid_request(message)}
    else
      :ok
    end
  end

  defp read_public(conn, opts) do
    max = ElixirDB.Config.host_limits()[:max_request_bytes] || 2_097_152
    content_type = conn |> Plug.Conn.get_req_header("content-type") |> List.first()

    cond do
      conn.method in ["GET", "DELETE"] ->
        {:ok, %{}, conn}

      is_nil(content_type) or not String.starts_with?(content_type, "application/json") ->
        {:error, ElixirDB.Error.invalid_request("request content type must be application/json")}

      true ->
        with {:ok, body, conn} <- read_chunks(conn, max, []),
             :ok <- maybe_reject_unknown_fields(body, opts) do
          {:ok, body, conn}
        end
    end
  end

  defp read_replication(conn, opts) do
    decoded_limit = replication_decoded_limit()

    if conn.method in ["GET", "DELETE"] do
      with {:ok, conn} <- reject_bodyless_payload(conn) do
        {:ok, %{}, conn}
      end
    else
      with {:ok, compressed, conn} <-
             read_chunks_raw(conn, WireCompression.encoded_limit(decoded_limit), []),
           {:ok, body} <-
             WireCompression.decode_json(compressed,
               decoded_limit: decoded_limit,
               headers: conn.req_headers,
               expect: :map_or_list
             ),
           :ok <- maybe_reject_unknown_fields(body, opts) do
        {:ok, body, conn}
      end
    end
  end

  defp maybe_reject_unknown_fields(body, opts) do
    case Keyword.get(opts, :allowed_fields) do
      nil ->
        :ok

      allowed when is_list(allowed) ->
        message =
          Keyword.get(opts, :unknown_message, "request contains an unknown field")

        reject_unknown_fields(body, allowed, message)
    end
  end

  defp unknown_fields?(map, allowed) when is_map(map),
    do: Enum.any?(Map.keys(map), &(&1 not in allowed))

  defp unknown_fields?(_, _), do: true

  defp read_chunks(conn, max, chunks) do
    case read_chunks_raw(conn, max, chunks) do
      {:ok, body, conn} -> decode(body, conn, max)
      {:error, _} = error -> error
    end
  end

  defp read_chunks_raw(conn, max, chunks) do
    case Plug.Conn.read_body(conn, length: max + 1, read_length: min(max + 1, 65_536)) do
      {:ok, body, conn} ->
        finish_raw(Enum.reverse([body | chunks]), conn, max)

      {:more, body, conn} ->
        size = Enum.reduce(chunks, byte_size(body), &(byte_size(&1) + &2))

        if size > max,
          do:
            {:error, ElixirDB.Error.payload_too_large("request body exceeds the configured limit")},
          else: read_chunks_raw(conn, max, [body | chunks])

      {:error, reason} ->
        {:error,
         ElixirDB.Error.invalid_request("request body could not be read", %{
           cause: inspect(reason)
         })}
    end
  end

  defp finish_raw(chunks, conn, max) do
    body = IO.iodata_to_binary(chunks)

    if byte_size(body) > max do
      {:error, ElixirDB.Error.payload_too_large("request body exceeds the configured limit")}
    else
      {:ok, body, conn}
    end
  end

  defp decode(body, conn, max) do
    case StrictDecoder.decode(body, max_bytes: max) do
      {:ok, value} when is_map(value) or is_list(value) ->
        {:ok, value, conn}

      {:ok, _} ->
        {:error, ElixirDB.Error.invalid_request("request body must be an object or array")}

      {:error, error} ->
        {:error, error}
    end
  end

  defp content_length_positive?(conn) do
    case Plug.Conn.get_req_header(conn, "content-length") do
      [value | _] ->
        case Integer.parse(value) do
          {n, ""} when n > 0 -> true
          _ -> false
        end

      _ ->
        false
    end
  end

  defp transfer_encoded?(conn) do
    case Plug.Conn.get_req_header(conn, "transfer-encoding") do
      [value | _] -> String.trim(value) != ""
      _ -> false
    end
  end

  defp replication_decoded_limit do
    ElixirDB.Config.host_limits()[:max_replication_batch_bytes] || 16_777_216
  end
end
