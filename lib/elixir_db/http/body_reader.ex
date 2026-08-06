defmodule ElixirDB.HTTP.BodyReader do
  @moduledoc false

  def read(conn) do
    max = ElixirDB.Config.host_limits()[:max_request_bytes] || 2_097_152
    content_type = conn |> Plug.Conn.get_req_header("content-type") |> List.first()

    cond do
      conn.method in ["GET", "DELETE"] ->
        {:ok, %{}, conn}

      is_nil(content_type) or not String.starts_with?(content_type, "application/json") ->
        {:error, ElixirDB.Error.invalid_request("request content type must be application/json")}

      true ->
        read_chunks(conn, max, [])
    end
  end

  defp read_chunks(conn, max, chunks) do
    case Plug.Conn.read_body(conn, length: max + 1, read_length: min(max + 1, 65_536)) do
      {:ok, body, conn} ->
        decode(Enum.reverse([body | chunks]), conn, max)

      {:more, body, conn} ->
        size = Enum.reduce(chunks, byte_size(body), &(byte_size(&1) + &2))

        if size > max,
          do:
            {:error, ElixirDB.Error.payload_too_large("request body exceeds the configured limit")},
          else: read_chunks(conn, max, [body | chunks])

      {:error, reason} ->
        {:error,
         ElixirDB.Error.invalid_request("request body could not be read", %{
           cause: inspect(reason)
         })}
    end
  end

  defp decode(chunks, conn, max) do
    body = IO.iodata_to_binary(chunks)

    if byte_size(body) > max do
      {:error, ElixirDB.Error.payload_too_large("request body exceeds the configured limit")}
    else
      case ElixirDB.JSON.StrictDecoder.decode(body, max_bytes: max) do
        {:ok, value} when is_map(value) or is_list(value) ->
          {:ok, value, conn}

        {:ok, _} ->
          {:error, ElixirDB.Error.invalid_request("request body must be an object or array")}

        {:error, error} ->
          {:error, error}
      end
    end
  end
end
