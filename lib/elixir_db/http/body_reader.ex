defmodule ElixirDB.HTTP.BodyReader do
  alias ElixirDB.JSON.StrictDecoder
  @moduledoc "Reads and validates bounded JSON request bodies at the HTTP boundary."

  @doc """
  Reads and decodes a Version 1 JSON request body.

  Options:

  * `:allowed_fields` — when set, reject unknown top-level object fields (`API-009`)
  * `:unknown_message` — error message used when an unknown field is present
  """
  def read(conn, opts \\ []) do
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
      case StrictDecoder.decode(body, max_bytes: max) do
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
