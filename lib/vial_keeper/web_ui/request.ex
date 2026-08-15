defmodule VialKeeper.WebUI.Request do
  @moduledoc """
  Form and query-parameter helpers for the embedded administration console.

  Handlers use these helpers instead of inventing a second request parser. Body
  size limits follow the host `max_request_bytes` ceiling used by the machine API.
  """

  alias VialKeeper.Error
  alias VialKeeper.JSON.StrictDecoder

  @uuid_re ~r/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i

  @doc """
  Fetches query parameters and, for mutation methods, parses an urlencoded body.

  Returns `{:ok, params, conn}` or `{:error, Error.t()}`.
  """
  @spec fetch_params(Plug.Conn.t()) ::
          {:ok, map(), Plug.Conn.t()} | {:error, Error.t()}
  def fetch_params(conn) do
    conn = Plug.Conn.fetch_query_params(conn)

    case conn.method do
      method when method in ["POST", "PUT", "PATCH", "DELETE"] ->
        read_form_body(conn)

      _ ->
        {:ok, conn.query_params, conn}
    end
  end

  @doc """
  Validates a UUID-shaped identifier used in UI path segments.
  """
  @spec require_uuid(term()) :: {:ok, String.t()} | {:error, Error.t()}
  def require_uuid(uuid) when is_binary(uuid) do
    if Regex.match?(@uuid_re, uuid) do
      {:ok, String.downcase(uuid)}
    else
      {:error, Error.invalid_request("database uuid is invalid")}
    end
  end

  def require_uuid(_), do: {:error, Error.invalid_request("database uuid is invalid")}

  @doc """
  Strictly decodes a textarea JSON field from form parameters.

  Empty or missing values are rejected.
  """
  @spec decode_json_field(map(), String.t() | atom()) ::
          {:ok, term()} | {:error, Error.t()}
  def decode_json_field(params, key) when is_map(params) do
    label = to_string(key)

    case param(params, key) do
      value when is_binary(value) ->
        trimmed = String.trim(value)

        if trimmed == "" do
          {:error, Error.invalid_request("#{label} must be non-empty JSON")}
        else
          StrictDecoder.decode(trimmed)
        end

      _ ->
        {:error, Error.invalid_request("#{label} must be non-empty JSON")}
    end
  end

  @doc """
  Reads a string-keyed form/query parameter with an optional default.
  """
  @spec param(map(), String.t() | atom(), term()) :: term()
  def param(params, key, default \\ nil) when is_map(params) do
    Map.get(params, to_string(key), default)
  end

  defp read_form_body(conn) do
    max = VialKeeper.Config.host_limits()[:max_request_bytes] || 2_097_152

    case Plug.Conn.read_body(conn, length: max + 1, read_length: min(max + 1, 65_536)) do
      {:ok, body, _conn} when byte_size(body) > max ->
        {:error, Error.payload_too_large("request body exceeds the configured limit")}

      {:ok, body, conn} ->
        merge_body_params(conn, body)

      {:more, _chunk, _conn} ->
        {:error, Error.payload_too_large("request body exceeds the configured limit")}

      {:error, reason} ->
        {:error, Error.invalid_request("request body could not be read", %{cause: inspect(reason)})}
    end
  end

  defp merge_body_params(conn, body) do
    content_type =
      conn
      |> Plug.Conn.get_req_header("content-type")
      |> List.first()
      |> to_string()

    body_params =
      cond do
        body == "" ->
          %{}

        String.starts_with?(content_type, "application/x-www-form-urlencoded") ->
          URI.decode_query(body)

        String.starts_with?(content_type, "multipart/form-data") ->
          %{}

        true ->
          # Plug.Test often omits content-type for encoded forms; still try decode.
          URI.decode_query(body)
      end

    {:ok, Map.merge(conn.query_params, body_params), conn}
  end
end
