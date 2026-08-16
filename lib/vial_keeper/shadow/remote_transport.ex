defmodule VialKeeper.Shadow.RemoteTransport do
  @moduledoc "Authenticated bounded transport for shadow control-plane JSON."

  alias VialKeeper.Error
  alias VialKeeper.Replication.RemoteTransport, as: ReplicationTransport

  @spec request(binary(), atom(), binary(), map() | nil, binary() | nil, pos_integer() | nil) ::
          {:ok, map()} | {:error, Error.t()}
  def request(base_url, method, path, body \\ nil, auth_token \\ nil, timeout \\ nil)

  def request(base_url, method, path, body, auth_token, timeout) do
    timeout = timeout || VialKeeper.Config.request_timeout_ms()

    if valid_request?(base_url, method, path, body, auth_token, timeout) do
      checked_request(base_url, method, path, body, auth_token, timeout)
    else
      {:error, Error.invalid_request("shadow control transport request is invalid")}
    end
  end

  defp valid_request?(base_url, method, path, body, auth_token, timeout) do
    is_binary(base_url) and method in [:get, :put, :post, :delete] and is_binary(path) and
      (is_map(body) or is_nil(body)) and (is_binary(auth_token) or is_nil(auth_token)) and
      is_integer(timeout) and timeout > 0
  end

  defp checked_request(base_url, method, path, body, auth_token, timeout) do
    request = &ReplicationTransport.request/6

    case request.(base_url, method, path, body, auth_token, timeout) do
      {:ok, %{"data" => data}} -> {:ok, data}
      {:ok, data} when is_map(data) -> {:ok, data}
      {:ok, _} -> {:error, Error.shadow_incompatible("shadow control response must be an object")}
      {:error, _} = error -> error
    end
  end

  @doc "Opens a physical attachment response from a POST control request."
  @spec open_stream(binary(), binary(), map(), binary() | nil, binary() | nil, timeout()) ::
          {:ok, map(), Enumerable.t(), non_neg_integer(), binary() | nil}
          | {:error, Error.t()}
  def open_stream(base_url, path, body, digest, auth_token, timeout) do
    with {:ok, descriptor, body, headers} <-
           ReplicationTransport.open_post_stream(base_url, path, body, digest, auth_token, timeout),
         {:ok, watermark} <- watermark_header(headers) do
      {:ok, descriptor, body, watermark, content_type_header(headers)}
    end
  end

  defp watermark_header(headers) do
    value = VialKeeper.Headers.get(headers, "x-vialkeeper-source-watermark")

    case Integer.parse(to_string(value || "")) do
      {watermark, ""} when watermark >= 0 -> {:ok, watermark}
      _ -> {:error, Error.shadow_incompatible("shadow read watermark is missing")}
    end
  end

  defp content_type_header(headers) do
    case VialKeeper.Headers.get(headers, "x-vialkeeper-attachment-content-type") do
      value when is_binary(value) and value != "" -> value
      _ -> "application/octet-stream"
    end
  end
end
