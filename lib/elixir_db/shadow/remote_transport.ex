defmodule ElixirDB.Shadow.RemoteTransport do
  @moduledoc "Authenticated bounded transport for shadow control-plane JSON."

  alias ElixirDB.Error
  alias ElixirDB.Replication.RemoteTransport, as: ReplicationTransport

  @spec request(binary(), atom(), binary(), map() | nil, binary() | nil, pos_integer()) ::
          {:ok, map()} | {:error, Error.t()}
  def request(base_url, method, path, body \\ nil, auth_token \\ nil, timeout \\ 30_000)

  def request(base_url, method, path, body, auth_token, timeout) do
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
  def open_stream(base_url, path, body, digest, auth_token, timeout) do
    ReplicationTransport.open_post_stream(base_url, path, body, digest, auth_token, timeout)
  end
end
