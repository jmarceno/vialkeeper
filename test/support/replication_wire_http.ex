defmodule ElixirDB.TestReplicationWire do
  @moduledoc """
  Test helpers for Zstandard JSON on `/v1/databases/:uuid/replication` routes.
  """

  alias ElixirDB.HTTP.ReplicationWirePlug
  alias ElixirDB.Replication.WireCompression

  @spec wire_path?(binary()) :: boolean()
  def wire_path?(path), do: ReplicationWirePlug.wire_path?(path)

  @spec encode!(term()) :: WireCompression.encoded()
  def encode!(term) do
    case WireCompression.encode_json(term, decoded_limit()) do
      {:ok, encoded} -> encoded
      {:error, error} -> raise "replication wire encode failed: #{inspect(error)}"
    end
  end

  @spec accept_headers() :: [{binary(), binary()}]
  def accept_headers, do: [{"accept-encoding", "zstd"}]

  @spec json_headers(WireCompression.encoded()) :: [{binary(), binary()}]
  def json_headers(encoded) do
    [
      {"accept", "application/json"},
      {"accept-encoding", "zstd"},
      {"content-type", "application/json"},
      {"content-encoding", "zstd"},
      {"x-elixirdb-uncompressed-length", Integer.to_string(encoded.uncompressed_length)},
      {"content-length", Integer.to_string(encoded.compressed_length)}
    ]
  end

  @spec decode_response(term(), binary()) :: term()
  def decode_response(headers, body) when is_binary(body) and body != "" do
    case WireCompression.decode_json(body,
           decoded_limit: decoded_limit(),
           headers: headers,
           expect: :map_or_list
         ) do
      {:ok, value} -> value
      {:error, _} -> body
    end
  end

  def decode_response(_headers, body), do: body

  @spec request(atom(), binary(), term()) :: {:ok, map()} | {:error, term()}
  def request(method, url, body \\ nil) do
    options =
      if is_nil(body) do
        [headers: accept_headers(), decode_body: false, compressed: false]
      else
        encoded = encode!(body)

        [
          body: encoded.body,
          headers: json_headers(encoded),
          decode_body: false,
          compressed: false
        ]
      end

    wrap_response(dispatch(method, url, options))
  end

  defp dispatch(:get, url, options), do: Req.get(url, options)
  defp dispatch(:post, url, options), do: Req.post(url, options)
  defp dispatch(:put, url, options), do: Req.put(url, options)

  defp wrap_response({:ok, %{headers: headers, body: body} = response}) do
    {:ok, %{response | body: decode_response(headers, body)}}
  end

  defp wrap_response(other), do: other

  defp decoded_limit do
    ElixirDB.Config.host_limits()[:max_replication_batch_bytes] || 16_777_216
  end
end
