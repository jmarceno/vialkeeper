defmodule ElixirDB.Replication.WireCompression do
  @moduledoc """
  Bounded one-shot Zstandard JSON codec for remote replication envelopes.

  Encoding uses the project JSON encoder and a single `:ezstd` pass at level 1
  so the frame declares its content size. Decoding enforces encoded and decoded
  limits, required content headers, exact-frame validation, and `StrictDecoder`
  before returning a map or list.
  """

  alias ElixirDB.Error
  alias ElixirDB.Headers
  alias ElixirDB.JSON.StrictDecoder
  alias ElixirDB.Replication.ZstdFrame

  @content_type "application/json"
  @content_encoding "zstd"
  @uncompressed_length_header "x-elixirdb-uncompressed-length"
  @compression_level 1

  @type encoded :: %{
          body: binary(),
          uncompressed_length: non_neg_integer(),
          compressed_length: non_neg_integer()
        }

  @spec encoded_limit(non_neg_integer()) :: non_neg_integer()
  def encoded_limit(decoded_limit) when is_integer(decoded_limit) and decoded_limit >= 0 do
    decoded_limit + div(decoded_limit, 256) + 64
  end

  @spec encode_json(term(), integer()) :: {:ok, encoded()} | {:error, Error.t()}
  def encode_json(term, decoded_limit) when is_integer(decoded_limit) and decoded_limit >= 0 do
    with {:ok, json} <- encode_term(term, decoded_limit),
         {:ok, compressed} <- compress(json),
         :ok <- reject_oversize(byte_size(compressed), encoded_limit(decoded_limit)) do
      {:ok,
       %{
         body: compressed,
         uncompressed_length: byte_size(json),
         compressed_length: byte_size(compressed)
       }}
    end
  end

  def encode_json(_term, _decoded_limit),
    do: {:error, Error.invalid_request("replication JSON decoded limit is invalid")}

  @doc """
  Compresses JSON that the project encoder already produced.

  Skips the term round-trip that `encode_json/2` performs; the caller
  guarantees `json` is valid encoder output. Both byte limits are still
  enforced.
  """
  @spec compress_encoded_json(iodata(), integer()) :: {:ok, encoded()} | {:error, Error.t()}
  def compress_encoded_json(json, decoded_limit)
      when is_integer(decoded_limit) and decoded_limit >= 0 do
    with {:ok, json} <- bounded_body(json, decoded_limit),
         {:ok, compressed} <- compress(json),
         :ok <- reject_oversize(byte_size(compressed), encoded_limit(decoded_limit)) do
      {:ok,
       %{
         body: compressed,
         uncompressed_length: byte_size(json),
         compressed_length: byte_size(compressed)
       }}
    end
  end

  def compress_encoded_json(_json, _decoded_limit),
    do: {:error, Error.invalid_request("replication JSON decoded limit is invalid")}

  @spec decode_json(iodata(), keyword()) :: {:ok, term()} | {:error, Error.t()}
  def decode_json(compressed, opts) when is_list(opts) do
    with {:ok, decoded_limit} <- decoded_limit(opts),
         {:ok, headers} <- headers(opts),
         {:ok, expect} <- expect(opts),
         {:ok, uncompressed_length} <- uncompressed_length(headers),
         :ok <- reject_oversize(uncompressed_length, decoded_limit),
         :ok <- require_content_headers(headers),
         {:ok, body} <- bounded_body(compressed, encoded_limit(decoded_limit)),
         {:ok, meta} <- ZstdFrame.validate(body, expected_content_size: uncompressed_length),
         :ok <- reject_oversize(meta.content_size, decoded_limit),
         {:ok, json} <- decompress(body),
         :ok <- match_decoded_size(json, uncompressed_length),
         {:ok, value} <-
           StrictDecoder.decode(json,
             max_bytes: uncompressed_length,
             max_depth: Keyword.get(opts, :max_depth, default_max_depth())
           ),
         :ok <- accept_shape(value, expect) do
      {:ok, value}
    end
  end

  def decode_json(_compressed, _opts),
    do: {:error, Error.invalid_request("replication JSON codec options are invalid")}

  defp encode_term(term, decoded_limit) do
    iodata = JSON.encode_to_iodata!(term)
    size = IO.iodata_length(iodata)

    with :ok <- reject_oversize(size, decoded_limit) do
      {:ok, IO.iodata_to_binary(iodata)}
    end
  rescue
    _error in [ArgumentError, Protocol.UndefinedError, ErlangError] ->
      {:error, Error.invalid_request("replication JSON could not be encoded")}
  end

  defp compress(json) do
    case :ezstd.compress(json, @compression_level) do
      compressed when is_binary(compressed) -> {:ok, compressed}
      {:error, _reason} -> nif_error()
    end
  rescue
    _error in [ArgumentError, ErlangError] -> nif_error()
  end

  defp decompress(body) do
    case :ezstd.decompress(body) do
      json when is_binary(json) -> {:ok, json}
      {:error, _reason} -> nif_error()
    end
  rescue
    _error in [ArgumentError, ErlangError] -> nif_error()
  end

  defp bounded_body(body, encoded_limit) when is_binary(body) do
    with :ok <- reject_oversize(byte_size(body), encoded_limit) do
      {:ok, body}
    end
  end

  defp bounded_body(body, encoded_limit) do
    size = IO.iodata_length(body)

    with :ok <- reject_oversize(size, encoded_limit) do
      {:ok, IO.iodata_to_binary(body)}
    end
  rescue
    _error in [ArgumentError, ErlangError, Protocol.UndefinedError] ->
      {:error, Error.invalid_request("replication JSON body is invalid")}
  end

  defp decoded_limit(opts) do
    case Keyword.get(opts, :decoded_limit) do
      limit when is_integer(limit) and limit >= 0 ->
        {:ok, limit}

      _ ->
        {:error, Error.invalid_request("replication JSON decoded limit is invalid")}
    end
  end

  defp headers(opts) do
    case Keyword.get(opts, :headers) do
      headers when is_map(headers) or is_list(headers) ->
        {:ok, headers}

      _ ->
        {:error, Error.invalid_request("replication JSON headers are required")}
    end
  end

  defp expect(opts) do
    case Keyword.get(opts, :expect, :map_or_list) do
      expect when expect in [:map, :list, :map_or_list] ->
        {:ok, expect}

      _ ->
        {:error, Error.invalid_request("replication JSON expected shape is invalid")}
    end
  end

  defp uncompressed_length(headers) do
    parse_canonical_non_neg_integer(Headers.get(headers, @uncompressed_length_header))
  end

  defp parse_canonical_non_neg_integer(value) when is_binary(value) do
    case Integer.parse(value) do
      {int, ""} when int >= 0 ->
        if Integer.to_string(int) == value do
          {:ok, int}
        else
          length_error()
        end

      _ ->
        length_error()
    end
  end

  defp parse_canonical_non_neg_integer(_), do: length_error()

  defp require_content_headers(headers) do
    case require_content_encoding(Headers.get(headers, "content-encoding")) do
      :ok -> require_content_type(Headers.get(headers, "content-type"))
      {:error, _} = error -> error
    end
  end

  defp require_content_encoding(value) when is_binary(value) do
    if String.downcase(value) == @content_encoding do
      :ok
    else
      {:error, Error.invalid_request("replication JSON content encoding must be zstd")}
    end
  end

  defp require_content_encoding(_),
    do: {:error, Error.invalid_request("replication JSON content encoding must be zstd")}

  defp require_content_type(value) when is_binary(value) do
    if String.starts_with?(String.downcase(value), @content_type) do
      :ok
    else
      {:error, Error.invalid_request("replication JSON content type must be application/json")}
    end
  end

  defp require_content_type(_),
    do: {:error, Error.invalid_request("replication JSON content type must be application/json")}

  defp match_decoded_size(json, expected) when byte_size(json) == expected, do: :ok
  defp match_decoded_size(_json, _expected), do: frame_mismatch()

  defp accept_shape(value, :map) when is_map(value), do: :ok
  defp accept_shape(value, :list) when is_list(value), do: :ok
  defp accept_shape(value, :map_or_list) when is_map(value) or is_list(value), do: :ok

  defp accept_shape(_value, :map),
    do: {:error, Error.invalid_request("replication JSON body must be an object")}

  defp accept_shape(_value, :list),
    do: {:error, Error.invalid_request("replication JSON body must be an array")}

  defp accept_shape(_value, :map_or_list),
    do: {:error, Error.invalid_request("replication JSON body must be an object or array")}

  defp reject_oversize(size, limit) when is_integer(size) and size <= limit, do: :ok

  defp reject_oversize(_size, _limit),
    do: {:error, Error.payload_too_large("replication JSON exceeds the configured limit")}

  defp default_max_depth do
    ElixirDB.Config.host_limits()[:max_json_nesting_depth] || 100
  end

  defp length_error,
    do: {:error, Error.invalid_request("replication JSON uncompressed length is invalid")}

  defp frame_mismatch,
    do: {:error, Error.invalid_request("replication JSON is not a valid Zstandard frame")}

  defp nif_error,
    do: {:error, Error.invalid_request("replication JSON is not a valid Zstandard frame")}
end
