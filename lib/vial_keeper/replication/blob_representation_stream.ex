defmodule VialKeeper.Replication.BlobRepresentationStream do
  @moduledoc """
  Replication attachment stream of a stored encoded payload.

  `body` is a lazy enumerable of exact encoded payload chunks. The constructor
  validates descriptor fields; it cannot validate `body` until the enumerable
  is consumed.
  """

  alias VialKeeper.Attachments.Representation
  alias VialKeeper.Error
  alias VialKeeper.Headers

  @media_type "application/vnd.vialkeeper.blob-representation"
  @format_version_header "x-vialkeeper-blob-format-version"
  @encoding_header "x-vialkeeper-blob-encoding"
  @logical_length_header "x-vialkeeper-blob-logical-length"
  @payload_sha256_header "x-vialkeeper-blob-payload-sha256"

  @enforce_keys [
    :logical_digest,
    :logical_length,
    :format_version,
    :encoding,
    :payload_length,
    :payload_sha256,
    :body
  ]
  defstruct [
    :logical_digest,
    :logical_length,
    :format_version,
    :encoding,
    :payload_length,
    :payload_sha256,
    :body
  ]

  @type t :: %__MODULE__{
          logical_digest: binary(),
          logical_length: non_neg_integer(),
          format_version: pos_integer(),
          encoding: Representation.encoding(),
          payload_length: non_neg_integer(),
          payload_sha256: binary(),
          body: Enumerable.t()
        }

  @spec media_type() :: binary()
  def media_type, do: @media_type

  @spec new(map()) :: {:ok, t()} | {:error, Error.t()}
  def new(attrs) when is_map(attrs) do
    body = Map.get(attrs, :body)

    with {:ok, descriptor} <- Representation.descriptor(attrs),
         :ok <- require_body(body) do
      {:ok,
       %__MODULE__{
         logical_digest: descriptor.logical_digest,
         logical_length: descriptor.logical_length,
         format_version: descriptor.format_version,
         encoding: descriptor.encoding,
         payload_length: descriptor.payload_length,
         payload_sha256: descriptor.payload_sha256,
         body: body
       }}
    end
  end

  def new(_), do: {:error, Error.invalid_request("invalid blob representation stream")}

  @doc "Builds a validated stream or raises for an internal stream contract violation."
  @spec new!(map()) :: t()
  def new!(attrs) do
    case new(attrs) do
      {:ok, stream} -> stream
      {:error, error} -> raise ArgumentError, error.message
    end
  end

  @spec descriptor(t()) :: Representation.descriptor()
  def descriptor(%__MODULE__{} = stream) do
    stream
    |> Map.from_struct()
    |> Map.delete(:body)
  end

  @spec response_headers(t()) :: [{binary(), binary()}]
  def response_headers(%__MODULE__{} = stream) do
    [
      {"content-type", @media_type},
      {"content-length", Integer.to_string(stream.payload_length)},
      {@format_version_header, Integer.to_string(stream.format_version)},
      {@encoding_header, encoding_header(stream.encoding)},
      {@logical_length_header, Integer.to_string(stream.logical_length)},
      {@payload_sha256_header, stream.payload_sha256},
      {"x-vialkeeper-blob-logical-digest", stream.logical_digest}
    ]
  end

  @spec parse_http_headers(term(), binary()) ::
          {:ok, Representation.descriptor()} | {:error, Error.t()}
  def parse_http_headers(headers, logical_digest) do
    digest = logical_digest || Headers.get(headers, "x-vialkeeper-blob-logical-digest")

    with :ok <- reject_content_encoding(headers),
         :ok <- require_media_type(headers),
         {:ok, format_version} <-
           parse_canonical_positive_integer(Headers.get(headers, @format_version_header), :version),
         {:ok, encoding} <- parse_encoding(Headers.get(headers, @encoding_header)),
         {:ok, logical_length} <-
           parse_canonical_non_neg_integer(
             Headers.get(headers, @logical_length_header),
             :logical_length
           ),
         {:ok, payload_length} <- parse_content_length(headers),
         payload_sha256 <- Headers.get(headers, @payload_sha256_header),
         {:ok, descriptor} <-
           Representation.descriptor(
             format_version: format_version,
             encoding: encoding,
             logical_digest: digest,
             logical_length: logical_length,
             payload_length: payload_length,
             payload_sha256: payload_sha256
           ),
         :ok <- Representation.validate_route_digest(digest, descriptor) do
      {:ok, descriptor}
    end
  end

  defp require_body(body) when not is_nil(body), do: :ok
  defp require_body(_), do: {:error, Error.invalid_request("invalid blob representation stream")}

  defp encoding_header(:raw), do: "raw"
  defp encoding_header(:zstd), do: "zstd"

  defp parse_encoding(nil),
    do: {:error, Error.invalid_request("malformed attachment representation header")}

  defp parse_encoding("raw"), do: {:ok, :raw}
  defp parse_encoding("zstd"), do: {:ok, :zstd}

  defp parse_encoding(_),
    do: {:error, Error.unsupported_format("unsupported attachment encoding")}

  defp require_media_type(headers) do
    content_type = Headers.get(headers, "content-type")

    cond do
      is_nil(content_type) ->
        {:error, Error.invalid_request("replication blob content type must be #{@media_type}")}

      String.starts_with?(content_type, @media_type) ->
        :ok

      true ->
        {:error, Error.invalid_request("replication blob content type must be #{@media_type}")}
    end
  end

  defp reject_content_encoding(headers) do
    case Headers.get(headers, "content-encoding") do
      nil ->
        :ok

      _ ->
        {:error, Error.invalid_request("replication blob must not set content-encoding")}
    end
  end

  defp parse_content_length(headers) do
    parse_canonical_non_neg_integer(Headers.get(headers, "content-length"), :content_length)
  end

  defp parse_canonical_positive_integer(value, :version) do
    case parse_canonical_non_neg_integer(value, :version) do
      {:ok, 1} ->
        {:ok, 1}

      {:ok, _} ->
        {:error, Error.unsupported_format("unsupported attachment representation version")}

      {:error, _} = error ->
        error
    end
  end

  defp parse_canonical_non_neg_integer(value, field) when is_binary(value) do
    case Integer.parse(value) do
      {int, ""} when int >= 0 ->
        if Integer.to_string(int) == value do
          {:ok, int}
        else
          header_error(field)
        end

      _ ->
        header_error(field)
    end
  end

  defp parse_canonical_non_neg_integer(_, field), do: header_error(field)

  defp header_error(:version),
    do: {:error, Error.invalid_request("malformed attachment representation header")}

  defp header_error(:content_length),
    do: {:error, Error.invalid_request("content-length must be a non-negative integer")}

  defp header_error(_field),
    do: {:error, Error.invalid_request("malformed attachment representation header")}
end
