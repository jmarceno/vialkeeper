defmodule ElixirDB.Attachments.Representation do
  @moduledoc """
  Canonical Version 1 attachment representation trailer and descriptor.

  A stored blob is `encoded payload || 92-byte trailer`. Encoding `0` is the
  original logical bytes; encoding `1` is one completed Zstandard frame from
  original-byte ingestion. This module is pure: it does not touch the
  filesystem or HTTP.
  """

  alias ElixirDB.Attachments.Manifest
  alias ElixirDB.Error

  @magic "ELXBLB01"
  @format_version 1
  @trailer_size 92
  @encoding_raw 0
  @encoding_zstd 1
  @descriptor_string_keys ~w(format_version encoding logical_digest payload_sha256 logical_length payload_length)

  @type encoding :: :raw | :zstd

  @type descriptor :: %{
          format_version: pos_integer(),
          encoding: encoding(),
          logical_digest: binary(),
          logical_length: non_neg_integer(),
          payload_length: non_neg_integer(),
          payload_sha256: binary()
        }

  @spec magic() :: binary()
  def magic, do: @magic

  @spec format_version() :: pos_integer()
  def format_version, do: @format_version

  @spec trailer_size() :: pos_integer()
  def trailer_size, do: @trailer_size

  @spec encode_trailer(descriptor()) :: {:ok, binary()} | {:error, Error.t()}
  def encode_trailer(descriptor) when is_map(descriptor) do
    with {:ok, descriptor} <- normalize_descriptor(descriptor),
         {:ok, logical_digest} <- decode_digest(descriptor.logical_digest),
         {:ok, payload_digest} <- decode_digest(descriptor.payload_sha256),
         {:ok, encoding} <- encoding_byte(descriptor.encoding) do
      trailer =
        <<
          @magic::binary,
          @format_version::unsigned-big-16,
          encoding::unsigned-8,
          0::unsigned-8,
          descriptor.logical_length::unsigned-big-64,
          descriptor.payload_length::unsigned-big-64,
          logical_digest::binary-size(32),
          payload_digest::binary-size(32)
        >>

      {:ok, trailer}
    end
  end

  def encode_trailer(_), do: {:error, Error.invalid_request("invalid attachment representation")}

  @spec parse_trailer(binary()) :: {:ok, descriptor()} | {:error, Error.t()}
  def parse_trailer(trailer) when is_binary(trailer) and byte_size(trailer) == @trailer_size do
    case trailer do
      <<
        @magic,
        version::unsigned-big-16,
        encoding::unsigned-8,
        flags::unsigned-8,
        logical_length::unsigned-big-64,
        payload_length::unsigned-big-64,
        logical_digest::binary-size(32),
        payload_digest::binary-size(32)
      >> ->
        with :ok <- validate_version(version),
             :ok <- validate_flags(flags),
             {:ok, encoding} <- encoding_atom(encoding) do
          descriptor(
            format_version: version,
            encoding: encoding,
            logical_digest: encode_digest(logical_digest),
            logical_length: logical_length,
            payload_length: payload_length,
            payload_sha256: encode_digest(payload_digest)
          )
        end

      _ ->
        {:error, Error.integrity_violation("attachment representation trailer is invalid")}
    end
  end

  def parse_trailer(_),
    do: {:error, Error.integrity_violation("attachment representation trailer is invalid")}

  @spec descriptor(map() | keyword()) :: {:ok, descriptor()} | {:error, Error.t()}
  def descriptor(attrs) when is_list(attrs) do
    if Keyword.keyword?(attrs) do
      descriptor(Map.new(attrs))
    else
      {:error, Error.invalid_request("invalid attachment representation")}
    end
  end

  def descriptor(attrs) when is_map(attrs) do
    with {:ok, descriptor} <- normalize_descriptor(attrs),
         :ok <- validate_raw_invariants(descriptor) do
      {:ok, descriptor}
    end
  end

  def descriptor(_), do: {:error, Error.invalid_request("invalid attachment representation")}

  @spec validate_file_size(non_neg_integer(), descriptor()) :: :ok | {:error, Error.t()}
  def validate_file_size(file_size, %{payload_length: payload_length})
      when is_integer(file_size) and file_size >= 0 do
    if file_size == payload_length + @trailer_size do
      :ok
    else
      {:error, Error.integrity_violation("attachment representation length mismatch")}
    end
  end

  def validate_file_size(_, _),
    do: {:error, Error.integrity_violation("attachment representation length mismatch")}

  @spec validate_route_digest(binary(), descriptor()) :: :ok | {:error, Error.t()}
  def validate_route_digest(digest, %{logical_digest: logical_digest}) do
    with {:ok, digest} <- Manifest.validate_digest(digest) do
      if digest == logical_digest do
        :ok
      else
        {:error, Error.invalid_request("attachment digest does not match representation")}
      end
    end
  end

  defp normalize_descriptor(attrs) do
    attrs = atomize_descriptor_keys(attrs)
    encoding = encoding_value(Map.get(attrs, :encoding))
    format_version = Map.get(attrs, :format_version) || @format_version

    with :ok <- validate_version(format_version),
         {:ok, encoding} <- require_encoding(encoding),
         {:ok, logical_digest} <- Manifest.validate_digest(Map.get(attrs, :logical_digest)),
         {:ok, payload_sha256} <- Manifest.validate_digest(Map.get(attrs, :payload_sha256)),
         {:ok, logical_length} <- validate_length(Map.get(attrs, :logical_length)),
         {:ok, payload_length} <- validate_length(Map.get(attrs, :payload_length)),
         :ok <- validate_zstd_payload_length(encoding, payload_length) do
      {:ok,
       %{
         format_version: @format_version,
         encoding: encoding,
         logical_digest: logical_digest,
         logical_length: logical_length,
         payload_length: payload_length,
         payload_sha256: payload_sha256
       }}
    end
  end

  defp atomize_descriptor_keys(attrs) do
    Map.new(attrs, fn
      {key, value} when key in @descriptor_string_keys -> {String.to_existing_atom(key), value}
      entry -> entry
    end)
  end

  defp validate_length(value) when is_integer(value) and value >= 0, do: {:ok, value}
  defp validate_length(_), do: {:error, Error.invalid_request("invalid attachment representation")}

  defp validate_raw_invariants(%{encoding: :raw} = descriptor) do
    if descriptor.payload_length == descriptor.logical_length and
         descriptor.payload_sha256 == descriptor.logical_digest do
      :ok
    else
      {:error, Error.integrity_violation("raw attachment representation invariants failed")}
    end
  end

  defp validate_raw_invariants(_descriptor), do: :ok

  defp validate_zstd_payload_length(:zstd, payload_length) when payload_length > 0, do: :ok

  defp validate_zstd_payload_length(:zstd, _),
    do: {:error, Error.integrity_violation("zstd attachment payload length must be positive")}

  defp validate_zstd_payload_length(:raw, _), do: :ok

  defp validate_version(@format_version), do: :ok

  defp validate_version(_),
    do: {:error, Error.unsupported_format("unsupported attachment representation version")}

  defp validate_flags(0), do: :ok

  defp validate_flags(_),
    do: {:error, Error.unsupported_format("unsupported attachment representation flags")}

  defp encoding_byte(:raw), do: {:ok, @encoding_raw}
  defp encoding_byte(:zstd), do: {:ok, @encoding_zstd}

  defp encoding_atom(@encoding_raw), do: {:ok, :raw}
  defp encoding_atom(@encoding_zstd), do: {:ok, :zstd}

  defp encoding_atom(_),
    do: {:error, Error.unsupported_format("unsupported attachment encoding")}

  defp require_encoding(:raw), do: {:ok, :raw}
  defp require_encoding(:zstd), do: {:ok, :zstd}

  defp require_encoding(_),
    do: {:error, Error.unsupported_format("unsupported attachment encoding")}

  defp encoding_value("raw"), do: :raw
  defp encoding_value("zstd"), do: :zstd
  defp encoding_value(other), do: other

  defp decode_digest(digest) do
    case Base.decode16(digest, case: :lower) do
      {:ok, bytes} when byte_size(bytes) == 32 -> {:ok, bytes}
      _ -> {:error, Error.invalid_request("attachment digest must be lowercase hex")}
    end
  end

  defp encode_digest(bytes) when is_binary(bytes) and byte_size(bytes) == 32 do
    Base.encode16(bytes, case: :lower)
  end
end
