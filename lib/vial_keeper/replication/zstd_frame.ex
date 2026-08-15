defmodule VialKeeper.Replication.ZstdFrame do
  @moduledoc """
  Pure structural validator for exactly one ordinary Zstandard frame.

  Parses RFC 8878 frame headers and block headers with bounded binary slicing.
  It does not call `:ezstd` and does not allocate from a declared content size.
  """

  import Bitwise

  alias VialKeeper.Error

  @magic <<0x28, 0xB5, 0x2F, 0xFD>>
  @skippable_magic_prefix <<0x2A, 0x4D, 0x18>>

  @type info :: %{content_size: non_neg_integer(), frame_size: non_neg_integer()}

  @spec validate(term(), keyword()) :: {:ok, info()} | {:error, Error.t()}
  def validate(binary, opts \\ [])

  def validate(binary, opts) when is_binary(binary) and is_list(opts) do
    expected = Keyword.get(opts, :expected_content_size)

    with {:ok, rest} <- require_ordinary_magic(binary),
         {:ok, header, rest} <- parse_frame_header(rest),
         :ok <- require_declared_size(header.content_size),
         :ok <- match_expected_size(header.content_size, expected),
         {:ok, rest} <- scan_blocks(rest),
         :ok <- consume_checksum(rest, header.content_checksum?) do
      {:ok, %{content_size: header.content_size, frame_size: byte_size(binary)}}
    end
  rescue
    _error in [ArgumentError, FunctionClauseError, MatchError, ArithmeticError] ->
      frame_error()
  end

  def validate(_, _), do: frame_error()

  defp require_ordinary_magic(<<@magic, rest::binary>>), do: {:ok, rest}

  defp require_ordinary_magic(<<nibble, @skippable_magic_prefix, _rest::binary>>)
       when nibble >= 0x50 and nibble <= 0x5F,
       do: frame_error()

  defp require_ordinary_magic(_), do: frame_error()

  defp parse_frame_header(<<descriptor, rest::binary>>) do
    fcs_flag = descriptor >>> 6
    single_segment? = band(descriptor, 0x20) != 0
    unused? = band(descriptor, 0x10) != 0
    reserved? = band(descriptor, 0x08) != 0
    content_checksum? = band(descriptor, 0x04) != 0
    dictionary_id_flag = band(descriptor, 0x03)

    with :ok <- reject_reserved(unused? or reserved?),
         {:ok, rest} <- maybe_take_window(rest, single_segment?),
         {:ok, rest} <- take_dictionary_id(rest, dictionary_id_flag),
         {:ok, content_size, rest} <- take_content_size(rest, fcs_flag, single_segment?) do
      {:ok, %{content_size: content_size, content_checksum?: content_checksum?}, rest}
    end
  end

  defp parse_frame_header(_), do: frame_error()

  defp reject_reserved(true), do: frame_error()
  defp reject_reserved(false), do: :ok

  defp maybe_take_window(rest, true), do: {:ok, rest}

  defp maybe_take_window(<<_window, rest::binary>>, false), do: {:ok, rest}

  defp maybe_take_window(_, false), do: frame_error()

  defp take_dictionary_id(rest, 0), do: {:ok, rest}
  defp take_dictionary_id(rest, 1), do: take_bytes(rest, 1)
  defp take_dictionary_id(rest, 2), do: take_bytes(rest, 2)
  defp take_dictionary_id(rest, 3), do: take_bytes(rest, 4)

  defp take_content_size(rest, 0, false), do: {:ok, :unknown, rest}
  defp take_content_size(rest, 0, true), do: take_uint(rest, 1)
  defp take_content_size(rest, 1, _single_segment?), do: take_fcs_plus_256(rest)
  defp take_content_size(rest, 2, _single_segment?), do: take_uint(rest, 4)
  defp take_content_size(rest, 3, _single_segment?), do: take_uint(rest, 8)

  defp take_fcs_plus_256(rest) do
    case take_uint(rest, 2) do
      {:ok, value, rest} -> {:ok, value + 256, rest}
      {:error, _} = error -> error
    end
  end

  defp require_declared_size(size) when is_integer(size) and size >= 0, do: :ok
  defp require_declared_size(_), do: frame_error()

  defp match_expected_size(_content_size, nil), do: :ok

  defp match_expected_size(content_size, expected)
       when is_integer(expected) and expected >= 0 and content_size == expected,
       do: :ok

  defp match_expected_size(_, _), do: frame_error()

  defp scan_blocks(rest) do
    case take_uint(rest, 3) do
      {:ok, header, rest} -> continue_blocks(rest, header)
      {:error, _} -> frame_error()
    end
  end

  defp continue_blocks(rest, header) do
    last_block? = band(header, 0x01) == 1
    block_type = band(header >>> 1, 0x03)
    block_size = header >>> 3

    case consume_block(rest, block_type, block_size) do
      {:ok, rest} when last_block? -> {:ok, rest}
      {:ok, rest} -> scan_blocks(rest)
      {:error, _} = error -> error
    end
  end

  defp consume_block(rest, 0, size), do: take_bytes(rest, size)
  defp consume_block(rest, 1, _repeat_count), do: take_bytes(rest, 1)
  defp consume_block(rest, 2, size), do: take_bytes(rest, size)
  defp consume_block(_rest, 3, _size), do: frame_error()

  defp consume_checksum(<<>>, false), do: :ok
  defp consume_checksum(_rest, false), do: frame_error()

  defp consume_checksum(rest, true) do
    case take_bytes(rest, 4) do
      {:ok, <<>>} -> :ok
      {:ok, _} -> frame_error()
      {:error, _} = error -> error
    end
  end

  defp take_bytes(rest, 0) when is_binary(rest), do: {:ok, rest}

  defp take_bytes(rest, size) when is_binary(rest) and is_integer(size) and size > 0 do
    rest_size = byte_size(rest)

    if size > rest_size do
      frame_error()
    else
      {:ok, binary_part(rest, size, rest_size - size)}
    end
  end

  defp take_bytes(_, _), do: frame_error()

  defp take_uint(binary, 1) when is_binary(binary) and byte_size(binary) >= 1 do
    <<value::unsigned-8, rest::binary>> = binary
    {:ok, value, rest}
  end

  defp take_uint(binary, 2) when is_binary(binary) and byte_size(binary) >= 2 do
    <<value::unsigned-little-16, rest::binary>> = binary
    {:ok, value, rest}
  end

  defp take_uint(binary, 3) when is_binary(binary) and byte_size(binary) >= 3 do
    <<value::unsigned-little-24, rest::binary>> = binary
    {:ok, value, rest}
  end

  defp take_uint(binary, 4) when is_binary(binary) and byte_size(binary) >= 4 do
    <<value::unsigned-little-32, rest::binary>> = binary
    {:ok, value, rest}
  end

  defp take_uint(binary, 8) when is_binary(binary) and byte_size(binary) >= 8 do
    <<value::unsigned-little-64, rest::binary>> = binary
    {:ok, value, rest}
  end

  defp take_uint(_, _), do: frame_error()

  defp frame_error do
    {:error, Error.invalid_request("replication JSON is not a valid Zstandard frame")}
  end
end
