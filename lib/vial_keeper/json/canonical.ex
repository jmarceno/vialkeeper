defmodule VialKeeper.JSON.Canonical do
  @moduledoc "RFC 8785-style canonical JSON for validated JSON values."
  alias VialKeeper.Error
  alias VialKeeper.JSON.StrictDecoder

  @safe_integer_max 9_007_199_254_740_991
  @default_max_depth 100

  @spec encode(term()) :: {:ok, binary()} | {:error, Error.t()}
  def encode(value) do
    {:ok, IO.iodata_to_binary(encode_value(value))}
  rescue
    ArgumentError -> {:error, Error.invalid_request("value is not canonical JSON")}
    ArithmeticError -> {:error, Error.invalid_request("value is not canonical JSON")}
    FunctionClauseError -> {:error, Error.invalid_request("value is not canonical JSON")}
  end

  @spec encode!(term()) :: binary()
  def encode!(value) do
    case encode(value) do
      {:ok, result} -> result
      {:error, error} -> raise ArgumentError, error.message
    end
  end

  @doc """
  Recovers the StrictDecoder term for `json` produced by `encode/1`.

  Elixir maps, lists, binaries, booleans, nil, and safe integers already match
  that term, so those values skip a second JSON parse. Floats and over-deep
  trees still round-trip through `StrictDecoder` so `1.0` becomes `1` and depth
  limits stay identical.
  """
  @spec decode_encoded(term(), binary(), keyword()) :: {:ok, term()} | {:error, Error.t()}
  def decode_encoded(value, json, opts \\ [])

  def decode_encoded(value, json, opts) when is_binary(json) and is_list(opts) do
    max_depth = Keyword.get(opts, :max_depth, @default_max_depth)

    case term_kind(value, 0, max_depth) do
      :canonical -> {:ok, value}
      :roundtrip -> StrictDecoder.decode(json, opts)
    end
  end

  def decode_encoded(_value, _json, _opts),
    do: {:error, Error.invalid_request("canonical JSON body must be UTF-8 text")}

  defp encode_value(nil), do: "null"
  defp encode_value(true), do: "true"
  defp encode_value(false), do: "false"

  defp encode_value(value) when is_integer(value) and abs(value) <= @safe_integer_max,
    do: Integer.to_string(value)

  defp encode_value(value) when is_float(value), do: encode_float(value)

  defp encode_value(value) when is_binary(value), do: JSON.encode_to_iodata!(value)

  defp encode_value(value) when is_list(value),
    do: [?[, Enum.map_intersperse(value, ?,, &encode_value/1), ?]]

  defp encode_value(value) when is_map(value) do
    [?{, encode_object_members(Map.to_list(value)), ?}]
  end

  defp encode_value(_), do: raise(ArgumentError)

  defp encode_object_members(pairs) do
    # RFC 8785 compares names as UTF-16 code units. ASCII names have the same
    # order as their UTF-8 bytes, so those objects skip the UTF-16 copies.
    if ascii_keys?(pairs) do
      pairs
      |> Enum.sort_by(&elem(&1, 0))
      |> Enum.map_intersperse(?,, &encode_member/1)
    else
      pairs
      |> Enum.map(&utf16_sort_pair/1)
      |> Enum.sort_by(&elem(&1, 0))
      |> Enum.map_intersperse(?,, fn {_sort_key, pair} -> encode_member(pair) end)
    end
  end

  defp encode_member({key, member}) when is_binary(key),
    do: [JSON.encode_to_iodata!(key), ?:, encode_value(member)]

  defp encode_member(_pair), do: raise(ArgumentError)

  defp utf16_sort_pair({key, member}) when is_binary(key),
    do: {utf16_key(key), {key, member}}

  defp utf16_sort_pair(_pair), do: raise(ArgumentError)

  defp ascii_keys?([]), do: true

  defp ascii_keys?([{key, _member} | rest]) when is_binary(key) do
    ascii_string?(key) and ascii_keys?(rest)
  end

  defp ascii_keys?(_pairs), do: raise(ArgumentError)

  defp ascii_string?(<<>>), do: true
  defp ascii_string?(<<byte, rest::binary>>) when byte <= 127, do: ascii_string?(rest)
  defp ascii_string?(_binary), do: false

  defp utf16_key(key), do: :unicode.characters_to_binary(key, :utf8, {:utf16, :big})

  defp encode_float(value) when is_float(value) do
    truncated = trunc(value)

    cond do
      value == 0.0 ->
        "0"

      value == truncated and abs(value) < 1.0e21 ->
        Integer.to_string(truncated)

      true ->
        value
        |> then(&:erlang.float_to_binary(&1, [:short]))
        |> normalize_float()
    end
  end

  defp normalize_float(value) do
    {mantissa, exponent} = split_exponent(value)
    sign = sign_for(mantissa)
    unsigned = String.trim_leading(mantissa, "-")
    {integer, fraction} = split_decimal(unsigned)
    digits = String.trim_trailing(integer <> fraction, "0")
    digits = if digits == "", do: "0", else: digits
    decimal_position = byte_size(integer) + exponent

    format_float_parts(sign, digits, decimal_position)
  end

  defp split_exponent(value) do
    case String.split(value, "e", parts: 2) do
      [mantissa] -> {mantissa, 0}
      [mantissa, exponent] -> {mantissa, String.to_integer(exponent)}
    end
  end

  defp sign_for(mantissa), do: if(String.starts_with?(mantissa, "-"), do: "-", else: "")

  defp split_decimal(unsigned) do
    case String.split(unsigned, ".", parts: 2) do
      [integer] -> {integer, ""}
      [integer, fraction] -> {integer, fraction}
    end
  end

  defp format_float_parts(sign, digits, decimal_position) do
    if decimal_position >= -5 and decimal_position < 22 do
      sign <> decimal_notation(digits, decimal_position)
    else
      sign <> scientific_notation(digits, decimal_position)
    end
  end

  defp decimal_notation(digits, decimal_position) do
    cond do
      decimal_position <= 0 ->
        "0." <> String.duplicate("0", -decimal_position) <> digits

      decimal_position >= byte_size(digits) ->
        digits <> String.duplicate("0", decimal_position - byte_size(digits))

      true ->
        binary_part(digits, 0, decimal_position) <>
          "." <> binary_part(digits, decimal_position, byte_size(digits) - decimal_position)
    end
  end

  defp scientific_notation(digits, decimal_position) do
    exponent_value = decimal_position - 1

    coefficient =
      binary_part(digits, 0, 1) <>
        if(byte_size(digits) > 1,
          do: "." <> binary_part(digits, 1, byte_size(digits) - 1),
          else: ""
        )

    exponent_sign = if exponent_value >= 0, do: "+", else: "-"
    coefficient <> "e" <> exponent_sign <> Integer.to_string(abs(exponent_value))
  end

  defp term_kind(_value, depth, max_depth) when depth > max_depth, do: :roundtrip
  defp term_kind(value, _depth, _max_depth) when is_nil(value) or is_boolean(value), do: :canonical

  defp term_kind(value, _depth, _max_depth)
       when is_integer(value) and abs(value) <= @safe_integer_max,
       do: :canonical

  defp term_kind(value, _depth, _max_depth) when is_float(value), do: :roundtrip

  defp term_kind(value, _depth, _max_depth) when is_binary(value) do
    if String.valid?(value), do: :canonical, else: :roundtrip
  end

  defp term_kind(value, depth, max_depth) when is_list(value),
    do: list_kind(value, depth, max_depth)

  defp term_kind(value, depth, max_depth) when is_map(value),
    do: map_kind(value, depth, max_depth)

  defp term_kind(_value, _depth, _max_depth), do: :roundtrip

  defp list_kind([], _depth, _max_depth), do: :canonical

  defp list_kind([head | tail], depth, max_depth) do
    case term_kind(head, depth + 1, max_depth) do
      :canonical -> list_kind(tail, depth, max_depth)
      kind -> kind
    end
  end

  defp map_kind(map, depth, max_depth) do
    Enum.reduce_while(map, :canonical, fn
      {key, value}, :canonical when is_binary(key) ->
        case term_kind(value, depth + 1, max_depth) do
          :canonical -> {:cont, :canonical}
          kind -> {:halt, kind}
        end

      _entry, _acc ->
        {:halt, :roundtrip}
    end)
  end
end
