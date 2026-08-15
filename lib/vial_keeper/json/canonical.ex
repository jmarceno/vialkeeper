defmodule VialKeeper.JSON.Canonical do
  @moduledoc "RFC 8785-style canonical JSON for validated JSON values."

  @spec encode(term()) :: {:ok, binary()} | {:error, VialKeeper.Error.t()}
  def encode(value) do
    {:ok, IO.iodata_to_binary(encode_value(value))}
  rescue
    ArgumentError -> {:error, VialKeeper.Error.invalid_request("value is not canonical JSON")}
    ArithmeticError -> {:error, VialKeeper.Error.invalid_request("value is not canonical JSON")}
    FunctionClauseError -> {:error, VialKeeper.Error.invalid_request("value is not canonical JSON")}
  end

  @spec encode!(term()) :: binary()
  def encode!(value) do
    case encode(value) do
      {:ok, result} -> result
      {:error, error} -> raise ArgumentError, error.message
    end
  end

  defp encode_value(nil), do: "null"
  defp encode_value(true), do: "true"
  defp encode_value(false), do: "false"

  defp encode_value(value) when is_integer(value) and abs(value) <= 9_007_199_254_740_991,
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
end
