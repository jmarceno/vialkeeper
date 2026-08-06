defmodule ElixirDB.JSON.Canonical do
  @moduledoc "RFC 8785-style canonical JSON for validated JSON values."

  @spec encode(term()) :: {:ok, binary()} | {:error, ElixirDB.Error.t()}
  def encode(value) do
    try do
      {:ok, encode_value(value)}
    rescue
      ArgumentError -> {:error, ElixirDB.Error.invalid_request("value is not canonical JSON")}
      ArithmeticError -> {:error, ElixirDB.Error.invalid_request("value is not canonical JSON")}
      FunctionClauseError -> {:error, ElixirDB.Error.invalid_request("value is not canonical JSON")}
    end
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

  defp encode_value(value) when is_binary(value),
    do: JSON.encode_to_iodata!(value) |> IO.iodata_to_binary()

  defp encode_value(value) when is_list(value),
    do: ["[", Enum.intersperse(Enum.map(value, &encode_value/1), ","), "]"] |> IO.iodata_to_binary()

  defp encode_value(value) when is_map(value) do
    members =
      value
      |> Map.to_list()
      |> Enum.map(fn
        {key, member} when is_binary(key) -> {utf16_key(key), key, member}
        {_key, _member} -> raise ArgumentError
      end)
      |> Enum.sort_by(fn {sort_key, _key, _member} -> sort_key end)
      |> Enum.map(fn {_sort_key, key, member} ->
        [JSON.encode_to_iodata!(key), ":", encode_value(member)]
      end)

    ["{", Enum.intersperse(members, ","), "}"] |> IO.iodata_to_binary()
  end

  defp encode_value(_), do: raise(ArgumentError)

  defp utf16_key(key), do: :unicode.characters_to_binary(key, :utf8, {:utf16, :big})

  defp encode_float(value) when is_float(value) do
    cond do
      value == 0.0 ->
        "0"

      value == trunc(value) and abs(value) < 1.0e21 ->
        Integer.to_string(trunc(value))

      true ->
        value
        |> then(&:erlang.float_to_binary(&1, [:short]))
        |> normalize_float()
    end
  end

  defp normalize_float(value) do
    {mantissa, exponent} =
      case String.split(value, "e", parts: 2) do
        [mantissa] -> {mantissa, 0}
        [mantissa, exponent] -> {mantissa, String.to_integer(exponent)}
      end

    sign = if String.starts_with?(mantissa, "-"), do: "-", else: ""
    unsigned = String.trim_leading(mantissa, "-")

    {integer, fraction} =
      case String.split(unsigned, ".", parts: 2) do
        [integer] -> {integer, ""}
        [integer, fraction] -> {integer, fraction}
      end

    digits = String.trim_trailing(integer <> fraction, "0")
    digits = if digits == "", do: "0", else: digits
    decimal_position = byte_size(integer) + exponent

    if decimal_position >= -5 and decimal_position < 22 do
      decimal =
        cond do
          decimal_position <= 0 ->
            "0." <> String.duplicate("0", -decimal_position) <> digits

          decimal_position >= byte_size(digits) ->
            digits <> String.duplicate("0", decimal_position - byte_size(digits))

          true ->
            binary_part(digits, 0, decimal_position) <>
              "." <> binary_part(digits, decimal_position, byte_size(digits) - decimal_position)
        end

      sign <> decimal
    else
      exponent_value = decimal_position - 1

      coefficient =
        binary_part(digits, 0, 1) <>
          if(byte_size(digits) > 1,
            do: "." <> binary_part(digits, 1, byte_size(digits) - 1),
            else: ""
          )

      exponent_sign = if exponent_value >= 0, do: "+", else: "-"
      sign <> coefficient <> "e" <> exponent_sign <> Integer.to_string(abs(exponent_value))
    end
  end
end
