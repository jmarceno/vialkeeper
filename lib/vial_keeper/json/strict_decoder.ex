defmodule VialKeeper.JSON.StrictDecoder do
  @moduledoc """
  Bounded JSON decoding with RustyJson doing the parse and Decimal preserving
  the binary64 number checks required by the storage and HTTP contracts.

  RustyJson has a fixed native nesting ceiling of 128. Configurations above
  that ceiling use the preserved legacy implementation so this optimization
  never narrows an explicitly configured JSON limit.
  """

  @default_max_depth 100
  @rusty_max_depth 128
  @safe_integer_max 9_007_199_254_740_991

  alias VialKeeper.JSON.StrictDecoder.Legacy

  @spec decode(binary(), keyword()) :: {:ok, term()} | {:error, VialKeeper.Error.t()}
  def decode(input, opts \\ [])

  def decode(input, opts) when is_binary(input) do
    max_depth = Keyword.get(opts, :max_depth, configured_max_depth())
    max_bytes = Keyword.get(opts, :max_bytes, byte_size(input))

    cond do
      not is_integer(max_bytes) or max_bytes < 0 ->
        {:error, VialKeeper.Error.invalid_request("JSON byte limit must be a non-negative integer")}

      byte_size(input) > max_bytes ->
        {:error, VialKeeper.Error.payload_too_large("JSON body exceeds the configured limit")}

      not String.valid?(input) ->
        {:error, VialKeeper.Error.invalid_request("JSON must be valid UTF-8")}

      not is_integer(max_depth) ->
        {:error, VialKeeper.Error.invalid_request("JSON depth limit must be an integer")}

      max_depth > @rusty_max_depth ->
        Legacy.decode(input, opts)

      true ->
        decode_with_rusty(input, max_depth, max_bytes)
    end
  end

  def decode(_, _), do: {:error, VialKeeper.Error.invalid_request("JSON body must be UTF-8 text")}

  @doc "Decodes JSON and returns nil for malformed or invalid input."
  @spec decode_or_nil(binary()) :: term() | nil
  def decode_or_nil(input) do
    case decode(input) do
      {:ok, value} -> value
      _ -> nil
    end
  end

  defp configured_max_depth do
    VialKeeper.Config.host_limits()[:max_json_nesting_depth] || @default_max_depth
  end

  defp decode_with_rusty(input, max_depth, max_bytes) do
    case RustyJson.decode(input,
           keys: :strings,
           floats: :decimals,
           duplicate_keys: :error,
           validate_strings: true,
           max_bytes: max_bytes,
           decoding_integer_digit_limit: 16
         ) do
      {:ok, value} -> normalize(value, 0, max_depth)
      {:error, error} -> {:error, rusty_error(error)}
    end
  rescue
    _error in [ArgumentError, ErlangError] ->
      {:error, VialKeeper.Error.invalid_request("malformed JSON")}
  end

  defp normalize(value, depth, max_depth) do
    with :ok <- validate_depth(depth, max_depth) do
      normalize_value(value, depth, max_depth)
    end
  end

  defp normalize_value(%Decimal{} = value, _depth, _max_depth),
    do: decimal_to_float(value)

  defp normalize_value(value, depth, max_depth) when is_map(value),
    do: normalize_map(value, depth, max_depth)

  defp normalize_value(value, depth, max_depth) when is_list(value),
    do: normalize_list(value, depth, max_depth)

  defp normalize_value(value, _depth, _max_depth) when is_nil(value) or is_boolean(value),
    do: {:ok, value}

  defp normalize_value(value, _depth, _max_depth) when is_binary(value) do
    if String.valid?(value), do: {:ok, value}, else: invalid_json("invalid Unicode string")
  end

  defp normalize_value(value, _depth, _max_depth) when is_integer(value) do
    if abs(value) <= @safe_integer_max,
      do: {:ok, value},
      else: invalid_json("integer is outside the binary64 safe range")
  end

  defp normalize_value(value, _depth, _max_depth) when is_float(value),
    do: validate_float(value)

  defp normalize_value(_value, _depth, _max_depth),
    do: invalid_json("JSON contains an unsupported value")

  defp normalize_map(map, depth, max_depth) do
    Enum.reduce_while(map, {:ok, %{}}, fn {key, value}, {:ok, acc} ->
      case normalize_map_entry(key, value, depth, max_depth) do
        {:ok, normalized} -> {:cont, {:ok, Map.put(acc, key, normalized)}}
        {:error, _} = error -> {:halt, error}
      end
    end)
  end

  defp normalize_map_entry(key, value, depth, max_depth) when is_binary(key),
    do: normalize(value, depth + 1, max_depth)

  defp normalize_map_entry(_key, _value, _depth, _max_depth),
    do: invalid_json("JSON object key is not a string")

  defp normalize_list(values, depth, max_depth),
    do: normalize_list(values, depth, max_depth, [])

  defp normalize_list([], _depth, _max_depth, acc), do: {:ok, :lists.reverse(acc)}

  defp normalize_list([value | rest], depth, max_depth, acc) do
    case normalize(value, depth + 1, max_depth) do
      {:ok, normalized} -> normalize_list(rest, depth, max_depth, [normalized | acc])
      {:error, _} = error -> error
    end
  end

  defp decimal_to_float(value) do
    {:ok, Decimal.to_float(value)}
  rescue
    error in Decimal.Error ->
      if String.contains?(Exception.message(error), "DBL_MIN"),
        do: invalid_json("number underflows to zero"),
        else: invalid_json("number overflows to infinity")
  end

  defp validate_float(value) do
    if finite_float?(value),
      do: {:ok, value},
      else: invalid_json("number is not finite")
  end

  defp finite_float?(value) do
    not (value !== value) and finite_float_magnitude?(abs(value))
  end

  defp finite_float_magnitude?(value) do
    maximum = Float.max_finite()
    value < maximum or value == maximum
  end

  defp validate_depth(depth, max_depth) when depth <= max_depth, do: :ok

  defp validate_depth(_depth, _max_depth),
    do: {:error, VialKeeper.Error.resource_limit("JSON nesting exceeds the configured limit")}

  defp rusty_error(%RustyJson.DecodeError{message: message}) do
    cond do
      String.contains?(message, "Nesting depth") ->
        VialKeeper.Error.resource_limit("JSON nesting exceeds the configured limit")

      String.contains?(message, "Duplicate key") ->
        VialKeeper.Error.invalid_request("duplicate JSON object key")

      String.contains?(message, "Invalid UTF-8") ->
        VialKeeper.Error.invalid_request("invalid Unicode string")

      String.contains?(message, "Invalid number") ->
        VialKeeper.Error.invalid_request("invalid JSON number")

      String.contains?(message, "Unexpected character") ->
        VialKeeper.Error.invalid_request("invalid JSON value")

      true ->
        VialKeeper.Error.invalid_request("malformed JSON")
    end
  end

  defp invalid_json(message), do: {:error, VialKeeper.Error.invalid_request(message)}
end
