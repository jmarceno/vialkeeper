defmodule VialKeeper.JSON.StrictDecoder.Legacy do
  @moduledoc "Compatibility JSON decoder for callers that provide explicit limits."

  alias VialKeeper.JSON.StrictDecoder

  @default_max_depth 100

  @spec decode(binary(), keyword()) :: {:ok, term()} | {:error, VialKeeper.Error.t()}
  def decode(input, opts \\ [])

  def decode(input, opts) when is_binary(input) do
    max_depth = Keyword.get(opts, :max_depth, configured_max_depth())
    max_bytes = Keyword.get(opts, :max_bytes, byte_size(input))

    cond do
      byte_size(input) > max_bytes ->
        {:error, VialKeeper.Error.payload_too_large("JSON body exceeds the configured limit")}

      not String.valid?(input) ->
        {:error, VialKeeper.Error.invalid_request("JSON must be valid UTF-8")}

      true ->
        with {:ok, value, rest} <- parse_value(skip_ws(input), 0, max_depth),
             <<>> <- skip_ws(rest) do
          {:ok, value}
        else
          {:error, %VialKeeper.Error{} = error} -> {:error, error}
          _ -> {:error, VialKeeper.Error.invalid_request("malformed JSON")}
        end
    end
  end

  def decode(_, _), do: {:error, VialKeeper.Error.invalid_request("JSON body must be UTF-8 text")}

  defp configured_max_depth do
    VialKeeper.Config.host_limits()[:max_json_nesting_depth] || @default_max_depth
  end

  defp parse_value(_input, depth, max_depth) when depth > max_depth,
    do: {:error, VialKeeper.Error.resource_limit("JSON nesting exceeds the configured limit")}

  defp parse_value(<<"null", rest::binary>>, _depth, _max), do: {:ok, nil, rest}
  defp parse_value(<<"true", rest::binary>>, _depth, _max), do: {:ok, true, rest}
  defp parse_value(<<"false", rest::binary>>, _depth, _max), do: {:ok, false, rest}

  defp parse_value(<<?{, rest::binary>>, depth, max),
    do: parse_object(skip_ws(rest), %{}, depth + 1, max)

  defp parse_value(<<?[, rest::binary>>, depth, max),
    do: parse_array(skip_ws(rest), [], depth + 1, max)

  defp parse_value(<<?\", rest::binary>>, _depth, _max) do
    with {:ok, raw, tail} <- consume_string(rest, []),
         {:ok, value} <- decode_string(raw) do
      validate_string_value(value, tail)
    else
      {:error, %VialKeeper.Error{} = error} -> {:error, error}
    end
  end

  defp parse_value(input, _depth, _max), do: parse_number(input)

  defp decode_string(raw) do
    input = IO.iodata_to_binary([?\", raw, ?\"])

    case RustyJson.decode(input,
           keys: :strings,
           floats: :decimals,
           duplicate_keys: :error,
           validate_strings: true,
           max_bytes: byte_size(input),
           decoding_integer_digit_limit: 16
         ) do
      {:ok, value} when is_binary(value) -> {:ok, value}
      {:ok, _value} -> {:error, VialKeeper.Error.invalid_request("invalid JSON string")}
      {:error, _error} -> {:error, VialKeeper.Error.invalid_request("invalid JSON string")}
    end
  rescue
    _error in [ArgumentError, ErlangError] ->
      {:error, VialKeeper.Error.invalid_request("invalid JSON string")}
  end

  defp validate_string_value(value, tail) when is_binary(value) do
    if String.valid?(value),
      do: {:ok, value, tail},
      else: {:error, VialKeeper.Error.invalid_request("invalid Unicode string")}
  end

  defp parse_object(<<"}", rest::binary>>, object, _depth, _max), do: {:ok, object, rest}

  defp parse_object(input, object, depth, max) do
    with <<?\", rest::binary>> <- input,
         {:ok, raw_key, after_key} <- consume_string(rest, []),
         {:ok, key} <- decode_string(raw_key),
         <<?:, after_colon::binary>> <- skip_ws(after_key),
         {:ok, value, after_value} <- parse_value(skip_ws(after_colon), depth, max) do
      put_object_value(object, key, value, after_value, depth, max)
    else
      {:error, %VialKeeper.Error{} = error} -> {:error, error}
      _ -> {:error, VialKeeper.Error.invalid_request("malformed JSON object")}
    end
  end

  defp put_object_value(object, key, _value, _after_value, _depth, _max)
       when is_map_key(object, key),
       do: {:error, VialKeeper.Error.invalid_request("duplicate JSON object key")}

  defp put_object_value(object, key, value, after_value, depth, max) do
    next = skip_ws(after_value)
    next_object = Map.put(object, key, value)

    case next do
      <<?}, tail::binary>> -> {:ok, next_object, tail}
      <<?,, tail::binary>> -> parse_object(skip_ws(tail), next_object, depth, max)
      _ -> {:error, VialKeeper.Error.invalid_request("malformed JSON object")}
    end
  end

  defp parse_array(<<"]", rest::binary>>, values, _depth, _max),
    do: {:ok, Enum.reverse(values), rest}

  defp parse_array(input, values, depth, max) do
    case parse_value(input, depth, max) do
      {:ok, value, rest} ->
        case skip_ws(rest) do
          <<?], tail::binary>> -> {:ok, Enum.reverse([value | values]), tail}
          <<?,, tail::binary>> -> parse_array(skip_ws(tail), [value | values], depth, max)
          _ -> {:error, VialKeeper.Error.invalid_request("malformed JSON array")}
        end

      {:error, %VialKeeper.Error{} = error} ->
        {:error, error}
    end
  end

  defp consume_string(<<>>, _acc),
    do: {:error, VialKeeper.Error.invalid_request("unterminated JSON string")}

  defp consume_string(<<?\", rest::binary>>, acc), do: {:ok, Enum.reverse(acc), rest}

  defp consume_string(<<?\\, next, rest::binary>>, acc) when next in ~c("\\\"/bfnrt") do
    consume_string(rest, [<<?\\, next>> | acc])
  end

  defp consume_string(<<?\\, ?u, a, b, c, d, rest::binary>>, acc) do
    if Enum.all?([a, b, c, d], &(&1 in ?0..?9 or &1 in ?a..?f or &1 in ?A..?F)),
      do: consume_string(rest, [<<?\\, ?u, a, b, c, d>> | acc]),
      else: {:error, VialKeeper.Error.invalid_request("invalid Unicode escape")}
  end

  defp consume_string(<<byte, _rest::binary>>, _acc) when byte < 0x20,
    do: {:error, VialKeeper.Error.invalid_request("unescaped control character in JSON string")}

  defp consume_string(<<byte, rest::binary>>, acc), do: consume_string(rest, [<<byte>> | acc])

  defp parse_number(input) do
    {token, rest} = take_number(input, [])

    case StrictDecoder.decode(token, max_depth: 0) do
      {:ok, value} when is_integer(value) or is_float(value) ->
        {:ok, value, rest}

      {:ok, _value} ->
        {:error, VialKeeper.Error.invalid_request("invalid JSON number")}

      {:error, %VialKeeper.Error{} = error} ->
        {:error, error}
    end
  end

  defp take_number(<<byte, rest::binary>>, acc) when byte in ?0..?9 or byte in ~c("-+.eE"),
    do: take_number(rest, [<<byte>> | acc])

  defp take_number(rest, acc), do: {acc |> Enum.reverse() |> IO.iodata_to_binary(), rest}

  defp skip_ws(<<byte, rest::binary>>) when byte in [32, 9, 10, 13], do: skip_ws(rest)
  defp skip_ws(rest), do: rest
end
