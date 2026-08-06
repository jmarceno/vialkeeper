defmodule ElixirDB.JSON.Pointer do
  @moduledoc "RFC 6901 JSON Pointer parsing and lookup."

  @spec parse(binary()) :: {:ok, [binary()]} | {:error, ElixirDB.Error.t()}
  def parse(""), do: {:ok, []}

  def parse(<<?/, rest::binary>>) do
    tokens = String.split(rest, "/", trim: false)

    if Enum.any?(tokens, &Regex.match?(~r/ ~(?![01]) /x, &1)),
      do: {:error, ElixirDB.Error.invalid_request("invalid JSON Pointer escape")},
      else: {:ok, Enum.map(tokens, &decode_token/1)}
  rescue
    _ -> {:error, ElixirDB.Error.invalid_request("invalid JSON Pointer")}
  end

  def parse(_), do: {:error, ElixirDB.Error.invalid_request("JSON Pointer must begin with /")}

  @spec get(map() | list(), binary()) :: {:ok, term()} | :missing | {:error, ElixirDB.Error.t()}
  def get(value, pointer) do
    with {:ok, tokens} <- parse(pointer), false <- tokens == [] do
      descend(value, tokens)
    else
      true -> {:ok, value}
      {:error, _} = error -> error
    end
  end

  defp descend(value, []), do: {:ok, value}

  defp descend(value, [token | rest]) when is_map(value) do
    case Map.fetch(value, token) do
      {:ok, next} -> descend(next, rest)
      :error -> :missing
    end
  end

  defp descend(value, [token | rest]) when is_list(value) do
    case Integer.parse(token) do
      {index, ""} when index >= 0 ->
        if index < length(value), do: descend(Enum.at(value, index), rest), else: :missing

      _ ->
        :missing
    end
  end

  defp descend(_value, _tokens), do: :missing

  defp decode_token(token), do: String.replace(String.replace(token, "~1", "/"), "~0", "~")
end
