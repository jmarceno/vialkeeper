defmodule ElixirDB.JSON.Stringify do
  @moduledoc "Converts nested Elixir maps to JSON-compatible string-keyed maps."

  @spec keys(term()) :: term()
  def keys(value) when is_map(value),
    do: Map.new(value, fn {key, child} -> {to_string(key), keys(child)} end)

  def keys(value) when is_list(value), do: Enum.map(value, &keys/1)
  def keys(value), do: value
end
