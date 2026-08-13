defmodule ElixirDB.MapAccess do
  @moduledoc "Consistent access to maps that may contain atom or wire-format keys."

  def get(map, key, default \\ nil)

  @spec get(map(), atom(), term()) :: term()
  def get(map, key, default) when is_map(map) and is_atom(key) do
    case Map.fetch(map, key) do
      {:ok, value} -> value
      :error -> Map.get(map, Atom.to_string(key), default)
    end
  end

  @spec get(term(), atom(), term()) :: term()
  def get(_map, _key, default), do: default

  @doc "Normalizes atom keys to wire strings and rejects atom/string collisions."
  @spec string_keys(map()) :: {:ok, map()} | :key_collision
  def string_keys(map) when is_map(map) do
    Enum.reduce_while(map, {:ok, %{}}, fn {key, value}, {:ok, normalized} ->
      key = if is_atom(key), do: Atom.to_string(key), else: key

      if Map.has_key?(normalized, key) do
        {:halt, :key_collision}
      else
        {:cont, {:ok, Map.put(normalized, key, value)}}
      end
    end)
  end

  @doc "Returns the first present value for the supplied atom/wire-key alternatives."
  @spec get_first(map(), [atom()]) :: term()
  def get_first(map, [key | keys]) when is_map(map) and is_atom(key) do
    case Map.fetch(map, key) do
      {:ok, value} ->
        value

      :error ->
        case Map.fetch(map, Atom.to_string(key)) do
          {:ok, value} -> value
          :error -> get_first(map, keys)
        end
    end
  end

  def get_first(map, [key | keys]) when is_map(map) and is_binary(key) do
    case Map.fetch(map, key) do
      {:ok, value} -> value
      :error -> get_first(map, keys)
    end
  end

  def get_first(_map, []), do: nil
  def get_first(_map, _keys), do: nil
end
