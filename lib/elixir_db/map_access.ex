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
end
