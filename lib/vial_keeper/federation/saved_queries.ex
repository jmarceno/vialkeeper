defmodule VialKeeper.Federation.SavedQueries do
  @moduledoc "Read-only access to normalized host-configured federation queries."

  alias VialKeeper.MapAccess

  @doc "Returns the normalized saved federation query definitions configured by the host."
  @spec list() :: [map()]
  def list do
    case Application.get_env(:vial_keeper, :federation_saved_queries, []) do
      values when is_list(values) -> Enum.filter(values, &is_map/1)
      _ -> []
    end
  end

  @doc "Finds a normalized saved federation query by its configured name."
  @spec get(binary()) :: map() | nil
  def get(name) when is_binary(name),
    do: Enum.find(list(), &(MapAccess.get(&1, :name) == name))
end
