defmodule ElixirDB.Query.Prepared do
  @moduledoc "Opaque wrapper for a normalized query request held by query callers."

  @enforce_keys [:data]
  defstruct [:data]

  @type t :: %__MODULE__{data: map()}

  @spec wrap(map()) :: t()
  def wrap(data) when is_map(data) do
    %__MODULE__{data: Map.delete(data, :normalized)}
  end

  @spec unwrap(t()) :: map()
  def unwrap(%__MODULE__{data: data}) when is_map(data), do: data
end
