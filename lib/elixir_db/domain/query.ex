defmodule ElixirDB.Domain.Query do
  @enforce_keys [:selector]
  defstruct [:selector, :sort, :fields, :limit, :bookmark, :index, :search]

  @type t :: %__MODULE__{
          selector: map(),
          sort: list(),
          fields: list() | nil,
          limit: pos_integer() | nil,
          bookmark: binary() | nil,
          index: binary() | nil,
          search: map() | nil
        }

  @known [:selector, :sort, :fields, :limit, :bookmark, :index, :search]

  @spec new(map()) :: {:ok, t()} | {:error, ElixirDB.Error.t()}
  def new(attrs) when is_map(attrs) do
    if Enum.any?(Map.keys(attrs), &(&1 not in @known)),
      do: {:error, ElixirDB.Error.invalid_request("unknown query field")},
      else: build(attrs)
  end

  def new(_), do: {:error, ElixirDB.Error.invalid_request("query must be an object")}

  defp build(attrs) do
    cond do
      not is_map(attrs[:selector]) ->
        {:error, ElixirDB.Error.invalid_request("query selector must be an object")}

      not is_nil(attrs[:sort]) and not is_list(attrs[:sort]) ->
        {:error, ElixirDB.Error.invalid_request("query sort must be an array")}

      not is_nil(attrs[:fields]) and not is_list(attrs[:fields]) ->
        {:error, ElixirDB.Error.invalid_request("query fields must be an array")}

      not is_nil(attrs[:limit]) and not (is_integer(attrs[:limit]) and attrs[:limit] > 0) ->
        {:error, ElixirDB.Error.invalid_request("query limit must be a positive integer")}

      not is_nil(attrs[:bookmark]) and not is_binary(attrs[:bookmark]) ->
        {:error, ElixirDB.Error.invalid_request("query bookmark must be a string")}

      not is_nil(attrs[:index]) and not is_binary(attrs[:index]) ->
        {:error, ElixirDB.Error.invalid_request("query index must be a string")}

      not is_nil(attrs[:search]) and not is_map(attrs[:search]) ->
        {:error, ElixirDB.Error.invalid_request("query search must be an object")}

      true ->
        {:ok,
         struct(__MODULE__, %{
           selector: attrs[:selector],
           sort: attrs[:sort],
           fields: attrs[:fields],
           limit: attrs[:limit],
           bookmark: attrs[:bookmark],
           index: attrs[:index],
           search: attrs[:search]
         })}
    end
  end
end
