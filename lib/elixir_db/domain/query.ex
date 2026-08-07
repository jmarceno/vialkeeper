defmodule ElixirDB.Domain.Query do
  @moduledoc "Validated query request state."

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
    case validation_error(attrs) do
      nil -> {:ok, struct(__MODULE__, attrs)}
      error -> {:error, error}
    end
  end

  defp validation_error(attrs) do
    validators = [
      &validate_selector/1,
      &validate_sort/1,
      &validate_fields/1,
      &validate_limit/1,
      &validate_bookmark/1,
      &validate_index/1,
      &validate_search/1
    ]

    Enum.find_value(validators, & &1.(attrs))
  end

  defp validate_selector(%{selector: value}) when is_map(value), do: nil
  defp validate_selector(_), do: ElixirDB.Error.invalid_request("query selector must be an object")

  defp validate_sort(%{sort: value}) when is_nil(value) or is_list(value), do: nil
  defp validate_sort(_), do: ElixirDB.Error.invalid_request("query sort must be an array")

  defp validate_fields(%{fields: value}) when is_nil(value) or is_list(value), do: nil
  defp validate_fields(_), do: ElixirDB.Error.invalid_request("query fields must be an array")

  defp validate_limit(%{limit: value}) when is_nil(value) or (is_integer(value) and value > 0),
    do: nil

  defp validate_limit(_),
    do: ElixirDB.Error.invalid_request("query limit must be a positive integer")

  defp validate_bookmark(%{bookmark: value}) when is_nil(value) or is_binary(value), do: nil

  defp validate_bookmark(_),
    do: ElixirDB.Error.invalid_request("query bookmark must be a string")

  defp validate_index(%{index: value}) when is_nil(value) or is_binary(value), do: nil
  defp validate_index(_), do: ElixirDB.Error.invalid_request("query index must be a string")

  defp validate_search(%{search: value}) when is_nil(value) or is_map(value), do: nil
  defp validate_search(_), do: ElixirDB.Error.invalid_request("query search must be an object")
end
