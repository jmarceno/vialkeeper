defmodule ElixirDB.Domain.Document do
  @enforce_keys [:id, :revision, :deleted]
  defstruct [:id, :revision, :deleted, :body, :conflicts, :sequence]

  @type t :: %__MODULE__{
          id: binary(),
          revision: binary(),
          deleted: boolean(),
          body: map() | nil,
          conflicts: [binary()] | nil,
          sequence: non_neg_integer() | nil
        }

  def new(attrs) when is_map(attrs) do
    keys = Map.keys(attrs)

    if Enum.any?(keys, &(&1 not in [:id, :revision, :deleted, :body, :conflicts, :sequence])),
      do: {:error, ElixirDB.Error.invalid_request("unknown document field")},
      else: build(attrs)
  end

  defp build(attrs) do
    cond do
      not is_binary(attrs[:id]) or attrs[:id] == "" ->
        {:error, ElixirDB.Error.invalid_request("document id is required")}

      not is_binary(attrs[:revision]) ->
        {:error, ElixirDB.Error.invalid_request("document revision is required")}

      not is_boolean(attrs[:deleted]) ->
        {:error, ElixirDB.Error.invalid_request("document deleted must be boolean")}

      not attrs[:deleted] and not is_map(attrs[:body]) ->
        {:error, ElixirDB.Error.invalid_request("live document body must be an object")}

      attrs[:deleted] and not is_nil(attrs[:body]) ->
        {:error, ElixirDB.Error.invalid_request("deleted document body must be null")}

      true ->
        {:ok, struct(__MODULE__, attrs)}
    end
  end
end
