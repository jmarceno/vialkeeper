defmodule ElixirDB.Domain.Leaf do
  @enforce_keys [:revision, :deleted]
  defstruct [:revision, :deleted]
  @type t :: %__MODULE__{revision: binary(), deleted: boolean()}

  @known [:revision, :deleted]

  @spec new(map()) :: {:ok, t()} | {:error, ElixirDB.Error.t()}
  def new(attrs) when is_map(attrs) do
    if Enum.any?(Map.keys(attrs), &(&1 not in @known)),
      do: {:error, ElixirDB.Error.invalid_request("unknown leaf field")},
      else: build(attrs)
  end

  def new(_), do: {:error, ElixirDB.Error.invalid_request("leaf must be an object")}

  @spec from_wire(map()) :: {:ok, t()} | {:error, ElixirDB.Error.t()}
  def from_wire(attrs) when is_map(attrs) do
    allowed = ["revision", "deleted"]

    if Enum.any?(Map.keys(attrs), &(&1 not in allowed)),
      do: {:error, ElixirDB.Error.invalid_request("unknown leaf field")},
      else:
        new(%{
          revision: attrs["revision"],
          deleted: attrs["deleted"]
        })
  end

  def from_wire(_), do: {:error, ElixirDB.Error.invalid_request("leaf must be an object")}

  defp build(attrs) do
    cond do
      not is_binary(attrs[:revision]) or attrs[:revision] == "" ->
        {:error, ElixirDB.Error.invalid_request("leaf revision is required")}

      not is_boolean(attrs[:deleted]) ->
        {:error, ElixirDB.Error.invalid_request("leaf deleted must be boolean")}

      true ->
        {:ok, struct(__MODULE__, attrs)}
    end
  end
end
