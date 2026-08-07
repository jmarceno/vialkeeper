defmodule ElixirDB.Domain.Change do
  @moduledoc "Validated change-feed entries and their revision leaves."

  @enforce_keys [:sequence, :document_id, :winning_revision, :deleted, :leaf_revisions]
  alias ElixirDB.Domain.Leaf
  defstruct [:sequence, :document_id, :winning_revision, :deleted, :leaf_revisions, :origin]

  @type t :: %__MODULE__{
          sequence: pos_integer(),
          document_id: binary(),
          winning_revision: binary(),
          deleted: boolean(),
          leaf_revisions: [Leaf.t()],
          origin: binary() | nil
        }

  @known [:sequence, :document_id, :winning_revision, :deleted, :leaf_revisions, :origin]

  @spec new(map()) :: {:ok, t()} | {:error, ElixirDB.Error.t()}
  def new(attrs) when is_map(attrs) do
    if Enum.any?(Map.keys(attrs), &(&1 not in @known)),
      do: {:error, ElixirDB.Error.invalid_request("unknown change field")},
      else: build(attrs)
  end

  def new(_), do: {:error, ElixirDB.Error.invalid_request("change must be an object")}

  defp build(attrs) do
    with :ok <- require_positive(attrs[:sequence], "change sequence"),
         :ok <- require_binary(attrs[:document_id], "change document_id"),
         :ok <- require_binary(attrs[:winning_revision], "change winning_revision"),
         :ok <- require_boolean(attrs[:deleted], "change deleted"),
         {:ok, leaves} <- normalize_leaves(attrs[:leaf_revisions]),
         :ok <- optional_binary(attrs[:origin], "change origin") do
      {:ok,
       struct(__MODULE__, %{
         sequence: attrs[:sequence],
         document_id: attrs[:document_id],
         winning_revision: attrs[:winning_revision],
         deleted: attrs[:deleted],
         leaf_revisions: leaves,
         origin: attrs[:origin]
       })}
    end
  end

  defp normalize_leaves(leaves) when is_list(leaves) do
    Enum.reduce_while(leaves, {:ok, []}, fn leaf, {:ok, acc} ->
      result = normalize_leaf(leaf)

      case result do
        {:ok, value} -> {:cont, {:ok, [value | acc]}}
        {:error, _} = error -> {:halt, error}
      end
    end)
    |> case do
      {:ok, values} -> {:ok, Enum.reverse(values)}
      error -> error
    end
  end

  defp normalize_leaves(_),
    do: {:error, ElixirDB.Error.invalid_request("leaf_revisions must be an array")}

  defp normalize_leaf(%Leaf{} = value), do: {:ok, value}

  defp normalize_leaf(%{} = value) do
    if Enum.any?(Map.keys(value), &is_atom/1), do: Leaf.new(value), else: Leaf.from_wire(value)
  end

  defp normalize_leaf(_),
    do: {:error, ElixirDB.Error.invalid_request("leaf_revisions entries must be objects")}

  defp require_positive(value, _label) when is_integer(value) and value > 0, do: :ok

  defp require_positive(_, label),
    do: {:error, ElixirDB.Error.invalid_request("#{label} must be a positive integer")}

  defp require_binary(value, _label) when is_binary(value) and value != "", do: :ok

  defp require_binary(_, label),
    do: {:error, ElixirDB.Error.invalid_request("#{label} is required")}

  defp require_boolean(value, _label) when is_boolean(value), do: :ok

  defp require_boolean(_, label),
    do: {:error, ElixirDB.Error.invalid_request("#{label} must be boolean")}

  defp optional_binary(nil, _label), do: :ok
  defp optional_binary(value, _label) when is_binary(value), do: :ok

  defp optional_binary(_, label),
    do: {:error, ElixirDB.Error.invalid_request("#{label} must be a string")}
end
