defmodule ElixirDB.Domain.Document do
  @moduledoc "Validated document state and revision metadata."

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
    case validation_error(attrs) do
      nil -> {:ok, struct(__MODULE__, attrs)}
      error -> {:error, error}
    end
  end

  defp validation_error(attrs) do
    validators = [
      &validate_id/1,
      &validate_revision/1,
      &validate_deleted/1,
      &validate_live_body/1,
      &validate_deleted_body/1
    ]

    Enum.find_value(validators, & &1.(attrs))
  end

  defp validate_id(%{id: value}) when is_binary(value) and value != "", do: nil
  defp validate_id(_), do: ElixirDB.Error.invalid_request("document id is required")

  defp validate_revision(%{revision: value}) when is_binary(value), do: nil
  defp validate_revision(_), do: ElixirDB.Error.invalid_request("document revision is required")

  defp validate_deleted(%{deleted: value}) when is_boolean(value), do: nil
  defp validate_deleted(_), do: ElixirDB.Error.invalid_request("document deleted must be boolean")

  defp validate_live_body(%{deleted: false, body: body}) when is_map(body), do: nil
  defp validate_live_body(%{deleted: true}), do: nil

  defp validate_live_body(_),
    do: ElixirDB.Error.invalid_request("live document body must be an object")

  defp validate_deleted_body(%{deleted: true, body: nil}), do: nil
  defp validate_deleted_body(%{deleted: false}), do: nil

  defp validate_deleted_body(_),
    do: ElixirDB.Error.invalid_request("deleted document body must be null")
end
