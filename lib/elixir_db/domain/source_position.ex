defmodule ElixirDB.Domain.SourcePosition do
  @moduledoc "Validated source position tuple for retention."

  @enforce_keys [:database_uuid, :history_epoch, :sequence]
  defstruct [:database_uuid, :history_epoch, :sequence]

  @type t :: %__MODULE__{
          database_uuid: binary(),
          history_epoch: binary(),
          sequence: non_neg_integer()
        }

  @known [:database_uuid, :history_epoch, :sequence]

  @spec new(map()) :: {:ok, t()} | {:error, ElixirDB.Error.t()}
  def new(attrs) when is_map(attrs) do
    if Enum.any?(Map.keys(attrs), &(&1 not in @known)),
      do: {:error, ElixirDB.Error.invalid_request("unknown source position field")},
      else: build(attrs)
  end

  def new(_), do: {:error, ElixirDB.Error.invalid_request("source position must be an object")}

  @spec from_wire(map()) :: {:ok, t()} | {:error, ElixirDB.Error.t()}
  def from_wire(attrs) when is_map(attrs) do
    allowed = ["database_uuid", "history_epoch", "sequence"]

    if Enum.any?(Map.keys(attrs), &(&1 not in allowed)) do
      {:error, ElixirDB.Error.invalid_request("unknown source position field")}
    else
      new(%{
        database_uuid: attrs["database_uuid"],
        history_epoch: attrs["history_epoch"],
        sequence: attrs["sequence"]
      })
    end
  end

  def from_wire(_),
    do: {:error, ElixirDB.Error.invalid_request("source position must be an object")}

  defp build(attrs) do
    case validation_error(attrs) do
      nil -> {:ok, struct(__MODULE__, attrs)}
      error -> {:error, error}
    end
  end

  defp validation_error(attrs) do
    validators = [
      &validate_database_uuid/1,
      &validate_history_epoch/1,
      &validate_sequence/1
    ]

    Enum.find_value(validators, & &1.(attrs))
  end

  defp validate_database_uuid(%{database_uuid: value})
       when is_binary(value) and value != "",
       do: nil

  defp validate_database_uuid(_),
    do: ElixirDB.Error.invalid_request("source position database_uuid is required")

  defp validate_history_epoch(%{history_epoch: value})
       when is_binary(value) and value != "",
       do: nil

  defp validate_history_epoch(_),
    do: ElixirDB.Error.invalid_request("source position history_epoch is required")

  defp validate_sequence(%{sequence: value}) when is_integer(value) and value >= 0, do: nil

  defp validate_sequence(_),
    do: ElixirDB.Error.invalid_request("source position sequence must be non-negative")
end
