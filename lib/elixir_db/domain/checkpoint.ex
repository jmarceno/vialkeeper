defmodule ElixirDB.Domain.Checkpoint do
  @moduledoc "Validated replication checkpoint state."

  @enforce_keys [
    :version,
    :replication_id,
    :checkpoint_version,
    :session_id,
    :source_sequence,
    :history
  ]
  defstruct [
    :version,
    :replication_id,
    :checkpoint_version,
    :session_id,
    :source_sequence,
    :history
  ]

  @type t :: %__MODULE__{
          version: pos_integer(),
          replication_id: binary(),
          checkpoint_version: non_neg_integer(),
          session_id: binary(),
          source_sequence: non_neg_integer(),
          history: list()
        }

  @known [
    :version,
    :replication_id,
    :checkpoint_version,
    :session_id,
    :source_sequence,
    :history
  ]

  @spec new(map()) :: {:ok, t()} | {:error, ElixirDB.Error.t()}
  def new(attrs) when is_map(attrs) do
    if Enum.any?(Map.keys(attrs), &(&1 not in @known)),
      do: {:error, ElixirDB.Error.invalid_request("unknown checkpoint field")},
      else: build(attrs)
  end

  def new(_), do: {:error, ElixirDB.Error.invalid_request("checkpoint must be an object")}

  @spec from_wire(map()) :: {:ok, t()} | {:error, ElixirDB.Error.t()}
  def from_wire(attrs) when is_map(attrs) do
    allowed = [
      "version",
      "replication_id",
      "checkpoint_version",
      "session_id",
      "source_sequence",
      "history"
    ]

    if Enum.any?(Map.keys(attrs), &(&1 not in allowed)) do
      {:error, ElixirDB.Error.invalid_request("unknown checkpoint field")}
    else
      new(%{
        version: attrs["version"],
        replication_id: attrs["replication_id"],
        checkpoint_version: attrs["checkpoint_version"],
        session_id: attrs["session_id"],
        source_sequence: attrs["source_sequence"],
        history: attrs["history"]
      })
    end
  end

  def from_wire(_), do: {:error, ElixirDB.Error.invalid_request("checkpoint must be an object")}

  defp build(attrs) do
    case validation_error(attrs) do
      nil -> {:ok, struct(__MODULE__, attrs)}
      error -> {:error, error}
    end
  end

  defp validation_error(attrs) do
    validators = [
      &validate_version/1,
      &validate_replication_id/1,
      &validate_checkpoint_version/1,
      &validate_session_id/1,
      &validate_source_sequence/1,
      &validate_history/1
    ]

    Enum.find_value(validators, & &1.(attrs))
  end

  defp validate_version(%{version: 1}), do: nil
  defp validate_version(_), do: ElixirDB.Error.invalid_request("unsupported checkpoint version")

  defp validate_replication_id(%{replication_id: value})
       when is_binary(value) and value != "",
       do: nil

  defp validate_replication_id(_),
    do: ElixirDB.Error.invalid_request("checkpoint replication_id is required")

  defp validate_checkpoint_version(%{checkpoint_version: value})
       when is_integer(value) and value >= 0,
       do: nil

  defp validate_checkpoint_version(_),
    do: ElixirDB.Error.invalid_request("checkpoint_version must be non-negative")

  defp validate_session_id(%{session_id: value}) when is_binary(value) and value != "", do: nil

  defp validate_session_id(_),
    do: ElixirDB.Error.invalid_request("checkpoint session_id is required")

  defp validate_source_sequence(%{source_sequence: value})
       when is_integer(value) and value >= 0,
       do: nil

  defp validate_source_sequence(_),
    do: ElixirDB.Error.invalid_request("source_sequence must be non-negative")

  defp validate_history(%{history: value}) when is_list(value), do: nil

  defp validate_history(_),
    do: ElixirDB.Error.invalid_request("checkpoint history must be an array")
end
