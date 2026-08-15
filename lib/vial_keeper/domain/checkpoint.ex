defmodule VialKeeper.Domain.Checkpoint do
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
    :history,
    :source_history_epoch,
    :source_compaction_epoch,
    :safe_source_sequence,
    :installed_source_compaction_epoch
  ]

  @type t :: %__MODULE__{
          version: pos_integer(),
          replication_id: binary(),
          checkpoint_version: non_neg_integer(),
          session_id: binary(),
          source_sequence: non_neg_integer(),
          history: list(),
          source_history_epoch: binary(),
          source_compaction_epoch: non_neg_integer(),
          safe_source_sequence: non_neg_integer(),
          installed_source_compaction_epoch: non_neg_integer()
        }

  @known [
    :version,
    :replication_id,
    :checkpoint_version,
    :session_id,
    :source_sequence,
    :history,
    :source_history_epoch,
    :source_compaction_epoch,
    :safe_source_sequence,
    :installed_source_compaction_epoch
  ]

  @spec new(map()) :: {:ok, t()} | {:error, VialKeeper.Error.t()}
  def new(attrs) when is_map(attrs) do
    if Enum.any?(Map.keys(attrs), &(&1 not in @known)),
      do: {:error, VialKeeper.Error.invalid_request("unknown checkpoint field")},
      else: build(attrs)
  end

  def new(_), do: {:error, VialKeeper.Error.invalid_request("checkpoint must be an object")}

  @spec from_wire(map()) :: {:ok, t()} | {:error, VialKeeper.Error.t()}
  def from_wire(attrs) when is_map(attrs) do
    allowed = [
      "version",
      "replication_id",
      "checkpoint_version",
      "session_id",
      "source_sequence",
      "history",
      "source_history_epoch",
      "source_compaction_epoch",
      "safe_source_sequence",
      "installed_source_compaction_epoch"
    ]

    if Enum.any?(Map.keys(attrs), &(&1 not in allowed)) do
      {:error, VialKeeper.Error.invalid_request("unknown checkpoint field")}
    else
      new(%{
        version: attrs["version"],
        replication_id: attrs["replication_id"],
        checkpoint_version: attrs["checkpoint_version"],
        session_id: attrs["session_id"],
        source_sequence: attrs["source_sequence"],
        history: attrs["history"],
        source_history_epoch: attrs["source_history_epoch"],
        source_compaction_epoch: attrs["source_compaction_epoch"],
        safe_source_sequence: attrs["safe_source_sequence"],
        installed_source_compaction_epoch: attrs["installed_source_compaction_epoch"]
      })
    end
  end

  def from_wire(_), do: {:error, VialKeeper.Error.invalid_request("checkpoint must be an object")}

  @wire_fields [
    "version",
    "replication_id",
    "checkpoint_version",
    "session_id",
    "source_sequence",
    "history",
    "source_history_epoch",
    "source_compaction_epoch",
    "safe_source_sequence",
    "installed_source_compaction_epoch"
  ]

  @spec valid_wire_put?(map()) :: boolean()
  def valid_wire_put?(body) when is_map(body) do
    with true <-
           is_integer(body["expected_checkpoint_version"]) and
             body["expected_checkpoint_version"] >= 0,
         {:ok, _} <- from_wire(Map.take(body, @wire_fields)) do
      true
    else
      _ -> false
    end
  end

  def valid_wire_put?(_), do: false

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
      &validate_history/1,
      &validate_source_history_epoch/1,
      &validate_source_compaction_epoch/1,
      &validate_safe_source_sequence/1,
      &validate_installed_source_compaction_epoch/1
    ]

    Enum.find_value(validators, & &1.(attrs))
  end

  defp validate_version(%{version: 1}), do: nil
  defp validate_version(_), do: VialKeeper.Error.invalid_request("unsupported checkpoint version")

  defp validate_replication_id(%{replication_id: value})
       when is_binary(value) and value != "",
       do: nil

  defp validate_replication_id(_),
    do: VialKeeper.Error.invalid_request("checkpoint replication_id is required")

  defp validate_checkpoint_version(%{checkpoint_version: value})
       when is_integer(value) and value >= 0,
       do: nil

  defp validate_checkpoint_version(_),
    do: VialKeeper.Error.invalid_request("checkpoint_version must be non-negative")

  defp validate_session_id(%{session_id: value}) when is_binary(value) and value != "", do: nil

  defp validate_session_id(_),
    do: VialKeeper.Error.invalid_request("checkpoint session_id is required")

  defp validate_source_sequence(%{source_sequence: value})
       when is_integer(value) and value >= 0,
       do: nil

  defp validate_source_sequence(_),
    do: VialKeeper.Error.invalid_request("source_sequence must be non-negative")

  defp validate_history(%{history: value}) when is_list(value), do: nil

  defp validate_history(_),
    do: VialKeeper.Error.invalid_request("checkpoint history must be an array")

  defp validate_source_history_epoch(%{source_history_epoch: value})
       when is_binary(value) and value != "",
       do: nil

  defp validate_source_history_epoch(_),
    do: VialKeeper.Error.invalid_request("checkpoint source_history_epoch is required")

  defp validate_source_compaction_epoch(%{source_compaction_epoch: value})
       when is_integer(value) and value >= 0,
       do: nil

  defp validate_source_compaction_epoch(_),
    do: VialKeeper.Error.invalid_request("checkpoint source_compaction_epoch is required")

  defp validate_safe_source_sequence(%{safe_source_sequence: value, source_sequence: source})
       when is_integer(value) and value >= 0 and is_integer(source) and value <= source,
       do: nil

  defp validate_safe_source_sequence(%{safe_source_sequence: value})
       when is_integer(value) and value >= 0,
       do: nil

  defp validate_safe_source_sequence(_),
    do: VialKeeper.Error.invalid_request("checkpoint safe_source_sequence is required")

  defp validate_installed_source_compaction_epoch(%{installed_source_compaction_epoch: value})
       when is_integer(value) and value >= 0,
       do: nil

  defp validate_installed_source_compaction_epoch(_),
    do: VialKeeper.Error.invalid_request("checkpoint installed_source_compaction_epoch is required")
end
