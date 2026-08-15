defmodule VialKeeper.Domain.RetentionState do
  @moduledoc "Validated compact-retention metadata state."

  @enforce_keys [
    :history_epoch,
    :floor_sequence,
    :compaction_epoch,
    :boundary_digest,
    :mode,
    :maintenance_counter
  ]
  defstruct [
    :history_epoch,
    :floor_sequence,
    :compaction_epoch,
    :boundary_digest,
    :mode,
    :maintenance_counter
  ]

  @type mode :: :disabled | :stable_frontier

  @type t :: %__MODULE__{
          history_epoch: binary(),
          floor_sequence: non_neg_integer(),
          compaction_epoch: non_neg_integer(),
          boundary_digest: binary() | nil,
          mode: mode(),
          maintenance_counter: non_neg_integer()
        }

  @known [
    :history_epoch,
    :floor_sequence,
    :compaction_epoch,
    :boundary_digest,
    :mode,
    :maintenance_counter
  ]

  @spec new(map()) :: {:ok, t()} | {:error, VialKeeper.Error.t()}
  def new(attrs) when is_map(attrs) do
    if Enum.any?(Map.keys(attrs), &(&1 not in @known)),
      do: {:error, VialKeeper.Error.invalid_request("unknown retention state field")},
      else: build(attrs)
  end

  def new(_), do: {:error, VialKeeper.Error.invalid_request("retention state must be an object")}

  @spec from_wire(map()) :: {:ok, t()} | {:error, VialKeeper.Error.t()}
  def from_wire(attrs) when is_map(attrs) do
    allowed = [
      "history_epoch",
      "floor_sequence",
      "compaction_epoch",
      "boundary_digest",
      "mode",
      "maintenance_counter"
    ]

    if Enum.any?(Map.keys(attrs), &(&1 not in allowed)) do
      {:error, VialKeeper.Error.invalid_request("unknown retention state field")}
    else
      with {:ok, mode} <- decode_mode(attrs["mode"]) do
        new(%{
          history_epoch: attrs["history_epoch"],
          floor_sequence: attrs["floor_sequence"],
          compaction_epoch: attrs["compaction_epoch"],
          boundary_digest: normalize_boundary_digest(attrs["boundary_digest"]),
          mode: mode,
          maintenance_counter: attrs["maintenance_counter"]
        })
      end
    end
  end

  def from_wire(_),
    do: {:error, VialKeeper.Error.invalid_request("retention state must be an object")}

  @spec floor_can_advance?(non_neg_integer(), non_neg_integer()) :: boolean()
  def floor_can_advance?(current_floor, candidate)
      when is_integer(current_floor) and current_floor >= 0 and is_integer(candidate) and
             candidate >= 0 do
    candidate >= current_floor
  end

  @spec compaction_epoch_after(boolean(), non_neg_integer()) :: non_neg_integer()
  def compaction_epoch_after(true, current_epoch)
      when is_integer(current_epoch) and current_epoch >= 0,
      do: current_epoch + 1

  def compaction_epoch_after(false, current_epoch)
      when is_integer(current_epoch) and current_epoch >= 0,
      do: current_epoch

  defp build(attrs) do
    case validation_error(attrs) do
      nil -> {:ok, struct(__MODULE__, attrs)}
      error -> {:error, error}
    end
  end

  defp validation_error(attrs) do
    validators = [
      &validate_history_epoch/1,
      &validate_floor_sequence/1,
      &validate_compaction_epoch/1,
      &validate_boundary_digest/1,
      &validate_mode/1,
      &validate_maintenance_counter/1
    ]

    Enum.find_value(validators, & &1.(attrs))
  end

  defp validate_history_epoch(%{history_epoch: value})
       when is_binary(value) and value != "",
       do: nil

  defp validate_history_epoch(_),
    do: VialKeeper.Error.invalid_request("retention state history_epoch is required")

  defp validate_floor_sequence(%{floor_sequence: value})
       when is_integer(value) and value >= 0,
       do: nil

  defp validate_floor_sequence(_),
    do: VialKeeper.Error.invalid_request("retention floor_sequence must be non-negative")

  defp validate_compaction_epoch(%{compaction_epoch: value})
       when is_integer(value) and value >= 0,
       do: nil

  defp validate_compaction_epoch(_),
    do: VialKeeper.Error.invalid_request("retention compaction_epoch must be non-negative")

  defp validate_boundary_digest(%{boundary_digest: value})
       when is_nil(value) or (is_binary(value) and value != ""),
       do: nil

  defp validate_boundary_digest(_),
    do: VialKeeper.Error.invalid_request("retention boundary_digest must be a binary or null")

  defp validate_mode(%{mode: value}) when value in [:disabled, :stable_frontier], do: nil

  defp validate_mode(_),
    do: VialKeeper.Error.invalid_request("retention mode must be disabled or stable_frontier")

  defp validate_maintenance_counter(%{maintenance_counter: value})
       when is_integer(value) and value >= 0,
       do: nil

  defp validate_maintenance_counter(_),
    do: VialKeeper.Error.invalid_request("retention maintenance_counter must be non-negative")

  defp decode_mode("disabled"), do: {:ok, :disabled}
  defp decode_mode("stable_frontier"), do: {:ok, :stable_frontier}

  defp decode_mode(_),
    do:
      {:error,
       VialKeeper.Error.invalid_request("retention mode must be disabled or stable_frontier")}

  defp normalize_boundary_digest(nil), do: nil
  defp normalize_boundary_digest(""), do: nil
  defp normalize_boundary_digest(value) when is_binary(value), do: value

  defp normalize_boundary_digest(_),
    do: raise(ArgumentError, "boundary_digest must be a binary or null")
end
