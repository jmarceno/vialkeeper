defmodule ElixirDB.Domain.ReplicationJob do
  @moduledoc "Validated replication job definitions."

  @enforce_keys [:job_id, :direction, :mode, :endpoint, :enabled]
  defstruct [
    :job_id,
    :direction,
    :mode,
    :endpoint,
    :enabled,
    :persist,
    :batch,
    :retry,
    :state,
    :diagnostic
  ]

  @type t :: %__MODULE__{
          job_id: binary(),
          direction: binary(),
          mode: binary(),
          endpoint: map() | ElixirDB.Domain.ReplicationEndpoint.t(),
          enabled: boolean(),
          persist: boolean() | nil,
          batch: map() | nil,
          retry: map() | nil,
          state: atom() | nil,
          diagnostic: map() | nil
        }

  @known [
    :job_id,
    :direction,
    :mode,
    :endpoint,
    :enabled,
    :persist,
    :batch,
    :retry,
    :state,
    :diagnostic
  ]

  @spec new(map()) :: {:ok, t()} | {:error, ElixirDB.Error.t()}
  def new(attrs) when is_map(attrs) do
    if Enum.any?(Map.keys(attrs), &(&1 not in @known)),
      do: {:error, ElixirDB.Error.invalid_request("unknown replication job field")},
      else: build(attrs)
  end

  def new(_), do: {:error, ElixirDB.Error.invalid_request("replication job must be an object")}

  defp build(attrs) do
    case validation_error(attrs) do
      nil -> {:ok, struct(__MODULE__, attrs)}
      error -> {:error, error}
    end
  end

  defp validation_error(attrs) do
    validators = [
      &validate_job_id/1,
      &validate_direction/1,
      &validate_mode/1,
      &validate_endpoint/1,
      &validate_enabled/1,
      &validate_persist/1,
      &validate_batch/1,
      &validate_retry/1
    ]

    Enum.find_value(validators, & &1.(attrs))
  end

  defp validate_job_id(%{job_id: value}) when is_binary(value) and value != "", do: nil
  defp validate_job_id(_), do: ElixirDB.Error.invalid_request("replication job_id is required")

  defp validate_direction(%{direction: value}) when value in ["push", "pull"], do: nil

  defp validate_direction(_),
    do: ElixirDB.Error.invalid_request("replication direction must be push or pull")

  defp validate_mode(%{mode: value}) when value in ["one_shot", "continuous"], do: nil

  defp validate_mode(_),
    do: ElixirDB.Error.invalid_request("replication mode must be one_shot or continuous")

  defp validate_endpoint(%{endpoint: value}) when not is_nil(value), do: nil
  defp validate_endpoint(_), do: ElixirDB.Error.invalid_request("replication endpoint is required")

  defp validate_enabled(%{enabled: value}) when is_boolean(value), do: nil

  defp validate_enabled(_),
    do: ElixirDB.Error.invalid_request("replication enabled must be boolean")

  defp validate_persist(%{persist: value}) when is_nil(value) or is_boolean(value), do: nil

  defp validate_persist(_),
    do: ElixirDB.Error.invalid_request("replication persist must be boolean")

  defp validate_batch(%{batch: value}) when is_nil(value) or is_map(value), do: nil
  defp validate_batch(_), do: ElixirDB.Error.invalid_request("replication batch must be an object")

  defp validate_retry(%{retry: value}) when is_nil(value) or is_map(value), do: nil
  defp validate_retry(_), do: ElixirDB.Error.invalid_request("replication retry must be an object")
end
