defmodule ElixirDB.Domain.ReplicationJob do
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
    cond do
      not is_binary(attrs[:job_id]) or attrs[:job_id] == "" ->
        {:error, ElixirDB.Error.invalid_request("replication job_id is required")}

      attrs[:direction] not in ["push", "pull"] ->
        {:error, ElixirDB.Error.invalid_request("replication direction must be push or pull")}

      attrs[:mode] not in ["one_shot", "continuous"] ->
        {:error, ElixirDB.Error.invalid_request("replication mode must be one_shot or continuous")}

      is_nil(attrs[:endpoint]) ->
        {:error, ElixirDB.Error.invalid_request("replication endpoint is required")}

      not is_boolean(attrs[:enabled]) ->
        {:error, ElixirDB.Error.invalid_request("replication enabled must be boolean")}

      not is_nil(attrs[:persist]) and not is_boolean(attrs[:persist]) ->
        {:error, ElixirDB.Error.invalid_request("replication persist must be boolean")}

      not is_nil(attrs[:batch]) and not is_map(attrs[:batch]) ->
        {:error, ElixirDB.Error.invalid_request("replication batch must be an object")}

      not is_nil(attrs[:retry]) and not is_map(attrs[:retry]) ->
        {:error, ElixirDB.Error.invalid_request("replication retry must be an object")}

      true ->
        {:ok, struct(__MODULE__, attrs)}
    end
  end
end
