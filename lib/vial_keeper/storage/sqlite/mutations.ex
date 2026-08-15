defmodule VialKeeper.Storage.SQLite.Mutations do
  @moduledoc """
  Compatibility wrappers that route SQLite adapter handles into shared mutation
  services. Prefer `VialKeeper.Storage.Services` with a `BackendContext`.
  """

  alias VialKeeper.Storage.Services.Mutations
  alias VialKeeper.Storage.SQLite.Adapter

  @doc "Applies one local mutation inside an already-open transaction."
  @spec apply_local_tx(map(), map()) :: {:ok, map()} | {:error, VialKeeper.Error.t()}
  def apply_local_tx(adapter, request),
    do: Mutations.apply_local_tx(Adapter.to_context(adapter), request)

  @doc "Applies a bulk mutation batch inside an already-open transaction."
  @spec bulk_tx(map(), [map()]) :: {:ok, [map()]} | {:error, VialKeeper.Error.t()}
  def bulk_tx(adapter, operations),
    do: Mutations.bulk_tx(Adapter.to_context(adapter), operations)

  @doc "Resolves a conflict inside an already-open transaction."
  @spec resolve_conflict_tx(map(), map()) :: {:ok, map()} | {:error, VialKeeper.Error.t()}
  def resolve_conflict_tx(adapter, request),
    do: Mutations.resolve_conflict_tx(Adapter.to_context(adapter), request)

  @doc "Validates a bulk operation list against host limits."
  @spec validate_operation_batch(term()) :: :ok | {:error, VialKeeper.Error.t()}
  def validate_operation_batch(operations), do: Mutations.validate_operation_batch(operations)
end
