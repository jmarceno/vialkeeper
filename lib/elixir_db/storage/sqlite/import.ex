defmodule ElixirDB.Storage.SQLite.Import do
  @moduledoc """
  Compatibility wrappers that route SQLite adapter handles into shared import
  services. Prefer `ElixirDB.Storage.Services` with a `BackendContext`.
  """

  alias ElixirDB.Storage.Services.Import
  alias ElixirDB.Storage.SQLite.Adapter

  @doc "Validates host limits for a replication chain batch."
  @spec validate_chain_batch(term()) :: :ok | {:error, ElixirDB.Error.t()}
  def validate_chain_batch(chains), do: Import.validate_chain_batch(chains)

  @doc "Validates purged boundary payloads."
  @spec validate_purged_boundaries(term()) :: :ok | {:error, ElixirDB.Error.t()}
  def validate_purged_boundaries(boundaries), do: Import.validate_purged_boundaries(boundaries)

  @spec validate_purged_boundaries(term(), binary() | nil) :: :ok | {:error, ElixirDB.Error.t()}
  def validate_purged_boundaries(boundaries, source_database_uuid),
    do: Import.validate_purged_boundaries(boundaries, source_database_uuid)

  @doc "Ensures attachment digests referenced by import chains are present."
  @spec ensure_physical_blobs(map(), term()) :: :ok | {:error, ElixirDB.Error.t()}
  def ensure_physical_blobs(adapter, chains),
    do: Import.ensure_physical_blobs(Adapter.to_context(adapter), chains)

  @doc "Imports revision chains inside an already-open transaction."
  @spec import_tx(map(), map()) :: {:ok, map()} | {:error, ElixirDB.Error.t()}
  def import_tx(adapter, request),
    do: Import.import_tx(Adapter.to_context(adapter), request)
end
