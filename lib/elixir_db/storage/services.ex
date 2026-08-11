defmodule ElixirDB.Storage.Services do
  @moduledoc """
  Shared storage services for mutation, import, and replication chain reads.

  Callers pass an opaque `BackendContext`. Services execute against storage
  ports inside backend transactions; physical backends only load facts and
  apply effects.
  """

  alias ElixirDB.MapAccess
  alias ElixirDB.Storage.BackendContext
  alias ElixirDB.Storage.Services.{Chains, Import, Mutations}
  alias ElixirDB.Storage.Transaction

  @doc "Applies one local put/delete mutation."
  @spec apply_local_mutation(BackendContext.t(), map()) ::
          {:ok, map()} | {:error, ElixirDB.Error.t()}
  def apply_local_mutation(%BackendContext{} = context, request) when is_map(request) do
    Transaction.run(context, &Mutations.apply_local_tx(&1, request))
  end

  @doc "Applies a bulk mutation/resolve batch."
  @spec apply_bulk_mutation(BackendContext.t(), map()) ::
          {:ok, list()} | {:error, ElixirDB.Error.t()}
  def apply_bulk_mutation(%BackendContext{} = context, request) when is_map(request) do
    operations = MapAccess.get(request, :operations)

    with :ok <- Mutations.validate_operation_batch(operations) do
      Transaction.run(context, &Mutations.bulk_tx(&1, operations))
    end
  end

  @doc "Resolves a document conflict."
  @spec resolve_conflict(BackendContext.t(), map()) ::
          {:ok, map()} | {:error, ElixirDB.Error.t()}
  def resolve_conflict(%BackendContext{} = context, request) when is_map(request) do
    Transaction.run(context, &Mutations.resolve_conflict_tx(&1, request))
  end

  @doc "Diffs requested leaves against stored leaf sets."
  @spec diff_revisions(BackendContext.t(), map()) ::
          {:ok, map()} | {:error, ElixirDB.Error.t()}
  def diff_revisions(%BackendContext{} = context, request) when is_map(request) do
    Chains.diff(context, request)
  end

  @doc "Loads parent-ordered revision chains."
  @spec get_revision_chains(BackendContext.t(), map()) ::
          {:ok, map()} | {:error, ElixirDB.Error.t()}
  def get_revision_chains(%BackendContext{} = context, request) when is_map(request) do
    Chains.get(context, request)
  end

  @doc "Imports revision chains with optional purged boundaries."
  @spec import_revision_chains(BackendContext.t(), map()) ::
          {:ok, map()} | {:error, ElixirDB.Error.t()}
  def import_revision_chains(%BackendContext{} = context, request) when is_map(request) do
    chains = MapAccess.get(request, :chains, [])

    with :ok <- Import.validate_chain_batch(chains),
         :ok <-
           Import.validate_purged_boundaries(
             MapAccess.get(request, :purged_boundaries, []),
             MapAccess.get(request, :source_database_uuid)
           ),
         :ok <- Import.ensure_physical_blobs(context, chains) do
      Transaction.run(context, &Import.import_tx(&1, request))
    end
  end
end
