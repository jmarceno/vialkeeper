defmodule ElixirDB.Storage.Services do
  @moduledoc """
  Shared storage services for mutation, import, replication chains, retention,
  integrity, attachment-metadata, query, local-view, and derived-view workflows.

  Callers pass an opaque `BackendContext`. Services execute against storage
  ports inside backend transactions; physical backends only load facts and
  apply effects.
  """

  alias ElixirDB.MapAccess
  alias ElixirDB.Storage.BackendContext

  alias ElixirDB.Storage.Services.{
    Attachments,
    Chains,
    DerivedViews,
    Import,
    Integrity,
    Mutations,
    Query,
    Retention,
    Views
  }

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

  @doc "Compacts retention to the stable frontier."
  @spec compact_retention(BackendContext.t(), map()) ::
          {:ok, map()} | {:error, ElixirDB.Error.t()}
  def compact_retention(%BackendContext{} = context, request \\ %{}) when is_map(request) do
    fault = Map.get(context.identity || %{}, :retention_fault)

    Transaction.run(context, fn tx_context ->
      Retention.compact_tx(put_retention_fault(tx_context, fault), request)
    end)
  end

  @doc "Returns retention state metadata."
  @spec retention_state(BackendContext.t()) :: {:ok, map()} | {:error, ElixirDB.Error.t()}
  def retention_state(%BackendContext{} = context), do: Retention.retention_state(context)

  @doc "Lists peer ledger positions."
  @spec list_peer_positions(BackendContext.t()) :: {:ok, list()} | {:error, ElixirDB.Error.t()}
  def list_peer_positions(%BackendContext{} = context),
    do: Retention.list_peer_positions(context)

  @doc "Compare-and-swaps a peer ledger position."
  @spec put_peer_position_cas(BackendContext.t(), map()) ::
          {:ok, map()} | {:error, ElixirDB.Error.t()}
  def put_peer_position_cas(%BackendContext{} = context, request) when is_map(request) do
    Transaction.run(context, &Retention.put_peer_position_cas(&1, request))
  end

  @doc "Reads one retention boundary page."
  @spec read_boundary_pages(BackendContext.t(), map()) ::
          {:ok, map()} | {:error, ElixirDB.Error.t()}
  def read_boundary_pages(%BackendContext{} = context, request) when is_map(request) do
    Retention.read_boundary_pages(context, request)
  end

  @doc "Installs one retention boundary page."
  @spec install_boundary_pages(BackendContext.t(), map()) ::
          {:ok, map()} | {:error, ElixirDB.Error.t()}
  def install_boundary_pages(%BackendContext{} = context, request) when is_map(request) do
    Transaction.run(context, &Retention.install_boundary_pages(&1, request))
  end

  @doc "Runs logical integrity rules and optional physical backend probes."
  @spec integrity_check(BackendContext.t(), map()) :: {:ok, map()} | {:error, ElixirDB.Error.t()}
  def integrity_check(%BackendContext{} = context, opts \\ %{}) when is_map(opts) do
    Integrity.check(context, opts)
  end

  @doc "Resolves an attachment stream ticket."
  @spec resolve_attachment_ticket(BackendContext.t(), map()) ::
          {:ok, map()} | {:error, ElixirDB.Error.t()}
  def resolve_attachment_ticket(%BackendContext{} = context, request) when is_map(request),
    do: Attachments.resolve_attachment_ticket(context, request)

  @doc "Resolves reachable blob metadata by digest."
  @spec resolve_blob_metadata(BackendContext.t(), map()) ::
          {:ok, map()} | {:error, ElixirDB.Error.t()}
  def resolve_blob_metadata(%BackendContext{} = context, request) when is_map(request),
    do: Attachments.resolve_blob_metadata(context, request)

  @doc "Protects a pending attachment blob digest."
  @spec protect_pending_blob(BackendContext.t(), map()) ::
          {:ok, map()} | {:error, ElixirDB.Error.t()}
  def protect_pending_blob(%BackendContext{} = context, request) when is_map(request),
    do: Attachments.protect_pending_blob(context, request)

  @doc "Removes pending attachment blob protection."
  @spec remove_pending_blob_protection(BackendContext.t(), map()) ::
          {:ok, map()} | {:error, ElixirDB.Error.t()}
  def remove_pending_blob_protection(%BackendContext{} = context, request) when is_map(request),
    do: Attachments.remove_pending_blob_protection(context, request)

  @doc "Lists live attachment digests with optional paging."
  @spec list_live_attachment_digests(BackendContext.t(), map()) ::
          {:ok, map()} | {:error, ElixirDB.Error.t()}
  def list_live_attachment_digests(%BackendContext{} = context, request) when is_map(request),
    do: Attachments.list_live_attachment_digests(context, request)

  @doc "Cleans up expired pending blob protection rows."
  @spec cleanup_expired_pending_blobs(BackendContext.t(), map()) ::
          {:ok, map()} | {:error, ElixirDB.Error.t()}
  def cleanup_expired_pending_blobs(%BackendContext{} = context, request \\ %{})
      when is_map(request),
      do: Attachments.cleanup_expired_pending_blobs(context, request)

  @doc "Executes a normalized query against storage ports."
  @spec execute_query(BackendContext.t(), map(), map() | nil) ::
          {:ok, map()} | {:error, ElixirDB.Error.t()}
  def execute_query(%BackendContext{} = context, request, identity \\ nil)
      when is_map(request) do
    Query.execute(context, request, identity)
  end

  @doc "Executes a subscription membership snapshot query."
  @spec execute_subscription_snapshot(BackendContext.t(), map(), map() | nil) ::
          {:ok, map()} | {:error, ElixirDB.Error.t()}
  def execute_subscription_snapshot(%BackendContext{} = context, request, identity \\ nil)
      when is_map(request) do
    Query.subscription_snapshot(context, request, identity)
  end

  @doc "Builds a public explain payload for a normalized query."
  @spec explain_query(BackendContext.t(), map(), map() | nil) ::
          {:ok, map()} | {:error, ElixirDB.Error.t()}
  def explain_query(%BackendContext{} = context, request, identity \\ nil)
      when is_map(request) do
    Query.explain(context, request, identity)
  end

  @doc "Lists local views."
  @spec list_views(BackendContext.t()) :: {:ok, [map()]} | {:error, ElixirDB.Error.t()}
  def list_views(%BackendContext{} = context), do: Views.list(context)

  @doc "Creates a local view definition."
  @spec create_view(BackendContext.t(), map()) :: {:ok, map()} | {:error, ElixirDB.Error.t()}
  def create_view(%BackendContext{} = context, definition) when is_map(definition),
    do: Views.create(context, definition)

  @doc "Deletes a local view."
  @spec delete_view(BackendContext.t(), binary()) :: {:ok, map()} | {:error, ElixirDB.Error.t()}
  def delete_view(%BackendContext{} = context, view_id) when is_binary(view_id),
    do: Views.delete(context, view_id)

  @doc "Reads local view state."
  @spec view_state(BackendContext.t(), binary()) :: {:ok, map()} | {:error, ElixirDB.Error.t()}
  def view_state(%BackendContext{} = context, view_id) when is_binary(view_id),
    do: Views.state(context, view_id)

  @doc "Applies an incremental local-view batch."
  @spec apply_view_batch(BackendContext.t(), map()) :: {:ok, map()} | {:error, ElixirDB.Error.t()}
  def apply_view_batch(%BackendContext{} = context, request) when is_map(request),
    do: Views.apply_batch(context, request)

  @doc "Begins a local-view rebuild."
  @spec begin_view_rebuild(BackendContext.t(), map()) ::
          {:ok, map()} | {:error, ElixirDB.Error.t()}
  def begin_view_rebuild(%BackendContext{} = context, request) when is_map(request),
    do: Views.begin_rebuild(context, request)

  @doc "Appends a local-view rebuild page."
  @spec append_view_rebuild_page(BackendContext.t(), map()) ::
          {:ok, map()} | {:error, ElixirDB.Error.t()}
  def append_view_rebuild_page(%BackendContext{} = context, request) when is_map(request),
    do: Views.append_rebuild_page(context, request)

  @doc "Finishes a local-view rebuild."
  @spec finish_view_rebuild(BackendContext.t(), map()) ::
          {:ok, map()} | {:error, ElixirDB.Error.t()}
  def finish_view_rebuild(%BackendContext{} = context, request) when is_map(request),
    do: Views.finish_rebuild(context, request)

  @doc "Queries a local view."
  @spec query_view(BackendContext.t(), map()) :: {:ok, map()} | {:error, ElixirDB.Error.t()}
  def query_view(%BackendContext{} = context, request) when is_map(request),
    do: Views.query(context, request)

  @doc "Reads a page of winning documents for view rebuild scans."
  @spec read_winning_documents_page(BackendContext.t(), map()) ::
          {:ok, map()} | {:error, ElixirDB.Error.t()}
  def read_winning_documents_page(%BackendContext{} = context, request) when is_map(request),
    do: Views.read_winning_documents_page(context, request)

  @doc "Loads derived view metadata."
  @spec get_derived_view(BackendContext.t()) :: {:ok, map()} | {:error, ElixirDB.Error.t()}
  def get_derived_view(%BackendContext{} = context), do: DerivedViews.get(context)

  @doc "Enables or disables derived materialization."
  @spec set_derived_enabled(BackendContext.t(), map()) ::
          {:ok, map()} | {:error, ElixirDB.Error.t()}
  def set_derived_enabled(%BackendContext{} = context, request) when is_map(request),
    do: DerivedViews.set_enabled(context, request)

  @doc "Lists derived sources."
  @spec list_derived_sources(BackendContext.t()) :: {:ok, [map()]} | {:error, ElixirDB.Error.t()}
  def list_derived_sources(%BackendContext{} = context), do: DerivedViews.list_sources(context)

  @doc "Records a derived source error."
  @spec set_derived_source_error(BackendContext.t(), map()) ::
          {:ok, map()} | {:error, ElixirDB.Error.t()}
  def set_derived_source_error(%BackendContext{} = context, request) when is_map(request),
    do: DerivedViews.set_source_error(context, request)

  @doc "Applies a derived source batch."
  @spec apply_derived_source_batch(BackendContext.t(), map()) ::
          {:ok, map()} | {:error, ElixirDB.Error.t()}
  def apply_derived_source_batch(%BackendContext{} = context, request) when is_map(request),
    do: DerivedViews.apply_source_batch(context, request)

  @doc "Begins a derived source rebuild."
  @spec begin_derived_source_rebuild(BackendContext.t(), map()) ::
          {:ok, map()} | {:error, ElixirDB.Error.t()}
  def begin_derived_source_rebuild(%BackendContext{} = context, request) when is_map(request),
    do: DerivedViews.begin_source_rebuild(context, request)

  @doc "Applies a derived rebuild page."
  @spec apply_derived_rebuild_page(BackendContext.t(), map()) ::
          {:ok, map()} | {:error, ElixirDB.Error.t()}
  def apply_derived_rebuild_page(%BackendContext{} = context, request) when is_map(request),
    do: DerivedViews.apply_rebuild_page(context, request)

  @doc "Prunes stale derived rebuild contributions."
  @spec prune_derived_rebuild_stale_page(BackendContext.t(), map()) ::
          {:ok, map()} | {:error, ElixirDB.Error.t()}
  def prune_derived_rebuild_stale_page(%BackendContext{} = context, request)
      when is_map(request),
      do: DerivedViews.prune_rebuild_stale_page(context, request)

  @doc "Finishes a derived source rebuild."
  @spec finish_derived_source_rebuild(BackendContext.t(), map()) ::
          {:ok, map()} | {:error, ElixirDB.Error.t()}
  def finish_derived_source_rebuild(%BackendContext{} = context, request) when is_map(request),
    do: DerivedViews.finish_source_rebuild(context, request)

  defp put_retention_fault(%BackendContext{} = context, fun) when is_function(fun, 1) do
    %{context | identity: Map.put(context.identity || %{}, :retention_fault, fun)}
  end

  defp put_retention_fault(%BackendContext{} = context, _), do: context
end
