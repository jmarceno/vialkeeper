defmodule ElixirDB.Storage.Services do
  @moduledoc """
  Shared storage services for mutation, import, replication chains, retention,
  integrity, attachment-metadata, query, local-view, and derived-view workflows.

  Callers pass an opaque `BackendContext`. Services execute against storage
  ports inside backend transactions; physical backends only load facts and
  apply effects.
  """

  alias ElixirDB.MapAccess
  alias ElixirDB.Query.{Normalizer, SubscriptionRequest}
  alias ElixirDB.Replication.Profile
  alias ElixirDB.Storage.BackendContext
  alias ElixirDB.Storage.Ports.Access
  alias ElixirDB.Storage.RequestValidation
  alias ElixirDB.Storage.Results

  alias ElixirDB.Storage.Services.{
    Attachments,
    Chains,
    DerivedViews,
    Facts,
    Import,
    Integrity,
    Mutations,
    Query,
    Retention,
    Shadows,
    Views
  }

  alias ElixirDB.Storage.Transaction

  @mutation_port_families [
    :transaction,
    :document_facts,
    :change_log,
    :retention_records,
    :index_candidates,
    :attachment_metadata
  ]

  @doc "Loads the current backend identity through the lifecycle port."
  @spec identity(BackendContext.t()) :: {:ok, map()} | {:error, ElixirDB.Error.t()}
  def identity(%BackendContext{} = context),
    do: with_port(context, :lifecycle, fn -> Access.port(context, :lifecycle).identity(context) end)

  @doc "Updates backend configuration through the lifecycle port."
  @spec update_config(BackendContext.t(), map()) ::
          {:ok, map()} | {:error, ElixirDB.Error.t()}
  def update_config(%BackendContext{} = context, config) when is_map(config),
    do:
      with_port(context, :lifecycle, fn ->
        Access.port(context, :lifecycle).update_config(context, config)
      end)

  @doc "Closes the selected backend through the lifecycle port."
  @spec close(BackendContext.t()) :: :ok | {:error, ElixirDB.Error.t()}
  def close(%BackendContext{} = context),
    do: with_port(context, :lifecycle, fn -> Access.port(context, :lifecycle).close(context) end)

  @doc "Loads one document through shared document and revision facts."
  @spec get_document(BackendContext.t(), map()) ::
          {:ok, map()} | {:error, ElixirDB.Error.t()}
  def get_document(%BackendContext{} = context, request) when is_map(request) do
    with_port(context, :document_facts, fn ->
      document_id = MapAccess.get(request, :document_id)
      requested_revision = MapAccess.get(request, :revision)
      include_conflicts = MapAccess.get(request, :include_conflicts, false)

      with :ok <- validate_identifier(document_id, "document_id"),
           {:ok, document} <- Facts.find_document(context, document_id),
           {:ok, document} <- require_document(document),
           {:ok, revision} <- selected_revision(context, document, requested_revision),
           {:ok, leaves} <- maybe_document_leaves(context, document_id, include_conflicts) do
        {:ok, Results.document_map(document, revision, leaves)}
      end
    end)
  end

  def get_document(%BackendContext{}, _request),
    do: {:error, ElixirDB.Error.invalid_request("document request must be an object")}

  @doc "Loads one requested revision through shared document facts."
  @spec get_revision(BackendContext.t(), map()) ::
          {:ok, map()} | {:error, ElixirDB.Error.t()}
  def get_revision(%BackendContext{} = context, request) when is_map(request) do
    with_port(context, :document_facts, fn ->
      document_id = MapAccess.get(request, :document_id)
      revision_id = MapAccess.get(request, :revision_id)

      with :ok <- validate_identifier(document_id, "document_id"),
           :ok <- validate_identifier(revision_id, "revision_id"),
           {:ok, document} <- Facts.find_document(context, document_id),
           {:ok, document} <- require_document(document),
           {:ok, revision} <- Facts.find_revision(context, document_id, revision_id) do
        {:ok, Results.document_map(document, revision, [])}
      end
    end)
  end

  def get_revision(%BackendContext{}, _request),
    do: {:error, ElixirDB.Error.invalid_request("revision request must be an object")}

  @doc "Reads a validated page through the change-log port."
  @spec read_changes(BackendContext.t(), map()) ::
          {:ok, map()} | {:error, ElixirDB.Error.t()}
  def read_changes(%BackendContext{} = context, request) when is_map(request) do
    with_port(context, :change_log, fn ->
      since = MapAccess.get(request, :since, 0)
      limit = MapAccess.get(request, :limit, 100)

      with :ok <- RequestValidation.validate_non_negative_integer(since, "since"),
           :ok <- RequestValidation.validate_positive_integer(limit, "limit"),
           {:ok, current_identity} <- identity(context),
           :ok <- RequestValidation.validate_changes_since_floor(since, current_identity),
           :ok <-
             RequestValidation.validate_changes_limit(
               limit,
               get_in(current_identity, [:config, "changes", "max_batch"])
             ) do
        Access.port(context, :change_log).read_page(context, since, limit)
      end
    end)
  end

  def read_changes(%BackendContext{}, _request),
    do: {:error, ElixirDB.Error.invalid_request("changes request must be an object")}

  @doc "Checks for pending local-origin changes through the change-log port."
  @spec has_local_origin_changes?(BackendContext.t(), binary() | nil) ::
          {:ok, boolean()} | {:error, ElixirDB.Error.t()}
  def has_local_origin_changes?(%BackendContext{} = context, peer_database_uuid \\ nil),
    do:
      with_port(context, :change_log, fn ->
        Access.port(context, :change_log).has_local_origin_changes?(context, peer_database_uuid)
      end)

  @doc "Clears pending local-causal state through the change-log port."
  @spec clear_pending_local_causal(BackendContext.t(), binary() | nil) ::
          {:ok, :cleared} | {:error, ElixirDB.Error.t()}
  def clear_pending_local_causal(%BackendContext{} = context, peer_database_uuid \\ nil),
    do:
      with_port(context, :change_log, fn ->
        Access.port(context, :change_log).clear_pending_local_causal(context, peer_database_uuid)
      end)

  @doc "Reads one versioned local record from a typed namespace."
  @spec get_local_record(BackendContext.t(), binary(), binary()) ::
          {:ok, map() | nil} | {:error, ElixirDB.Error.t()}
  def get_local_record(%BackendContext{} = context, namespace, key)
      when is_binary(namespace) and is_binary(key) do
    call_optional_port(context, :local_records, fn port -> port.get(context, namespace, key) end)
  end

  @doc "Compare-and-swaps one versioned local record."
  @spec put_local_record_cas(BackendContext.t(), map()) ::
          {:ok, map()} | {:error, ElixirDB.Error.t()}
  def put_local_record_cas(%BackendContext{} = context, request) when is_map(request) do
    call_optional_port(context, :local_records, fn port -> port.put_cas(context, request) end)
  end

  @doc "Lists logical index definitions from the index-candidate port."
  @spec list_indexes(BackendContext.t()) :: {:ok, list()} | {:error, ElixirDB.Error.t()}
  def list_indexes(%BackendContext{} = context),
    do:
      with_port(context, :index_candidates, fn ->
        Access.port(context, :index_candidates).list_indexes(context)
      end)

  @doc "Creates one logical index definition."
  @spec create_index(BackendContext.t(), map()) ::
          {:ok, map()} | {:error, ElixirDB.Error.t()}
  def create_index(%BackendContext{} = context, definition) when is_map(definition),
    do:
      with_port(context, :index_candidates, fn ->
        Access.port(context, :index_candidates).create_index(context, definition)
      end)

  @doc "Deletes one logical index definition."
  @spec delete_index(BackendContext.t(), binary()) :: :ok | {:error, ElixirDB.Error.t()}
  def delete_index(%BackendContext{} = context, index_id) when is_binary(index_id),
    do:
      with_port(context, :index_candidates, fn ->
        Access.port(context, :index_candidates).delete_index(context, index_id)
      end)

  @doc "Rebuilds one logical index definition."
  @spec rebuild_index(BackendContext.t(), binary()) ::
          {:ok, map()} | {:error, ElixirDB.Error.t()}
  def rebuild_index(%BackendContext{} = context, index_id) when is_binary(index_id),
    do:
      with_port(context, :index_candidates, fn ->
        Access.port(context, :index_candidates).rebuild_index(context, index_id)
      end)

  @doc "Normalizes and executes a public query through shared query services."
  @spec execute_public_query(BackendContext.t(), map()) ::
          {:ok, map()} | {:error, ElixirDB.Error.t()}
  def execute_public_query(%BackendContext{} = context, request) when is_map(request) do
    with_port(context, :index_candidates, fn ->
      with {:ok, current_identity} <- identity(context),
           {:ok, normalized} <- Normalizer.normalize_public_request(request) do
        Query.execute(context, normalized, current_identity)
      end
    end)
  end

  @doc "Normalizes and executes a public subscription snapshot."
  @spec execute_public_subscription_snapshot(BackendContext.t(), map()) ::
          {:ok, map()} | {:error, ElixirDB.Error.t()}
  def execute_public_subscription_snapshot(%BackendContext{} = context, request)
      when is_map(request) do
    with_port(context, :index_candidates, fn ->
      with {:ok, current_identity} <- identity(context),
           {:ok, normalized} <-
             SubscriptionRequest.prepare_snapshot(
               request,
               Map.get(current_identity, :config, %{})
             ) do
        Query.subscription_snapshot(context, normalized, current_identity)
      end
    end)
  end

  @doc "Normalizes and explains a public query."
  @spec explain_public_query(BackendContext.t(), map()) ::
          {:ok, map()} | {:error, ElixirDB.Error.t()}
  def explain_public_query(%BackendContext{} = context, request) when is_map(request) do
    with_port(context, :index_candidates, fn ->
      with {:ok, current_identity} <- identity(context),
           {:ok, normalized} <- Normalizer.normalize_public_request(request) do
        Query.explain(context, normalized, current_identity)
      end
    end)
  end

  @doc "Loads a bounded batch of revisions referenced by change entries."
  @spec get_revisions_batch(BackendContext.t(), [map()]) ::
          {:ok, [map()]} | {:error, ElixirDB.Error.t()}
  def get_revisions_batch(%BackendContext{} = context, requests) when is_list(requests) do
    with_port(context, :document_facts, fn ->
      get_revisions_batch_available(context, requests)
    end)
  end

  @doc "Lists persisted replication jobs through an optional backend port."
  @spec list_replication_jobs(BackendContext.t()) ::
          {:ok, list()} | {:error, ElixirDB.Error.t()}
  def list_replication_jobs(%BackendContext{} = context),
    do: call_optional_port(context, :replication_jobs, & &1.list(context))

  @doc "Persists one replication job through an optional backend port."
  @spec put_replication_job(BackendContext.t(), map()) ::
          {:ok, map()} | {:error, ElixirDB.Error.t()}
  def put_replication_job(%BackendContext{} = context, request) when is_map(request),
    do: call_optional_port(context, :replication_jobs, & &1.put(context, request))

  @doc "Deletes one replication job through an optional backend port."
  @spec delete_replication_job(BackendContext.t(), binary()) ::
          :ok | {:error, ElixirDB.Error.t()}
  def delete_replication_job(%BackendContext{} = context, job_id) when is_binary(job_id),
    do: call_optional_port(context, :replication_jobs, & &1.delete(context, job_id))

  @doc "Applies one local put/delete mutation."
  @spec apply_local_mutation(BackendContext.t(), map()) ::
          {:ok, map()} | {:error, ElixirDB.Error.t()}
  def apply_local_mutation(%BackendContext{} = context, request) when is_map(request) do
    families =
      if shadow_profile?(request),
        do: @mutation_port_families ++ [:shadow_state],
        else: @mutation_port_families

    with_ports(context, families, fn ->
      Transaction.run(context, &Mutations.apply_local_tx(&1, request))
    end)
  end

  @doc "Applies a bulk mutation/resolve batch."
  @spec apply_bulk_mutation(BackendContext.t(), map()) ::
          {:ok, list()} | {:error, ElixirDB.Error.t()}
  def apply_bulk_mutation(%BackendContext{} = context, request) when is_map(request) do
    operations = MapAccess.get(request, :operations)

    with_ports(context, @mutation_port_families, fn ->
      with :ok <- Mutations.validate_operation_batch(operations) do
        Transaction.run(context, &Mutations.bulk_tx(&1, operations))
      end
    end)
  end

  @doc "Resolves a document conflict."
  @spec resolve_conflict(BackendContext.t(), map()) ::
          {:ok, map()} | {:error, ElixirDB.Error.t()}
  def resolve_conflict(%BackendContext{} = context, request) when is_map(request) do
    with_ports(context, @mutation_port_families, fn ->
      Transaction.run(context, &Mutations.resolve_conflict_tx(&1, request))
    end)
  end

  @doc "Diffs requested leaves against stored leaf sets."
  @spec diff_revisions(BackendContext.t(), map()) ::
          {:ok, map()} | {:error, ElixirDB.Error.t()}
  def diff_revisions(%BackendContext{} = context, request) when is_map(request) do
    with_ports(context, [:document_facts, :retention_records], fn ->
      Chains.diff(context, request)
    end)
  end

  @doc "Loads parent-ordered revision chains."
  @spec get_revision_chains(BackendContext.t(), map()) ::
          {:ok, map()} | {:error, ElixirDB.Error.t()}
  def get_revision_chains(%BackendContext{} = context, request) when is_map(request) do
    with_ports(context, [:document_facts, :retention_records], fn ->
      Chains.get(context, request)
    end)
  end

  @doc "Imports revision chains with optional purged boundaries."
  @spec import_revision_chains(BackendContext.t(), map()) ::
          {:ok, map()} | {:error, ElixirDB.Error.t()}
  def import_revision_chains(%BackendContext{} = context, request) when is_map(request) do
    chains = MapAccess.get(request, :chains, [])

    with_ports(context, @mutation_port_families, fn ->
      with :ok <- Import.validate_chain_batch(chains),
           :ok <-
             Import.validate_purged_boundaries(
               MapAccess.get(request, :purged_boundaries, []),
               MapAccess.get(request, :source_database_uuid)
             ),
           :ok <- maybe_ensure_physical_blobs(context, chains, request) do
        Transaction.run(context, &Import.import_tx(&1, request))
      end
    end)
  end

  @doc "Reads the immutable shadow binding metadata."
  @spec shadow_metadata(BackendContext.t()) ::
          {:ok, map() | nil} | {:error, ElixirDB.Error.t()}
  def shadow_metadata(%BackendContext{} = context), do: Shadows.metadata(context)

  @doc "Reads one durable source document origin sequence."
  @spec shadow_origin(BackendContext.t(), binary()) ::
          {:ok, non_neg_integer() | nil} | {:error, ElixirDB.Error.t()}
  def shadow_origin(%BackendContext{} = context, document_id),
    do: Shadows.origin(context, document_id)

  @doc "Reads the durable shadow applied source watermark."
  @spec shadow_watermark(BackendContext.t()) ::
          {:ok, non_neg_integer()} | {:error, ElixirDB.Error.t()}
  def shadow_watermark(%BackendContext{} = context), do: Shadows.watermark(context)

  defp maybe_ensure_physical_blobs(context, chains, request) do
    if shadow_profile?(MapAccess.get(request, :profile)),
      do: :ok,
      else: Import.ensure_physical_blobs(context, chains)
  end

  defp shadow_profile?(%Profile{kind: :shadow}), do: true

  defp shadow_profile?(value) when is_map(value),
    do: shadow_profile?(MapAccess.get(value, :profile))

  defp shadow_profile?(value), do: value in [:shadow, "shadow"]

  @doc "Compacts retention to the stable frontier."
  @spec compact_retention(BackendContext.t(), map()) ::
          {:ok, map()} | {:error, ElixirDB.Error.t()}
  def compact_retention(%BackendContext{} = context, request \\ %{}) when is_map(request) do
    with_ports(context, [:transaction, :document_facts, :change_log, :retention_records], fn ->
      fault = Map.get(context.identity || %{}, :retention_fault)

      Transaction.run(context, fn tx_context ->
        Retention.compact_tx(put_retention_fault(tx_context, fault), request)
      end)
    end)
  end

  @doc "Returns retention state metadata."
  @spec retention_state(BackendContext.t()) :: {:ok, map()} | {:error, ElixirDB.Error.t()}
  def retention_state(%BackendContext{} = context),
    do: with_port(context, :retention_records, fn -> Retention.retention_state(context) end)

  @doc "Lists peer ledger positions."
  @spec list_peer_positions(BackendContext.t()) :: {:ok, list()} | {:error, ElixirDB.Error.t()}
  def list_peer_positions(%BackendContext{} = context),
    do: with_port(context, :retention_records, fn -> Retention.list_peer_positions(context) end)

  @doc "Compare-and-swaps a peer ledger position."
  @spec put_peer_position_cas(BackendContext.t(), map()) ::
          {:ok, map()} | {:error, ElixirDB.Error.t()}
  def put_peer_position_cas(%BackendContext{} = context, request) when is_map(request) do
    with_ports(context, [:transaction, :retention_records], fn ->
      Transaction.run(context, &Retention.put_peer_position_cas(&1, request))
    end)
  end

  @doc "Reads one retention boundary page."
  @spec read_boundary_pages(BackendContext.t(), map()) ::
          {:ok, map()} | {:error, ElixirDB.Error.t()}
  def read_boundary_pages(%BackendContext{} = context, request) when is_map(request) do
    with_port(context, :retention_records, fn ->
      Retention.read_boundary_pages(context, request)
    end)
  end

  @doc "Installs one retention boundary page."
  @spec install_boundary_pages(BackendContext.t(), map()) ::
          {:ok, map()} | {:error, ElixirDB.Error.t()}
  def install_boundary_pages(%BackendContext{} = context, request) when is_map(request) do
    with_ports(context, [:transaction, :retention_records], fn ->
      Transaction.run(context, &Retention.install_boundary_pages(&1, request))
    end)
  end

  @doc "Runs logical integrity rules and optional physical backend probes."
  @spec integrity_check(BackendContext.t(), map()) :: {:ok, map()} | {:error, ElixirDB.Error.t()}
  def integrity_check(%BackendContext{} = context, opts \\ %{}) when is_map(opts) do
    with_port(context, :inspection, fn -> Integrity.check(context, opts) end)
  end

  @doc "Resolves an attachment stream ticket."
  @spec resolve_attachment_ticket(BackendContext.t(), map()) ::
          {:ok, map()} | {:error, ElixirDB.Error.t()}
  def resolve_attachment_ticket(%BackendContext{} = context, request) when is_map(request),
    do:
      with_port(context, :attachment_metadata, fn ->
        Attachments.resolve_attachment_ticket(context, request)
      end)

  @doc "Resolves reachable blob metadata by digest."
  @spec resolve_blob_metadata(BackendContext.t(), map()) ::
          {:ok, map()} | {:error, ElixirDB.Error.t()}
  def resolve_blob_metadata(%BackendContext{} = context, request) when is_map(request),
    do:
      with_port(context, :attachment_metadata, fn ->
        Attachments.resolve_blob_metadata(context, request)
      end)

  @doc "Protects a pending attachment blob digest."
  @spec protect_pending_blob(BackendContext.t(), map()) ::
          {:ok, map()} | {:error, ElixirDB.Error.t()}
  def protect_pending_blob(%BackendContext{} = context, request) when is_map(request),
    do:
      with_ports(context, [:transaction, :attachment_metadata], fn ->
        Attachments.protect_pending_blob(context, request)
      end)

  @doc "Removes pending attachment blob protection."
  @spec remove_pending_blob_protection(BackendContext.t(), map()) ::
          {:ok, map()} | {:error, ElixirDB.Error.t()}
  def remove_pending_blob_protection(%BackendContext{} = context, request) when is_map(request),
    do:
      with_ports(context, [:transaction, :attachment_metadata], fn ->
        Attachments.remove_pending_blob_protection(context, request)
      end)

  @doc "Lists live attachment digests with optional paging."
  @spec list_live_attachment_digests(BackendContext.t(), map()) ::
          {:ok, map()} | {:error, ElixirDB.Error.t()}
  def list_live_attachment_digests(%BackendContext{} = context, request) when is_map(request),
    do:
      with_port(context, :attachment_metadata, fn ->
        Attachments.list_live_attachment_digests(context, request)
      end)

  @doc "Cleans up expired pending blob protection rows."
  @spec cleanup_expired_pending_blobs(BackendContext.t(), map()) ::
          {:ok, map()} | {:error, ElixirDB.Error.t()}
  def cleanup_expired_pending_blobs(%BackendContext{} = context, request \\ %{})
      when is_map(request),
      do:
        with_ports(context, [:transaction, :attachment_metadata], fn ->
          Attachments.cleanup_expired_pending_blobs(context, request)
        end)

  @doc "Executes a normalized query against storage ports."
  @spec execute_query(BackendContext.t(), map(), map() | nil) ::
          {:ok, map()} | {:error, ElixirDB.Error.t()}
  def execute_query(%BackendContext{} = context, request, identity \\ nil)
      when is_map(request) do
    with_port(context, :index_candidates, fn -> Query.execute(context, request, identity) end)
  end

  @doc "Executes a subscription membership snapshot query."
  @spec execute_subscription_snapshot(BackendContext.t(), map(), map() | nil) ::
          {:ok, map()} | {:error, ElixirDB.Error.t()}
  def execute_subscription_snapshot(%BackendContext{} = context, request, identity \\ nil)
      when is_map(request) do
    with_port(context, :index_candidates, fn ->
      Query.subscription_snapshot(context, request, identity)
    end)
  end

  @doc "Builds a public explain payload for a normalized query."
  @spec explain_query(BackendContext.t(), map(), map() | nil) ::
          {:ok, map()} | {:error, ElixirDB.Error.t()}
  def explain_query(%BackendContext{} = context, request, identity \\ nil)
      when is_map(request) do
    with_port(context, :index_candidates, fn -> Query.explain(context, request, identity) end)
  end

  @doc "Lists local views."
  @spec list_views(BackendContext.t()) :: {:ok, [map()]} | {:error, ElixirDB.Error.t()}
  def list_views(%BackendContext{} = context),
    do: with_port(context, :view_state, fn -> Views.list(context) end)

  @doc "Creates a local view definition."
  @spec create_view(BackendContext.t(), map()) :: {:ok, map()} | {:error, ElixirDB.Error.t()}
  def create_view(%BackendContext{} = context, definition) when is_map(definition),
    do: with_port(context, :view_state, fn -> Views.create(context, definition) end)

  @doc "Deletes a local view."
  @spec delete_view(BackendContext.t(), binary()) :: {:ok, map()} | {:error, ElixirDB.Error.t()}
  def delete_view(%BackendContext{} = context, view_id) when is_binary(view_id),
    do: with_port(context, :view_state, fn -> Views.delete(context, view_id) end)

  @doc "Reads local view state."
  @spec view_state(BackendContext.t(), binary()) :: {:ok, map()} | {:error, ElixirDB.Error.t()}
  def view_state(%BackendContext{} = context, view_id) when is_binary(view_id),
    do: with_port(context, :view_state, fn -> Views.state(context, view_id) end)

  @doc "Applies an incremental local-view batch."
  @spec apply_view_batch(BackendContext.t(), map()) :: {:ok, map()} | {:error, ElixirDB.Error.t()}
  def apply_view_batch(%BackendContext{} = context, request) when is_map(request),
    do: with_port(context, :view_state, fn -> Views.apply_batch(context, request) end)

  @doc "Begins a local-view rebuild."
  @spec begin_view_rebuild(BackendContext.t(), map()) ::
          {:ok, map()} | {:error, ElixirDB.Error.t()}
  def begin_view_rebuild(%BackendContext{} = context, request) when is_map(request),
    do: with_port(context, :view_state, fn -> Views.begin_rebuild(context, request) end)

  @doc "Appends a local-view rebuild page."
  @spec append_view_rebuild_page(BackendContext.t(), map()) ::
          {:ok, map()} | {:error, ElixirDB.Error.t()}
  def append_view_rebuild_page(%BackendContext{} = context, request) when is_map(request),
    do: with_port(context, :view_state, fn -> Views.append_rebuild_page(context, request) end)

  @doc "Finishes a local-view rebuild."
  @spec finish_view_rebuild(BackendContext.t(), map()) ::
          {:ok, map()} | {:error, ElixirDB.Error.t()}
  def finish_view_rebuild(%BackendContext{} = context, request) when is_map(request),
    do: with_port(context, :view_state, fn -> Views.finish_rebuild(context, request) end)

  @doc "Queries a local view."
  @spec query_view(BackendContext.t(), map()) :: {:ok, map()} | {:error, ElixirDB.Error.t()}
  def query_view(%BackendContext{} = context, request) when is_map(request),
    do: with_port(context, :view_state, fn -> Views.query(context, request) end)

  @doc "Reads a page of winning documents for view rebuild scans."
  @spec read_winning_documents_page(BackendContext.t(), map()) ::
          {:ok, map()} | {:error, ElixirDB.Error.t()}
  def read_winning_documents_page(%BackendContext{} = context, request) when is_map(request),
    do:
      with_port(context, :view_state, fn -> Views.read_winning_documents_page(context, request) end)

  @doc "Loads derived view metadata."
  @spec get_derived_view(BackendContext.t()) :: {:ok, map()} | {:error, ElixirDB.Error.t()}
  def get_derived_view(%BackendContext{} = context),
    do: with_port(context, :derived_state, fn -> DerivedViews.get(context) end)

  @doc "Enables or disables derived materialization."
  @spec set_derived_enabled(BackendContext.t(), map()) ::
          {:ok, map()} | {:error, ElixirDB.Error.t()}
  def set_derived_enabled(%BackendContext{} = context, request) when is_map(request),
    do: with_port(context, :derived_state, fn -> DerivedViews.set_enabled(context, request) end)

  @doc "Lists derived sources."
  @spec list_derived_sources(BackendContext.t()) :: {:ok, [map()]} | {:error, ElixirDB.Error.t()}
  def list_derived_sources(%BackendContext{} = context),
    do: with_port(context, :derived_state, fn -> DerivedViews.list_sources(context) end)

  @doc "Records a derived source error."
  @spec set_derived_source_error(BackendContext.t(), map()) ::
          {:ok, map()} | {:error, ElixirDB.Error.t()}
  def set_derived_source_error(%BackendContext{} = context, request) when is_map(request),
    do:
      with_port(context, :derived_state, fn -> DerivedViews.set_source_error(context, request) end)

  @doc "Applies a derived source batch."
  @spec apply_derived_source_batch(BackendContext.t(), map()) ::
          {:ok, map()} | {:error, ElixirDB.Error.t()}
  def apply_derived_source_batch(%BackendContext{} = context, request) when is_map(request),
    do:
      with_port(context, :derived_state, fn -> DerivedViews.apply_source_batch(context, request) end)

  @doc "Begins a derived source rebuild."
  @spec begin_derived_source_rebuild(BackendContext.t(), map()) ::
          {:ok, map()} | {:error, ElixirDB.Error.t()}
  def begin_derived_source_rebuild(%BackendContext{} = context, request) when is_map(request),
    do:
      with_port(context, :derived_state, fn ->
        DerivedViews.begin_source_rebuild(context, request)
      end)

  @doc "Applies a derived rebuild page."
  @spec apply_derived_rebuild_page(BackendContext.t(), map()) ::
          {:ok, map()} | {:error, ElixirDB.Error.t()}
  def apply_derived_rebuild_page(%BackendContext{} = context, request) when is_map(request),
    do:
      with_port(context, :derived_state, fn -> DerivedViews.apply_rebuild_page(context, request) end)

  @doc "Prunes stale derived rebuild contributions."
  @spec prune_derived_rebuild_stale_page(BackendContext.t(), map()) ::
          {:ok, map()} | {:error, ElixirDB.Error.t()}
  def prune_derived_rebuild_stale_page(%BackendContext{} = context, request)
      when is_map(request),
      do:
        with_port(context, :derived_state, fn ->
          DerivedViews.prune_rebuild_stale_page(context, request)
        end)

  @doc "Finishes a derived source rebuild."
  @spec finish_derived_source_rebuild(BackendContext.t(), map()) ::
          {:ok, map()} | {:error, ElixirDB.Error.t()}
  def finish_derived_source_rebuild(%BackendContext{} = context, request) when is_map(request),
    do:
      with_port(context, :derived_state, fn ->
        DerivedViews.finish_source_rebuild(context, request)
      end)

  defp require_document(nil),
    do: {:error, ElixirDB.Error.document_not_found("document does not exist")}

  defp require_document(document) when is_map(document), do: {:ok, document}

  defp selected_revision(%BackendContext{} = context, document, nil) do
    cond do
      document.winning_deleted ->
        {:error,
         ElixirDB.Error.document_not_found("document is deleted", %{
           winning_revision: document.winning_revision
         })}

      is_binary(document.winning_revision) ->
        Facts.find_revision(context, document.document_id, document.winning_revision)

      true ->
        {:error, ElixirDB.Error.document_not_found("document has no winning revision")}
    end
  end

  defp selected_revision(%BackendContext{} = context, document, revision_id) do
    with :ok <- validate_identifier(revision_id, "revision"),
         do: Facts.find_revision(context, document.document_id, revision_id)
  end

  defp get_revisions_batch_available(%BackendContext{} = context, requests) do
    with {:ok, current_identity} <- identity(context),
         :ok <-
           SubscriptionRequest.validate_revisions_batch(
             requests,
             Map.get(current_identity, :config, %{})
           ) do
      revision_batch_results(context, requests)
    end
  end

  defp revision_batch_results(context, requests) do
    Enum.reduce_while(requests, {:ok, []}, fn request, {:ok, acc} ->
      case revision_batch_item(context, request) do
        {:ok, result} -> {:cont, {:ok, [result | acc]}}
        {:error, _} = error -> {:halt, error}
      end
    end)
    |> case do
      {:ok, results} -> {:ok, Enum.reverse(results)}
      {:error, _} = error -> error
    end
  end

  defp maybe_document_leaves(_context, _document_id, false), do: {:ok, []}

  defp maybe_document_leaves(%BackendContext{} = context, document_id, true),
    do: Facts.list_leaves(context, document_id)

  defp maybe_document_leaves(_context, _document_id, _), do: {:ok, []}

  defp revision_batch_item(%BackendContext{} = context, request) when is_map(request) do
    document_id = MapAccess.get(request, :document_id)
    revision_id = MapAccess.get(request, :revision_id)

    with :ok <- validate_identifier(document_id, "document_id"),
         :ok <- validate_identifier(revision_id, "revision_id"),
         {:ok, document} <- Facts.find_document(context, document_id),
         {:ok, document} <- require_document(document),
         {:ok, revision} <- Facts.find_revision(context, document_id, revision_id) do
      {:ok,
       %{
         id: document.document_id,
         revision: revision.revision_id,
         deleted: revision.deleted,
         body: revision.body
       }}
    else
      {:error, %ElixirDB.Error{code: :document_not_found}} ->
        {:error,
         ElixirDB.Error.integrity_violation("changes entry references a missing document", %{
           document_id: document_id,
           revision_id: revision_id
         })}

      {:error, %ElixirDB.Error{code: :revision_not_found}} ->
        {:error,
         ElixirDB.Error.integrity_violation("changes entry references a missing revision", %{
           document_id: document_id,
           revision_id: revision_id
         })}

      {:error, _} = error ->
        error
    end
  end

  defp validate_identifier(value, _label) when is_binary(value) and value != "", do: :ok

  defp validate_identifier(_value, label),
    do: {:error, ElixirDB.Error.invalid_request("#{label} must be a non-empty string")}

  defp call_optional_port(%BackendContext{} = context, family, fun)
       when is_atom(family) and is_function(fun, 1) do
    if Access.available?(context, family) do
      fun.(Access.port(context, family))
    else
      {:error,
       ElixirDB.Error.invalid_request("storage backend does not implement the #{family} port")}
    end
  end

  defp with_port(%BackendContext{} = context, family, fun)
       when is_atom(family) and is_function(fun, 0) do
    with_ports(context, [family], fun)
  end

  defp with_ports(%BackendContext{} = context, families, fun)
       when is_list(families) and is_function(fun, 0) do
    case Enum.find(families, &(not Access.available?(context, &1))) do
      nil -> fun.()
      family -> unsupported_port(family)
    end
  end

  defp unsupported_port(family),
    do:
      {:error,
       ElixirDB.Error.invalid_request("storage backend does not implement the #{family} port")}

  defp put_retention_fault(%BackendContext{} = context, fun) when is_function(fun, 1) do
    %{context | identity: Map.put(context.identity || %{}, :retention_fault, fun)}
  end

  defp put_retention_fault(%BackendContext{} = context, _), do: context
end
