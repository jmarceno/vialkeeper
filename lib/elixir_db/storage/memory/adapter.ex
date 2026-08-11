defmodule ElixirDB.Storage.Memory.Adapter do
  @moduledoc """
  In-memory storage backend for shared mutation, import, and chain semantic tests.

  Implements lifecycle and service entry points against Memory ports.
  Unsupported callbacks return typed errors.
  """
  @behaviour ElixirDB.Storage.Adapter

  alias ElixirDB.MapAccess
  alias ElixirDB.Revisions.Winner
  alias ElixirDB.Storage.BackendContext
  alias ElixirDB.Storage.RequestValidation

  alias ElixirDB.Storage.Memory.{
    AttachmentMetadata,
    ChangeLog,
    DocumentFacts,
    IndexCandidates,
    RetentionRecords,
    Store,
    Transaction
  }

  alias ElixirDB.Storage.OpaqueHandle
  alias ElixirDB.Storage.Services

  defstruct [:root, :store, :identity, closed?: false]

  @type t :: %__MODULE__{
          root: binary(),
          store: pid(),
          identity: map(),
          closed?: boolean()
        }

  @unsupported [
    {:update_config, 2},
    {:integrity_check, 2},
    {:get_local_record, 3},
    {:put_local_record_cas, 2},
    {:retention_state, 1},
    {:list_peer_positions, 1},
    {:put_peer_position_cas, 2},
    {:read_boundary_pages, 2},
    {:install_boundary_pages, 2},
    {:compact_retention, 2},
    {:list_replication_jobs, 1},
    {:put_replication_job, 2},
    {:delete_replication_job, 2},
    {:create_index, 2},
    {:delete_index, 2},
    {:rebuild_index, 2},
    {:list_indexes, 1},
    {:execute_query, 2},
    {:execute_subscription_snapshot, 2},
    {:get_revisions_batch, 2},
    {:explain_query, 2},
    {:resolve_attachment_ticket, 2},
    {:resolve_blob_metadata, 2},
    {:list_live_attachment_digests, 2},
    {:cleanup_expired_pending_blobs, 2},
    {:list_views, 1},
    {:create_view, 2},
    {:delete_view, 2},
    {:view_state, 2},
    {:apply_view_batch, 2},
    {:begin_view_rebuild, 2},
    {:append_view_rebuild_page, 2},
    {:finish_view_rebuild, 2},
    {:query_view, 2},
    {:read_winning_documents_page, 2},
    {:get_derived_view, 1},
    {:set_derived_enabled, 2},
    {:list_derived_sources, 1},
    {:set_derived_source_error, 2},
    {:apply_derived_source_batch, 2},
    {:begin_derived_source_rebuild, 2},
    {:apply_derived_rebuild_page, 2},
    {:prune_derived_rebuild_stale_page, 2},
    {:finish_derived_source_rebuild, 2}
  ]

  for {name, arity} <- @unsupported do
    args = Macro.generate_arguments(arity, __MODULE__)

    @impl true
    def unquote(name)(unquote_splicing(args)) do
      unsupported(unquote(name))
    end
  end

  @impl true
  def create(path, options \\ %{}) when is_binary(path) do
    options = if is_map(options), do: options, else: %{}
    uuid = MapAccess.get(options, :database_uuid, ElixirDB.UUID.v4())
    config = MapAccess.get(options, :config, ElixirDB.Config.defaults())

    with :ok <- validate_uuid(uuid),
         {:ok, bounded_config} <- ElixirDB.Config.merge_and_bound(config),
         :ok <- File.mkdir_p(path),
         identity <- build_identity(uuid, bounded_config, options),
         {:ok, store} <- Store.start_link(root: path, identity: identity) do
      {:ok, %__MODULE__{root: Path.expand(path), store: store, identity: identity}}
    end
  end

  @impl true
  def open(path, _options \\ %{}) when is_binary(path) do
    with {:ok, store} <- Store.open(path) do
      identity = Store.identity(store)
      {:ok, %__MODULE__{root: Path.expand(path), store: store, identity: identity}}
    end
  end

  @impl true
  def close(%__MODULE__{store: store} = adapter) do
    case Store.close(store) do
      :ok -> :ok
      {:error, _} = error -> error
    end
  after
    _ = adapter
  end

  @impl true
  def identity(%__MODULE__{store: store}) do
    {:ok, Store.identity(store)}
  end

  @impl true
  def get_document(%__MODULE__{} = adapter, request) when is_map(request) do
    document_id = MapAccess.get(request, :document_id)
    context = to_context(adapter)

    with {:ok, doc} <- DocumentFacts.find_document(context, document_id),
         {:ok, doc} <- require_document(doc),
         {:ok, revision} <-
           DocumentFacts.find_revision(context, document_id, doc.winning_revision),
         {:ok, leaves} <- DocumentFacts.list_leaves(context, document_id) do
      {:ok, document_result(doc, revision, leaves)}
    else
      :missing_document ->
        {:error, ElixirDB.Error.document_not_found("document does not exist")}

      {:error, error} ->
        {:error, error}
    end
  end

  def get_document(_adapter, _request),
    do: {:error, ElixirDB.Error.invalid_request("document request must be an object")}

  @impl true
  def get_revision(%__MODULE__{} = adapter, request) when is_map(request) do
    document_id = MapAccess.get(request, :document_id)
    revision_id = MapAccess.get(request, :revision_id)
    context = to_context(adapter)

    with {:ok, doc} <- DocumentFacts.find_document(context, document_id),
         {:ok, doc} <- require_document(doc),
         {:ok, revision} <- DocumentFacts.find_revision(context, document_id, revision_id) do
      {:ok, document_result(doc, revision, [])}
    else
      :missing_document ->
        {:error, ElixirDB.Error.document_not_found("document does not exist")}

      {:error, error} ->
        {:error, error}
    end
  end

  def get_revision(_adapter, _request),
    do: {:error, ElixirDB.Error.invalid_request("revision request must be an object")}

  @impl true
  def apply_local_mutation(%__MODULE__{} = adapter, request) when is_map(request) do
    Services.apply_local_mutation(to_context(adapter), request)
  end

  def apply_local_mutation(_adapter, _request),
    do: {:error, ElixirDB.Error.invalid_request("mutation request must be an object")}

  @impl true
  def apply_bulk_mutation(%__MODULE__{} = adapter, request) when is_map(request) do
    Services.apply_bulk_mutation(to_context(adapter), request)
  end

  def apply_bulk_mutation(_adapter, _request),
    do: {:error, ElixirDB.Error.invalid_request("bulk mutation request must be an object")}

  @impl true
  def resolve_conflict(%__MODULE__{} = adapter, request) when is_map(request) do
    Services.resolve_conflict(to_context(adapter), request)
  end

  def resolve_conflict(_adapter, _request),
    do: {:error, ElixirDB.Error.invalid_request("conflict request must be an object")}

  @impl true
  def read_changes(%__MODULE__{} = adapter, request) when is_map(request) do
    since = MapAccess.get(request, :since, 0)
    limit = MapAccess.get(request, :limit, 100)

    with :ok <- validate_non_negative_integer(since, "since"),
         :ok <- validate_positive_integer(limit, "limit"),
         {:ok, identity} <- identity(adapter),
         :ok <- validate_changes_since_floor(since, identity),
         :ok <-
           validate_changes_limit(
             limit,
             get_in(identity, [:config, "changes", "max_batch"])
           ) do
      ChangeLog.read_page(to_context(adapter), since, limit)
    end
  end

  def read_changes(_adapter, _request),
    do: {:error, ElixirDB.Error.invalid_request("changes request must be an object")}

  @impl true
  def has_local_origin_changes?(%__MODULE__{} = adapter),
    do: ChangeLog.has_local_origin_changes?(to_context(adapter))

  @impl true
  def has_local_origin_changes?(%__MODULE__{} = adapter, peer),
    do: ChangeLog.has_local_origin_changes?(to_context(adapter), peer)

  @impl true
  def clear_pending_local_causal(%__MODULE__{} = adapter),
    do: ChangeLog.clear_pending_local_causal(to_context(adapter))

  @impl true
  def clear_pending_local_causal(%__MODULE__{} = adapter, peer),
    do: ChangeLog.clear_pending_local_causal(to_context(adapter), peer)

  @impl true
  def diff_revisions(%__MODULE__{} = adapter, request) when is_map(request),
    do: Services.diff_revisions(to_context(adapter), request)

  def diff_revisions(_adapter, _request),
    do: {:error, ElixirDB.Error.invalid_request("revision diff request must be an object")}

  @impl true
  def get_revision_chains(%__MODULE__{} = adapter, request) when is_map(request),
    do: Services.get_revision_chains(to_context(adapter), request)

  def get_revision_chains(_adapter, _request),
    do: {:error, ElixirDB.Error.invalid_request("revision chain request must be an object")}

  @impl true
  def import_revision_chains(%__MODULE__{} = adapter, request) when is_map(request),
    do: Services.import_revision_chains(to_context(adapter), request)

  def import_revision_chains(_adapter, _request),
    do: {:error, ElixirDB.Error.invalid_request("revision import request must be an object")}

  @impl true
  def protect_pending_blob(%__MODULE__{} = adapter, request) when is_map(request),
    do: AttachmentMetadata.protect_pending_blob(to_context(adapter), request)

  @impl true
  def remove_pending_blob_protection(%__MODULE__{} = adapter, request) when is_map(request),
    do: AttachmentMetadata.remove_pending_blob_protection(to_context(adapter), request)

  @doc "Wraps an open Memory adapter in an opaque backend context."
  @spec to_context(t()) :: BackendContext.t()
  def to_context(%__MODULE__{} = adapter) do
    BackendContext.new(
      backend: __MODULE__,
      backend_ref: OpaqueHandle.wrap(adapter),
      bundle_root: adapter.root,
      capabilities: %{sql: false, engine: "memory"},
      identity: Store.identity(adapter.store)
    )
  end

  @doc "Returns the Memory module that implements `family`."
  @spec port(atom()) :: module()
  def port(family) when is_atom(family), do: Map.fetch!(port_modules(), family)

  @doc "Returns the Memory port composition map."
  @spec port_modules() :: %{atom() => module()}
  def port_modules do
    %{
      transaction: Transaction,
      document_facts: DocumentFacts,
      change_log: ChangeLog,
      retention_records: RetentionRecords,
      index_candidates: IndexCandidates,
      attachment_metadata: AttachmentMetadata
    }
  end

  @doc "Transaction port entry used by `ElixirDB.Storage.Transaction.run/2`."
  @spec run_transaction(BackendContext.t(), (BackendContext.t() -> term())) ::
          {:ok, term()} | {:error, ElixirDB.Error.t()}
  def run_transaction(%BackendContext{} = context, fun) when is_function(fun, 1) do
    Transaction.run(context, fun)
  end

  @doc "Returns the Memory transaction port module."
  @spec transaction_port() :: module()
  def transaction_port, do: Transaction

  defp build_identity(uuid, config, options) do
    %{
      database_uuid: uuid,
      database_kind: MapAccess.get(options, :database_kind, :ordinary),
      history_epoch: ElixirDB.UUID.v4(),
      file_format_version: 1,
      logical_schema_version: 1,
      revision_algorithm_version: 1,
      canonicalization_version: 1,
      replication_protocol_major: 1,
      current_sequence: 0,
      retention_floor_sequence: 0,
      compaction_epoch: 0,
      retention_boundary_digest: nil,
      config: config,
      backend: "memory",
      engine: "memory"
    }
  end

  defp require_document(nil), do: :missing_document
  defp require_document(doc), do: {:ok, doc}

  defp document_result(doc, revision, leaves) do
    result = %{
      id: doc.document_id,
      revision: revision.revision_id,
      deleted: revision.deleted,
      body: revision.body,
      sequence: doc.update_sequence,
      attachments: revision.attachments || %{}
    }

    if leaves == [],
      do: result,
      else: Map.put(result, :conflicts, Winner.conflicts(leaves, revision))
  end

  defp validate_uuid(uuid), do: RequestValidation.validate_uuid(uuid)

  defp validate_non_negative_integer(value, label),
    do: RequestValidation.validate_non_negative_integer(value, label)

  defp validate_positive_integer(value, label),
    do: RequestValidation.validate_positive_integer(value, label)

  defp validate_changes_limit(limit, database_max),
    do: RequestValidation.validate_changes_limit(limit, database_max)

  defp validate_changes_since_floor(since, identity),
    do: RequestValidation.validate_changes_since_floor(since, identity)

  defp unsupported(operation) do
    {:error, ElixirDB.Error.invalid_request("memory backend does not implement #{operation}")}
  end
end
