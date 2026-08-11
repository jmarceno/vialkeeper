defmodule ElixirDB.Storage.Memory.Adapter do
  @moduledoc """
  In-memory storage backend for shared mutation, import, and chain semantic tests.

  Implements lifecycle and fact ports. High-level mutation/import entry points
  are thin wrappers over `ElixirDB.Storage.Services`, not Adapter behaviour
  callbacks. Unsupported Adapter callbacks return typed errors.
  """
  @behaviour ElixirDB.Storage.Adapter

  alias ElixirDB.MapAccess
  alias ElixirDB.Query.{Normalizer, SubscriptionRequest}
  alias ElixirDB.Revisions.Winner
  alias ElixirDB.Storage.BackendContext
  alias ElixirDB.Storage.RequestValidation

  alias ElixirDB.Storage.Memory.{
    AttachmentMetadata,
    ChangeLog,
    DerivedState,
    DocumentFacts,
    IndexCandidates,
    Inspection,
    Lifecycle,
    RetentionRecords,
    Store,
    Transaction,
    ViewState
  }

  alias ElixirDB.Storage.OpaqueHandle
  alias ElixirDB.Storage.Services

  defstruct [
    :root,
    :store,
    :identity,
    closed?: false,
    retention_fault: nil,
    view_fault: nil,
    derived_fault: nil
  ]

  @type fault_fn :: (atom() -> :ok | {:error, ElixirDB.Error.t()}) | nil

  @type t :: %__MODULE__{
          root: binary(),
          store: pid(),
          identity: map(),
          closed?: boolean(),
          retention_fault: fault_fn(),
          view_fault: fault_fn(),
          derived_fault: fault_fn()
        }

  @unsupported [
    {:get_local_record, 3},
    {:put_local_record_cas, 2},
    {:list_replication_jobs, 1},
    {:put_replication_job, 2},
    {:delete_replication_job, 2},
    {:create_index, 2},
    {:delete_index, 2},
    {:rebuild_index, 2},
    {:list_indexes, 1},
    {:get_revisions_batch, 2}
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
    initial_derived = MapAccess.get(options, :initial_derived_view)

    with :ok <- validate_uuid(uuid),
         {:ok, bounded_config} <- ElixirDB.Config.merge_and_bound(config),
         :ok <- File.mkdir_p(path),
         identity <- build_identity(uuid, bounded_config, options),
         {:ok, store} <- Store.start_link(root: path, identity: identity),
         :ok <- maybe_seed_derived(store, identity, initial_derived) do
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
  def update_config(%__MODULE__{store: store}, config) when is_map(config) do
    with {:ok, bounded} <- ElixirDB.Config.merge_and_bound(config) do
      Store.update(store, fn state ->
        identity = Map.put(state.identity, :config, bounded)
        {:ok, %{state | identity: identity}, bounded}
      end)
    end
  end

  def update_config(_adapter, _config),
    do: {:error, ElixirDB.Error.invalid_request("config must be an object")}

  @impl true
  def integrity_check(%__MODULE__{} = adapter, options \\ %{}) when is_map(options) do
    Services.Integrity.check(to_context(adapter), options)
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

  @doc "Applies a local put/delete via `ElixirDB.Storage.Services` (not an Adapter callback)."
  def apply_local_mutation(%__MODULE__{} = adapter, request) when is_map(request) do
    Services.apply_local_mutation(to_context(adapter), request)
  end

  def apply_local_mutation(_adapter, _request),
    do: {:error, ElixirDB.Error.invalid_request("mutation request must be an object")}

  @doc "Applies a bulk mutation batch via `ElixirDB.Storage.Services` (not an Adapter callback)."
  def apply_bulk_mutation(%__MODULE__{} = adapter, request) when is_map(request) do
    Services.apply_bulk_mutation(to_context(adapter), request)
  end

  def apply_bulk_mutation(_adapter, _request),
    do: {:error, ElixirDB.Error.invalid_request("bulk mutation request must be an object")}

  @doc "Resolves a conflict via `ElixirDB.Storage.Services` (not an Adapter callback)."
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
  def retention_state(%__MODULE__{} = adapter),
    do: Services.retention_state(to_context(adapter))

  @impl true
  def list_peer_positions(%__MODULE__{} = adapter),
    do: Services.list_peer_positions(to_context(adapter))

  @impl true
  def put_peer_position_cas(%__MODULE__{} = adapter, request) when is_map(request),
    do: Services.put_peer_position_cas(to_context(adapter), request)

  def put_peer_position_cas(_adapter, _request),
    do: {:error, ElixirDB.Error.invalid_request("peer position request must be an object")}

  @impl true
  def read_boundary_pages(%__MODULE__{} = adapter, request) when is_map(request),
    do: Services.read_boundary_pages(to_context(adapter), request)

  def read_boundary_pages(_adapter, _request),
    do: {:error, ElixirDB.Error.invalid_request("boundary page request must be an object")}

  @impl true
  def install_boundary_pages(%__MODULE__{} = adapter, request) when is_map(request),
    do: Services.install_boundary_pages(to_context(adapter), request)

  def install_boundary_pages(_adapter, _request),
    do: {:error, ElixirDB.Error.invalid_request("boundary page request must be an object")}

  @impl true
  def compact_retention(%__MODULE__{} = adapter, request \\ %{}) when is_map(request),
    do: Services.compact_retention(to_context(adapter), request)

  @doc "Diffs revision leaves via `ElixirDB.Storage.Services` (not an Adapter callback)."
  def diff_revisions(%__MODULE__{} = adapter, request) when is_map(request),
    do: Services.diff_revisions(to_context(adapter), request)

  def diff_revisions(_adapter, _request),
    do: {:error, ElixirDB.Error.invalid_request("revision diff request must be an object")}

  @doc "Loads revision chains via `ElixirDB.Storage.Services` (not an Adapter callback)."
  def get_revision_chains(%__MODULE__{} = adapter, request) when is_map(request),
    do: Services.get_revision_chains(to_context(adapter), request)

  def get_revision_chains(_adapter, _request),
    do: {:error, ElixirDB.Error.invalid_request("revision chain request must be an object")}

  @doc "Imports revision chains via `ElixirDB.Storage.Services` (not an Adapter callback)."
  def import_revision_chains(%__MODULE__{} = adapter, request) when is_map(request),
    do: Services.import_revision_chains(to_context(adapter), request)

  def import_revision_chains(_adapter, _request),
    do: {:error, ElixirDB.Error.invalid_request("revision import request must be an object")}

  @impl true
  def protect_pending_blob(%__MODULE__{} = adapter, request) when is_map(request),
    do: Services.protect_pending_blob(to_context(adapter), request)

  @impl true
  def remove_pending_blob_protection(%__MODULE__{} = adapter, request) when is_map(request),
    do: Services.remove_pending_blob_protection(to_context(adapter), request)

  @impl true
  def resolve_attachment_ticket(%__MODULE__{} = adapter, request) when is_map(request),
    do: Services.resolve_attachment_ticket(to_context(adapter), request)

  def resolve_attachment_ticket(_adapter, _request),
    do: {:error, ElixirDB.Error.invalid_request("attachment ticket request must be an object")}

  @impl true
  def resolve_blob_metadata(%__MODULE__{} = adapter, request) when is_map(request),
    do: Services.resolve_blob_metadata(to_context(adapter), request)

  def resolve_blob_metadata(_adapter, _request),
    do: {:error, ElixirDB.Error.invalid_request("blob metadata request must be an object")}

  @impl true
  def list_live_attachment_digests(%__MODULE__{} = adapter, request) when is_map(request),
    do: Services.list_live_attachment_digests(to_context(adapter), request)

  def list_live_attachment_digests(_adapter, _request),
    do: {:error, ElixirDB.Error.invalid_request("live attachment digest request must be an object")}

  @impl true
  def cleanup_expired_pending_blobs(%__MODULE__{} = adapter, request \\ %{})
      when is_map(request) do
    Services.cleanup_expired_pending_blobs(to_context(adapter), request)
  end

  @impl true
  def execute_query(%__MODULE__{} = adapter, request) when is_map(request) do
    with {:ok, identity} <- identity(adapter),
         {:ok, normalized} <- prepare_query_request(request) do
      Services.execute_query(to_context(adapter), normalized, identity)
    end
  end

  def execute_query(_adapter, _request),
    do: {:error, ElixirDB.Error.invalid_request("query must be an object")}

  @impl true
  def execute_subscription_snapshot(%__MODULE__{} = adapter, request) when is_map(request) do
    with {:ok, identity} <- identity(adapter),
         {:ok, normalized} <-
           SubscriptionRequest.prepare_snapshot(request, Map.get(identity, :config, %{})) do
      Services.execute_subscription_snapshot(to_context(adapter), normalized, identity)
    end
  end

  def execute_subscription_snapshot(_adapter, _request),
    do: {:error, ElixirDB.Error.invalid_request("subscription snapshot request must be an object")}

  @impl true
  def explain_query(%__MODULE__{} = adapter, request) when is_map(request) do
    with {:ok, identity} <- identity(adapter),
         {:ok, normalized} <- prepare_query_request(request) do
      Services.explain_query(to_context(adapter), normalized, identity)
    end
  end

  def explain_query(_adapter, _request),
    do: {:error, ElixirDB.Error.invalid_request("query explanation must be an object")}

  @impl true
  def list_views(%__MODULE__{} = adapter), do: Services.list_views(to_context(adapter))

  @impl true
  def create_view(%__MODULE__{} = adapter, definition),
    do: Services.create_view(to_context(adapter), definition)

  @impl true
  def delete_view(%__MODULE__{} = adapter, view_id),
    do: Services.delete_view(to_context(adapter), view_id)

  @impl true
  def view_state(%__MODULE__{} = adapter, view_id),
    do: Services.view_state(to_context(adapter), view_id)

  @impl true
  def apply_view_batch(%__MODULE__{} = adapter, request),
    do: Services.apply_view_batch(to_context(adapter), request)

  @impl true
  def begin_view_rebuild(%__MODULE__{} = adapter, request),
    do: Services.begin_view_rebuild(to_context(adapter), request)

  @impl true
  def append_view_rebuild_page(%__MODULE__{} = adapter, request),
    do: Services.append_view_rebuild_page(to_context(adapter), request)

  @impl true
  def finish_view_rebuild(%__MODULE__{} = adapter, request),
    do: Services.finish_view_rebuild(to_context(adapter), request)

  @impl true
  def query_view(%__MODULE__{} = adapter, request),
    do: Services.query_view(to_context(adapter), request)

  @impl true
  def read_winning_documents_page(%__MODULE__{} = adapter, request),
    do: Services.read_winning_documents_page(to_context(adapter), request)

  @impl true
  def get_derived_view(%__MODULE__{} = adapter),
    do: Services.get_derived_view(to_context(adapter))

  @impl true
  def set_derived_enabled(%__MODULE__{} = adapter, request),
    do: Services.set_derived_enabled(to_context(adapter), request)

  @impl true
  def list_derived_sources(%__MODULE__{} = adapter),
    do: Services.list_derived_sources(to_context(adapter))

  @impl true
  def set_derived_source_error(%__MODULE__{} = adapter, request),
    do: Services.set_derived_source_error(to_context(adapter), request)

  @impl true
  def apply_derived_source_batch(%__MODULE__{} = adapter, request),
    do: Services.apply_derived_source_batch(to_context(adapter), request)

  @impl true
  def begin_derived_source_rebuild(%__MODULE__{} = adapter, request),
    do: Services.begin_derived_source_rebuild(to_context(adapter), request)

  @impl true
  def apply_derived_rebuild_page(%__MODULE__{} = adapter, request),
    do: Services.apply_derived_rebuild_page(to_context(adapter), request)

  @impl true
  def prune_derived_rebuild_stale_page(%__MODULE__{} = adapter, request),
    do: Services.prune_derived_rebuild_stale_page(to_context(adapter), request)

  @impl true
  def finish_derived_source_rebuild(%__MODULE__{} = adapter, request),
    do: Services.finish_derived_source_rebuild(to_context(adapter), request)

  @doc "Wraps an open Memory adapter in an opaque backend context."
  @spec to_context(t()) :: BackendContext.t()
  def to_context(%__MODULE__{} = adapter) do
    identity =
      adapter.store
      |> Store.identity()
      |> maybe_put_fault(:retention_fault, adapter.retention_fault)
      |> maybe_put_fault(:view_fault, adapter.view_fault)
      |> maybe_put_fault(:derived_fault, adapter.derived_fault)

    BackendContext.new(
      backend: __MODULE__,
      backend_ref: OpaqueHandle.wrap(adapter),
      bundle_root: adapter.root,
      capabilities: %{sql: false, engine: "memory"},
      identity: identity
    )
  end

  defp maybe_put_fault(identity, _key, nil), do: identity
  defp maybe_put_fault(identity, key, fun) when is_function(fun, 1), do: Map.put(identity, key, fun)

  defp prepare_query_request(request), do: Normalizer.normalize_public_request(request)

  @doc "Returns the Memory module that implements `family`."
  @spec port(atom()) :: module()
  def port(family) when is_atom(family), do: Map.fetch!(port_modules(), family)

  @doc "Returns the Memory port composition map."
  @spec port_modules() :: %{atom() => module()}
  def port_modules do
    %{
      lifecycle: Lifecycle,
      transaction: Transaction,
      document_facts: DocumentFacts,
      change_log: ChangeLog,
      retention_records: RetentionRecords,
      index_candidates: IndexCandidates,
      view_state: ViewState,
      derived_state: DerivedState,
      attachment_metadata: AttachmentMetadata,
      inspection: Inspection
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

  defp maybe_seed_derived(_store, %{database_kind: :derived}, nil),
    do: {:error, ElixirDB.Error.invalid_request("derived metadata is required")}

  defp maybe_seed_derived(store, _identity, initial) when is_map(initial) do
    case Store.update(store, &seed_derived_state(&1, initial)) do
      {:ok, :ok} -> :ok
      {:error, _} = error -> error
    end
  end

  defp maybe_seed_derived(_store, _identity, _), do: :ok

  defp seed_derived_state(state, initial) do
    case DerivedState.seed(state, initial) do
      {:ok, seeded} -> {:ok, seeded, :ok}
      {:error, _} = error -> error
    end
  end
end
