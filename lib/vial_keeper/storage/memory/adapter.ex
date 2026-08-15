defmodule VialKeeper.Storage.Memory.Adapter do
  @moduledoc """
  In-memory storage backend for shared mutation, import, and chain semantic tests.

  Implements lifecycle and fact ports. High-level mutation/import entry points
  are thin wrappers over `VialKeeper.Storage.Services`, not Adapter behaviour
  callbacks. Unsupported Adapter callbacks return typed errors.
  """
  @behaviour VialKeeper.Storage.Adapter
  use VialKeeper.Storage.AdapterFacade

  alias VialKeeper.MapAccess
  alias VialKeeper.Query.{Normalizer, SubscriptionRequest}
  alias VialKeeper.Storage.BackendContext
  alias VialKeeper.Storage.RequestValidation
  alias VialKeeper.Storage.Results

  alias VialKeeper.Storage.Memory.{
    AttachmentMetadata,
    ChangeLog,
    DerivedState,
    DocumentFacts,
    IndexCandidates,
    Inspection,
    Lifecycle,
    RetentionRecords,
    ShadowState,
    Store,
    Transaction,
    ViewState
  }

  alias VialKeeper.Storage.OpaqueHandle
  alias VialKeeper.Storage.Services

  defstruct [
    :root,
    :store,
    :identity,
    closed?: false,
    retention_fault: nil,
    view_fault: nil,
    derived_fault: nil
  ]

  @type fault_fn :: (atom() -> :ok | {:error, VialKeeper.Error.t()}) | nil

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
    uuid = MapAccess.get(options, :database_uuid, VialKeeper.UUID.v4())
    config = MapAccess.get(options, :config, VialKeeper.Config.defaults())
    initial_derived = MapAccess.get(options, :initial_derived_view)
    shadow_metadata = MapAccess.get(options, :shadow_metadata)

    with :ok <- RequestValidation.validate_uuid(uuid),
         {:ok, bounded_config} <- VialKeeper.Config.merge_and_bound(config),
         :ok <- File.mkdir_p(path),
         identity <- build_identity(uuid, bounded_config, options),
         {:ok, store} <- Store.start_link(root: path, identity: identity),
         :ok <- maybe_seed_derived(store, identity, initial_derived),
         :ok <- maybe_seed_shadow(store, identity, shadow_metadata) do
      {:ok, %__MODULE__{root: Path.expand(path), store: store, identity: identity}}
    end
  end

  @impl true
  def open(path, _options \\ %{}) when is_binary(path) do
    with {:ok, store} <- Store.open(path) do
      identity = Store.identity(store)

      case validate_shadow_state(identity, Store.get(store)) do
        :ok ->
          {:ok, %__MODULE__{root: Path.expand(path), store: store, identity: identity}}

        {:error, _} = error ->
          _ = Store.close(store)
          error
      end
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
    with {:ok, bounded} <- VialKeeper.Config.merge_and_bound(config) do
      Store.update(store, fn state ->
        identity = Map.put(state.identity, :config, bounded)
        {:ok, %{state | identity: identity}, bounded}
      end)
    end
  end

  def update_config(_adapter, _config),
    do: {:error, VialKeeper.Error.invalid_request("config must be an object")}

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
      {:ok, Results.document_map(doc, revision, leaves)}
    else
      :missing_document ->
        {:error, VialKeeper.Error.document_not_found("document does not exist")}

      {:error, error} ->
        {:error, error}
    end
  end

  def get_document(_adapter, _request),
    do: {:error, VialKeeper.Error.invalid_request("document request must be an object")}

  @impl true
  def get_revision(%__MODULE__{} = adapter, request) when is_map(request) do
    document_id = MapAccess.get(request, :document_id)
    revision_id = MapAccess.get(request, :revision_id)
    context = to_context(adapter)

    with {:ok, doc} <- DocumentFacts.find_document(context, document_id),
         {:ok, doc} <- require_document(doc),
         {:ok, revision} <- DocumentFacts.find_revision(context, document_id, revision_id) do
      {:ok, Results.document_map(doc, revision, [])}
    else
      :missing_document ->
        {:error, VialKeeper.Error.document_not_found("document does not exist")}

      {:error, error} ->
        {:error, error}
    end
  end

  def get_revision(_adapter, _request),
    do: {:error, VialKeeper.Error.invalid_request("revision request must be an object")}

  @doc "Applies a local put/delete via `VialKeeper.Storage.Services` (not an Adapter callback)."
  def apply_local_mutation(%__MODULE__{} = adapter, request) when is_map(request) do
    Services.apply_local_mutation(to_context(adapter), request)
  end

  def apply_local_mutation(_adapter, _request),
    do: {:error, VialKeeper.Error.invalid_request("mutation request must be an object")}

  @doc "Applies a bulk mutation batch via `VialKeeper.Storage.Services` (not an Adapter callback)."
  def apply_bulk_mutation(%__MODULE__{} = adapter, request) when is_map(request) do
    Services.apply_bulk_mutation(to_context(adapter), request)
  end

  def apply_bulk_mutation(_adapter, _request),
    do: {:error, VialKeeper.Error.invalid_request("bulk mutation request must be an object")}

  @doc "Resolves a conflict via `VialKeeper.Storage.Services` (not an Adapter callback)."
  def resolve_conflict(%__MODULE__{} = adapter, request) when is_map(request) do
    Services.resolve_conflict(to_context(adapter), request)
  end

  def resolve_conflict(_adapter, _request),
    do: {:error, VialKeeper.Error.invalid_request("conflict request must be an object")}

  @impl true
  def read_changes(%__MODULE__{} = adapter, request) when is_map(request) do
    since = MapAccess.get(request, :since, 0)
    limit = MapAccess.get(request, :limit, 100)

    with :ok <- RequestValidation.validate_non_negative_integer(since, "since"),
         :ok <- RequestValidation.validate_positive_integer(limit, "limit"),
         {:ok, identity} <- identity(adapter),
         :ok <- RequestValidation.validate_changes_since_floor(since, identity),
         :ok <-
           RequestValidation.validate_changes_limit(
             limit,
             get_in(identity, [:config, "changes", "max_batch"])
           ) do
      ChangeLog.read_page(to_context(adapter), since, limit)
    end
  end

  def read_changes(_adapter, _request),
    do: {:error, VialKeeper.Error.invalid_request("changes request must be an object")}

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

  @doc "Imports revision chains via `VialKeeper.Storage.Services` (not an Adapter callback)."
  def import_revision_chains(%__MODULE__{} = adapter, request) when is_map(request),
    do: Services.import_revision_chains(to_context(adapter), request)

  def import_revision_chains(_adapter, _request),
    do: {:error, VialKeeper.Error.invalid_request("revision import request must be an object")}

  @impl true
  def execute_query(%__MODULE__{} = adapter, request) when is_map(request) do
    with {:ok, identity} <- identity(adapter),
         {:ok, normalized} <- prepare_query_request(request) do
      Services.execute_query(to_context(adapter), normalized, identity)
    end
  end

  def execute_query(_adapter, _request),
    do: {:error, VialKeeper.Error.invalid_request("query must be an object")}

  @impl true
  def execute_subscription_snapshot(%__MODULE__{} = adapter, request) when is_map(request) do
    with {:ok, identity} <- identity(adapter),
         {:ok, normalized} <-
           SubscriptionRequest.prepare_snapshot(request, Map.get(identity, :config, %{})) do
      Services.execute_subscription_snapshot(to_context(adapter), normalized, identity)
    end
  end

  def execute_subscription_snapshot(_adapter, _request),
    do:
      {:error, VialKeeper.Error.invalid_request("subscription snapshot request must be an object")}

  @impl true
  def explain_query(%__MODULE__{} = adapter, request) when is_map(request) do
    with {:ok, identity} <- identity(adapter),
         {:ok, normalized} <- prepare_query_request(request) do
      Services.explain_query(to_context(adapter), normalized, identity)
    end
  end

  def explain_query(_adapter, _request),
    do: {:error, VialKeeper.Error.invalid_request("query explanation must be an object")}

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
      capabilities: %{engine: "memory"},
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
      shadow_state: ShadowState,
      retention_records: RetentionRecords,
      index_candidates: IndexCandidates,
      view_state: ViewState,
      derived_state: DerivedState,
      attachment_metadata: AttachmentMetadata,
      inspection: Inspection
    }
  end

  @doc "Transaction port entry used by `VialKeeper.Storage.Transaction.run/2`."
  @spec run_transaction(BackendContext.t(), (BackendContext.t() -> term())) ::
          {:ok, term()} | {:error, VialKeeper.Error.t()}
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
      history_epoch: VialKeeper.UUID.v4(),
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

  defp unsupported(operation) do
    {:error, VialKeeper.Error.invalid_request("memory backend does not implement #{operation}")}
  end

  defp maybe_seed_derived(_store, %{database_kind: :derived}, nil),
    do: {:error, VialKeeper.Error.invalid_request("derived metadata is required")}

  defp maybe_seed_derived(store, _identity, initial) when is_map(initial) do
    case Store.update(store, &seed_derived_state(&1, initial)) do
      {:ok, :ok} -> :ok
      {:error, _} = error -> error
    end
  end

  defp maybe_seed_derived(_store, _identity, _), do: :ok

  defp maybe_seed_shadow(_store, %{database_kind: kind}, nil) when kind != :shadow, do: :ok

  defp maybe_seed_shadow(_store, %{database_kind: :shadow}, nil),
    do: {:error, VialKeeper.Error.invalid_request("shadow metadata is required")}

  defp maybe_seed_shadow(store, %{database_kind: :shadow}, metadata) when is_map(metadata) do
    state = Store.get(store)

    case validate_shadow_state(state.identity, %{state | shadow_metadata: metadata}) do
      :ok ->
        Store.update(store, fn next_state ->
          {:ok, %{next_state | shadow_metadata: metadata}, :ok}
        end)
        |> normalize_seed_result()

      {:error, _} = error ->
        error
    end
  end

  defp maybe_seed_shadow(_store, _identity, _metadata), do: :ok

  defp normalize_seed_result({:ok, :ok}), do: :ok
  defp normalize_seed_result({:error, _} = error), do: error

  defp validate_shadow_state(%{database_kind: :shadow, database_uuid: database_uuid}, state) do
    case state.shadow_metadata do
      %{source_database_uuid: source_uuid, shadow_database_uuid: ^database_uuid} = metadata
      when source_uuid != database_uuid ->
        if valid_shadow_metadata?(metadata), do: :ok, else: invalid_shadow_metadata()

      _ ->
        invalid_shadow_metadata()
    end
  end

  defp validate_shadow_state(_identity, _state), do: :ok

  defp valid_shadow_metadata?(metadata) do
    is_binary(metadata[:source_database_uuid]) and
      is_binary(metadata[:operation_id]) and
      is_integer(metadata[:generation]) and
      metadata[:generation] > 0 and
      metadata[:attachment_store_type] == "external_cas" and
      is_binary(metadata[:attachment_location]) and
      Path.type(metadata[:attachment_location]) == :absolute and
      is_binary(metadata[:specification_digest]) and
      is_binary(metadata[:created_at])
  end

  defp invalid_shadow_metadata,
    do: {:error, VialKeeper.Error.unsupported_format("shadow database metadata is invalid")}

  defp seed_derived_state(state, initial) do
    case DerivedState.seed(state, initial) do
      {:ok, seeded} -> {:ok, seeded, :ok}
      {:error, _} = error -> error
    end
  end
end
