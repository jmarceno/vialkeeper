defmodule ElixirDB.Storage.SQLite.Adapter do
  @moduledoc """
  Version 1 SQLite storage adapter and port composition facade.

  Satisfies `ElixirDB.Storage.Adapter` for Waves 3–6 compatibility while
  composing storage ports:

  * `Lifecycle` / `Transaction` / `OwnershipPort`
  * `DocumentFacts` / `ChangeLog` / `LocalRecordsPort` / `RetentionRecordsPort`
  * `IndexCandidates` / `ViewStatePort` / `DerivedStatePort`
  * `AttachmentMetadataPort` / `InspectionPort`

  Per-concern SQL lives in sibling SQLite modules. Engine handles, SQL, and
  transaction text stay inside `ElixirDB.Storage.SQLite.*`.
  """
  @behaviour ElixirDB.Storage.Adapter

  alias ElixirDB.JSON.{Canonical, StrictCache, StrictDecoder}
  alias ElixirDB.MapAccess
  alias ElixirDB.Observability.Instrumentation.{Query, SQLite}
  alias ElixirDB.Query.{Normalizer, Prepared, SubscriptionRequest}
  alias ElixirDB.Storage.BackendContext
  alias ElixirDB.Storage.OpaqueHandle
  alias ElixirDB.Storage.Ports.Errors
  alias ElixirDB.Storage.RequestValidation
  alias ElixirDB.Storage.Services

  alias ElixirDB.Storage.SQLite.{
    Capabilities,
    Changes,
    Connection,
    Documents,
    IndexCatalog,
    LocalRecords,
    Ownership,
    QueryRunner,
    ReplicationJobs,
    Retention,
    Revisions,
    Schema,
    Transaction,
    Views
  }

  @identity_cache_key :elixir_db_sqlite_identity_cache
  @query_normalization_cache_limit 16

  defstruct [
    :path,
    :conn,
    :identity,
    storage_mode: :disk,
    retention_fault: nil,
    view_fault: nil,
    derived_fault: nil
  ]

  @type storage_mode :: :disk | :memory
  @type retention_fault :: (atom() -> :ok | {:error, ElixirDB.Error.t()}) | nil
  @type view_fault :: (atom() -> :ok | {:error, ElixirDB.Error.t()}) | nil
  @type derived_fault :: (atom() -> :ok | {:error, ElixirDB.Error.t()}) | nil
  @type t :: %__MODULE__{
          path: binary(),
          conn: Connection.handle(),
          identity: map(),
          storage_mode: storage_mode(),
          retention_fault: retention_fault(),
          view_fault: view_fault(),
          derived_fault: derived_fault()
        }

  @impl true
  def create(path, options \\ %{}) do
    options = if is_map(options), do: options, else: %{}
    uuid = MapAccess.get(options, :database_uuid, ElixirDB.UUID.v4())
    config = MapAccess.get(options, :config, ElixirDB.Config.defaults())

    with {:ok, storage_mode} <- storage_mode(path, options),
         true <- valid_uuid?(uuid),
         {:ok, bounded_config} <- ElixirDB.Config.merge_and_bound(config),
         {:ok, config_json} <- Canonical.encode(bounded_config),
         :ok <- ensure_parent_directory(path, storage_mode),
         {:ok, conn} <- Connection.open(connection_path(path, storage_mode)),
         :ok <- Schema.configure(conn, storage_mode: storage_mode),
         :ok <-
           Schema.create(conn, uuid, config_json,
             storage_mode: storage_mode,
             database_kind: MapAccess.get(options, :database_kind, :ordinary),
             initial_derived_view: MapAccess.get(options, :initial_derived_view)
           ),
         {:ok, identity} <- Schema.validate(conn, storage_mode: storage_mode) do
      {:ok,
       %__MODULE__{
         path: path,
         conn: conn,
         identity: decode_identity(identity),
         storage_mode: storage_mode
       }}
    else
      false ->
        {:error, ElixirDB.Error.invalid_request("database UUID must be a UUID")}

      {:error, reason} ->
        {:error, normalize_error(reason)}
    end
  end

  @doc "Returns the SQLite data artifact path inside a bundle root."
  @spec artifact_path(binary()) :: binary()
  def artifact_path(bundle_root) when is_binary(bundle_root),
    do: Path.join(bundle_root, "database.sqlite3")

  @doc "Starts SQLite ownership for the data artifact under `bundle_root`."
  @spec start_ownership(binary()) :: GenServer.on_start()
  def start_ownership(bundle_root) when is_binary(bundle_root) do
    Ownership.start_link(artifact_path(bundle_root))
  end

  @doc "Validates required SQLite runtime capabilities."
  @spec validate_capabilities!() :: binary()
  def validate_capabilities!, do: Capabilities.validate!()

  @doc "Returns opaque SQLite capability metadata."
  @spec capabilities_report() :: map()
  def capabilities_report, do: Capabilities.report()

  @doc "Wraps an open SQLite adapter in an opaque backend context."
  @spec to_context(t()) :: BackendContext.t()
  def to_context(%__MODULE__{} = adapter) do
    bundle_root =
      case adapter.path do
        ":memory:" -> ":memory:"
        path when is_binary(path) -> Path.dirname(path)
      end

    identity =
      (adapter.identity || %{})
      |> maybe_put_fault(:retention_fault, adapter.retention_fault)
      |> maybe_put_fault(:view_fault, adapter.view_fault)
      |> maybe_put_fault(:derived_fault, adapter.derived_fault)

    BackendContext.new(
      backend: __MODULE__,
      backend_ref: OpaqueHandle.wrap(adapter),
      bundle_root: bundle_root,
      capabilities: capabilities_report(),
      identity: identity
    )
  end

  @doc "Returns the SQLite module that implements `family`."
  @spec port(atom()) :: module()
  def port(family) when is_atom(family) do
    Map.fetch!(port_modules(), family)
  end

  @doc "Returns the SQLite port composition map."
  @spec port_modules() :: %{atom() => module()}
  def port_modules do
    %{
      lifecycle: ElixirDB.Storage.SQLite.Lifecycle,
      transaction: ElixirDB.Storage.SQLite.Transaction,
      ownership: ElixirDB.Storage.SQLite.OwnershipPort,
      document_facts: ElixirDB.Storage.SQLite.DocumentFacts,
      change_log: ElixirDB.Storage.SQLite.ChangeLog,
      local_records: ElixirDB.Storage.SQLite.LocalRecordsPort,
      retention_records: ElixirDB.Storage.SQLite.RetentionRecordsPort,
      index_candidates: ElixirDB.Storage.SQLite.IndexCandidates,
      view_state: ElixirDB.Storage.SQLite.ViewStatePort,
      derived_state: ElixirDB.Storage.SQLite.DerivedStatePort,
      attachment_metadata: ElixirDB.Storage.SQLite.AttachmentMetadataPort,
      inspection: ElixirDB.Storage.SQLite.InspectionPort
    }
  end

  @doc "Transaction port entry used by `ElixirDB.Storage.Transaction.run/2`."
  @spec run_transaction(BackendContext.t(), (BackendContext.t() -> term())) ::
          {:ok, term()} | {:error, ElixirDB.Error.t()}
  def run_transaction(%BackendContext{} = context, fun) when is_function(fun, 1) do
    Transaction.run(context, fun)
  end

  @doc "Returns the SQLite transaction port module."
  @spec transaction_port() :: module()
  def transaction_port, do: Transaction

  @doc false
  @spec invalidate_identity_cache(Connection.handle()) :: term()
  def invalidate_identity_cache(conn), do: Process.delete({@identity_cache_key, conn})

  @impl true
  def open(path, _options \\ %{}) do
    with {:ok, conn} <- Connection.open(path, mode: [:readwrite]),
         :ok <- Schema.configure(conn),
         {:ok, identity} <- Schema.validate(conn) do
      {:ok, %__MODULE__{path: path, conn: conn, identity: decode_identity(identity)}}
    else
      {:error, reason} -> {:error, normalize_error(reason)}
    end
  end

  @impl true
  def close(%__MODULE__{conn: conn}) do
    IndexCatalog.clear_cache(conn)
    Views.clear_cache(conn)
    QueryRunner.clear_cache(conn)
    invalidate_identity_cache(conn)
    Connection.close(conn)
  end

  @impl true
  def identity(%__MODULE__{conn: conn, identity: identity}) do
    case Process.get({@identity_cache_key, conn}) do
      {:ok, _cached_identity} = cached ->
        cached

      nil ->
        load_and_cache_identity(conn, identity)
    end
  end

  defp load_and_cache_identity(conn, identity) do
    result = read_identity_metadata(conn, identity)
    if match?({:ok, _}, result), do: Process.put({@identity_cache_key, conn}, result)
    result
  end

  defp read_identity_metadata(conn, identity) do
    case Connection.query(
           conn,
           "SELECT database_kind, current_sequence, history_epoch, retention_floor_sequence, compaction_epoch, retention_boundary_digest, config_json FROM db_meta WHERE id = 1"
         ) do
      {:ok,
       [
         [
           database_kind,
           sequence,
           history_epoch,
           floor,
           compaction_epoch,
           boundary_digest,
           config_json
         ]
       ]} ->
        with {:ok, database_kind} <- ElixirDB.DatabaseKind.from_storage(database_kind) do
          config = identity_config(config_json, identity)

          {:ok,
           %{
             identity
             | database_kind: database_kind,
               current_sequence: sequence,
               history_epoch: history_epoch,
               retention_floor_sequence: floor,
               compaction_epoch: compaction_epoch,
               retention_boundary_digest: boundary_digest,
               config: config,
               config_json: config_json,
               retention_mode: get_in(config, ["retention", "mode"]) || "disabled"
           }}
        end

      {:error, reason} ->
        {:error, normalize_error(reason)}
    end
  end

  defp identity_config(config_json, identity) do
    if config_json == identity.config_json, do: identity.config, else: decode_json!(config_json)
  end

  @impl true
  def update_config(%__MODULE__{conn: conn}, config) do
    with {:ok, bounded} <- ElixirDB.Config.merge_and_bound(config),
         {:ok, json} <- Canonical.encode(bounded),
         :ok <- Connection.execute(conn, "UPDATE db_meta SET config_json = ? WHERE id = 1", [json]) do
      invalidate_identity_cache(conn)
      {:ok, bounded}
    else
      {:error, error} -> {:error, normalize_error(error)}
    end
  end

  @impl true
  def integrity_check(%__MODULE__{} = adapter, options \\ %{}) when is_map(options) do
    Services.Integrity.check(to_context(adapter), options)
  end

  @impl true
  def get_document(%__MODULE__{} = adapter, request) when is_map(request) do
    with {:ok, doc} <-
           SQLite.trace_sqlite_phase(:document_lookup, fn ->
             Documents.find(adapter.conn, MapAccess.get(request, :document_id))
           end),
         {:ok, revision} <-
           SQLite.trace_sqlite_phase(:revision_lookup, fn ->
             choose_revision(adapter, doc, MapAccess.get(request, :revision))
           end) do
      include_conflicts = MapAccess.get(request, :include_conflicts, false)

      leaves =
        if include_conflicts do
          SQLite.trace_sqlite_phase(:document_leaves, fn ->
            Revisions.leaves(adapter.conn, doc.doc_key)
          end)
        else
          []
        end

      {:ok, Documents.to_result(doc, revision, leaves)}
    end
  end

  def get_document(_adapter, _request),
    do: {:error, ElixirDB.Error.invalid_request("document request must be an object")}

  @impl true
  def get_revision(%__MODULE__{} = adapter, request) when is_map(request) do
    document_id = MapAccess.get(request, :document_id)
    revision_id = MapAccess.get(request, :revision_id)

    with {:ok, doc} <- Documents.find(adapter.conn, document_id),
         {:ok, revision} <- Revisions.find(adapter.conn, doc.doc_key, revision_id) do
      {:ok, Documents.to_result(doc, revision, [])}
    end
  end

  def get_revision(_adapter, _request),
    do: {:error, ElixirDB.Error.invalid_request("revision request must be an object")}

  @doc "Applies a local put/delete via `ElixirDB.Storage.Services` (not an Adapter callback)."
  def apply_local_mutation(adapter, request) when is_map(request) do
    with :ok <- ensure_writable(adapter) do
      Services.apply_local_mutation(to_context(adapter), request)
    end
  end

  def apply_local_mutation(_adapter, _request),
    do: {:error, ElixirDB.Error.invalid_request("mutation request must be an object")}

  @doc "Applies a bulk mutation batch via `ElixirDB.Storage.Services` (not an Adapter callback)."
  def apply_bulk_mutation(adapter, request) when is_map(request) do
    with :ok <- ensure_writable(adapter) do
      Services.apply_bulk_mutation(to_context(adapter), request)
    end
  end

  def apply_bulk_mutation(_adapter, _request),
    do: {:error, ElixirDB.Error.invalid_request("bulk mutation request must be an object")}

  @doc "Resolves a conflict via `ElixirDB.Storage.Services` (not an Adapter callback)."
  def resolve_conflict(adapter, request) when is_map(request) do
    with :ok <- ensure_writable(adapter) do
      Services.resolve_conflict(to_context(adapter), request)
    end
  end

  def resolve_conflict(_adapter, _request),
    do: {:error, ElixirDB.Error.invalid_request("conflict request must be an object")}

  @impl true
  def read_changes(%__MODULE__{conn: conn} = adapter, request) when is_map(request) do
    since = MapAccess.get(request, :since, 0)
    limit = MapAccess.get(request, :limit, 100)

    with :ok <- validate_non_negative_integer(since, "since"),
         {:ok, identity} <-
           SQLite.trace_sqlite_phase(:changes_identity, fn -> identity(adapter) end),
         :ok <- validate_changes_since_floor(since, identity),
         :ok <-
           validate_changes_limit(
             limit,
             get_in(identity, [:config, "changes", "max_batch"])
           ),
         {:ok, {rows, page_has_more}} <-
           SQLite.trace_sqlite_phase(:changes_fetch, [entries: limit], fn ->
             Changes.fetch_page(conn, since, limit)
           end),
         {:ok, results} <-
           SQLite.trace_sqlite_phase(:changes_decode, [entries: length(rows)], fn ->
             Changes.decode_rows(rows)
           end),
         last_sequence <- List.last(results, %{sequence: since}).sequence,
         {:ok, has_more} <-
           SQLite.trace_sqlite_phase(:changes_has_more, fn ->
             {:ok, page_has_more}
           end) do
      {:ok,
       %{
         results: results,
         last_sequence: last_sequence,
         has_more: has_more
       }}
    else
      {:error, reason} -> {:error, normalize_error(reason)}
    end
  end

  def read_changes(_adapter, _request),
    do: {:error, ElixirDB.Error.invalid_request("changes request must be an object")}

  @impl true
  def has_local_origin_changes?(%__MODULE__{conn: conn}),
    do: Changes.has_local_origin_changes?(conn)

  @impl true
  def has_local_origin_changes?(%__MODULE__{conn: conn}, peer_database_uuid),
    do: Changes.has_local_origin_changes?(conn, peer_database_uuid)

  @impl true
  def clear_pending_local_causal(%__MODULE__{} = adapter),
    do: clear_pending_local_causal(adapter, nil)

  @impl true
  def clear_pending_local_causal(%__MODULE__{conn: conn}, peer_database_uuid) do
    case Retention.clear_pending_local_causal(conn, peer_database_uuid) do
      :ok -> {:ok, :cleared}
      {:error, error} -> {:error, error}
    end
  end

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
  def import_revision_chains(adapter, request) when is_map(request) do
    with :ok <- ensure_writable(adapter) do
      Services.import_revision_chains(to_context(adapter), request)
    end
  end

  def import_revision_chains(_adapter, _request),
    do: {:error, ElixirDB.Error.invalid_request("revision import request must be an object")}

  @impl true
  def get_local_record(%__MODULE__{conn: conn}, namespace, key),
    do: LocalRecords.fetch(conn, namespace, key)

  @impl true
  def put_local_record_cas(%__MODULE__{} = adapter, request) when is_map(request),
    do: transaction(adapter, fn -> LocalRecords.put_cas_tx(adapter, request) end)

  def put_local_record_cas(_adapter, _request),
    do: {:error, ElixirDB.Error.invalid_request("local record request must be an object")}

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

  @impl true
  def list_replication_jobs(%__MODULE__{conn: conn}), do: ReplicationJobs.list_all(conn)

  @impl true
  def put_replication_job(%__MODULE__{conn: conn}, job), do: ReplicationJobs.upsert(conn, job)

  @impl true
  def delete_replication_job(%__MODULE__{conn: conn}, job_id),
    do: ReplicationJobs.delete_by_id(conn, job_id)

  @impl true
  def create_index(%__MODULE__{conn: conn} = adapter, definition) do
    uuid = adapter_identity_uuid(adapter)
    index_id = MapAccess.get(definition, :index_id)
    index_type = MapAccess.get(definition, :type)

    Query.build_index(uuid, index_id, index_type, fn ->
      transaction(%__MODULE__{conn: conn}, fn -> IndexCatalog.create_tx(conn, definition) end)
    end)
  end

  @impl true
  def delete_index(%__MODULE__{conn: conn}, index_id) do
    transaction(%__MODULE__{conn: conn}, fn -> IndexCatalog.delete_tx(conn, index_id) end)
  end

  @impl true
  def rebuild_index(%__MODULE__{conn: conn} = adapter, index_id) do
    uuid = adapter_identity_uuid(adapter)

    Query.build_index(uuid, index_id, nil, fn ->
      transaction(%__MODULE__{conn: conn}, fn -> IndexCatalog.rebuild_tx(conn, index_id) end)
    end)
  end

  @impl true
  def list_indexes(%__MODULE__{conn: conn}), do: IndexCatalog.list(conn)

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

  @impl true
  def execute_query(%__MODULE__{} = adapter, request) when is_map(request) do
    # Capture the start in native units for the span (OTel uses native), and in
    # milliseconds for the overrun guard (config is in ms). Reusing one clock
    # keeps the span and guard measurements consistent.
    started_native = System.monotonic_time()
    started_ms = System.monotonic_time(:millisecond)

    identity =
      SQLite.trace_sqlite_phase(:query_identity, fn -> adapter_identity(adapter) end)

    uuid = Map.get(identity, :database_uuid)
    maximum = get_in(identity, [:config, "queries", "max_execution_ms"]) || 5_000

    # Wrap the actual query execution in the span so its duration is real and
    # errors flow through the error.code/status policy (§5.5, §6.5). The
    # wrapper receives {result, examined_count} and returns the bare result
    # after recording the examined attribute.
    prepared_request =
      SQLite.trace_sqlite_phase(:query_prepare_request, fn -> prepare_query_request(request) end)

    result =
      Query.execute(uuid, 0, started_native, fn ->
        res =
          case prepared_request do
            {:ok, normalized} -> QueryRunner.execute(adapter, normalized, identity)
            {:error, _} = error -> error
          end

        {res, examined_count(res)}
      end)

    elapsed = System.monotonic_time(:millisecond) - started_ms

    if elapsed <= maximum,
      do: result,
      else:
        {:error,
         ElixirDB.Error.resource_limit("query execution exceeded the configured limit", %{
           elapsed_ms: elapsed,
           maximum_ms: maximum
         })}
  end

  def execute_query(_adapter, _request),
    do: {:error, ElixirDB.Error.invalid_request("query must be an object")}

  @impl true
  def execute_subscription_snapshot(%__MODULE__{} = adapter, request) when is_map(request) do
    started_native = System.monotonic_time()
    started_ms = System.monotonic_time(:millisecond)

    identity =
      SQLite.trace_sqlite_phase(:query_identity, fn -> adapter_identity(adapter) end)

    uuid = Map.get(identity, :database_uuid)
    maximum = get_in(identity, [:config, "queries", "max_execution_ms"]) || 5_000

    prepared_request =
      SQLite.trace_sqlite_phase(:query_prepare_request, fn ->
        prepare_subscription_snapshot_request(adapter, request)
      end)

    result =
      Query.execute(uuid, 0, started_native, fn ->
        res =
          case prepared_request do
            {:ok, normalized} ->
              QueryRunner.subscription_snapshot(adapter, normalized, identity)

            {:error, _} = error ->
              error
          end

        {res, examined_count(res)}
      end)

    elapsed = System.monotonic_time(:millisecond) - started_ms

    if elapsed <= maximum,
      do: result,
      else:
        {:error,
         ElixirDB.Error.resource_limit("query execution exceeded the configured limit", %{
           elapsed_ms: elapsed,
           maximum_ms: maximum
         })}
  end

  def execute_subscription_snapshot(_adapter, _request),
    do: {:error, ElixirDB.Error.invalid_request("subscription snapshot request must be an object")}

  @impl true
  def get_revisions_batch(%__MODULE__{} = adapter, requests) when is_list(requests) do
    with {:ok, identity} <- identity(adapter),
         :ok <-
           SubscriptionRequest.validate_revisions_batch(requests, Map.get(identity, :config, %{})) do
      fetch_revision_batch(adapter, requests)
    end
  end

  def get_revisions_batch(_adapter, _requests),
    do: {:error, ElixirDB.Error.invalid_request("revision batch requests must be a list")}

  defp fetch_revision_batch(adapter, requests) do
    Enum.reduce_while(requests, {:ok, []}, fn request, {:ok, acc} ->
      case fetch_revision_batch_item(adapter, request) do
        {:ok, envelope} -> {:cont, {:ok, [envelope | acc]}}
        {:error, _} = error -> {:halt, error}
      end
    end)
    |> case do
      {:ok, results} -> {:ok, Enum.reverse(results)}
      error -> error
    end
  end

  defp fetch_revision_batch_item(adapter, request) do
    document_id = MapAccess.get(request, :document_id)
    revision_id = MapAccess.get(request, :revision_id)

    with {:ok, doc} <- Documents.find(adapter.conn, document_id),
         {:ok, revision} <- fetch_batch_revision(adapter, doc, revision_id) do
      envelope = Documents.to_result(doc, revision, [])

      {:ok,
       %{
         id: envelope.id,
         revision: envelope.revision,
         deleted: envelope.deleted,
         body: envelope.body
       }}
    else
      {:error, %ElixirDB.Error{code: :revision_not_found}} ->
        {:error,
         ElixirDB.Error.integrity_violation("changes entry references a missing revision", %{
           document_id: document_id,
           revision_id: revision_id
         })}

      {:error, %ElixirDB.Error{code: :document_not_found}} ->
        {:error,
         ElixirDB.Error.integrity_violation("changes entry references a missing document", %{
           document_id: document_id,
           revision_id: revision_id
         })}

      {:error, %ElixirDB.Error{} = error} ->
        {:error, error}
    end
  end

  defp fetch_batch_revision(_adapter, nil, _revision_id),
    do: {:error, ElixirDB.Error.document_not_found("document not found")}

  defp fetch_batch_revision(adapter, doc, revision_id),
    do: Revisions.find(adapter.conn, doc.doc_key, revision_id)

  @impl true
  def explain_query(%__MODULE__{} = adapter, request) when is_map(request) do
    case prepare_query_request(request) do
      {:ok, normalized} -> QueryRunner.explain(adapter, normalized)
      {:error, _} = error -> error
    end
  end

  def explain_query(_adapter, _request),
    do: {:error, ElixirDB.Error.invalid_request("query explanation must be an object")}

  defp prepare_query_request(%Prepared{} = prepared),
    do: Normalizer.normalize_public_request(prepared)

  defp prepare_query_request(request) when is_map(request) do
    StrictCache.memoize(
      :public_query_normalization,
      request,
      @query_normalization_cache_limit,
      fn -> Normalizer.normalize_public_request(request) end
    )
  end

  defp prepare_subscription_snapshot_request(adapter, request) when is_map(request) do
    with {:ok, identity} <- identity(adapter) do
      SubscriptionRequest.prepare_snapshot(request, Map.get(identity, :config, %{}))
    end
  end

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
  def protect_pending_blob(%__MODULE__{} = adapter, request) when is_map(request),
    do: Services.protect_pending_blob(to_context(adapter), request)

  def protect_pending_blob(_adapter, _request),
    do:
      {:error, ElixirDB.Error.invalid_request("pending blob protection request must be an object")}

  @impl true
  def remove_pending_blob_protection(%__MODULE__{} = adapter, request) when is_map(request),
    do: Services.remove_pending_blob_protection(to_context(adapter), request)

  def remove_pending_blob_protection(_adapter, _request),
    do:
      {:error,
       ElixirDB.Error.invalid_request("remove pending blob protection request must be an object")}

  @impl true
  def list_live_attachment_digests(%__MODULE__{} = adapter, request) when is_map(request),
    do: Services.list_live_attachment_digests(to_context(adapter), request)

  def list_live_attachment_digests(_adapter, _request),
    do: {:error, ElixirDB.Error.invalid_request("live attachment digest request must be an object")}

  @impl true
  def cleanup_expired_pending_blobs(%__MODULE__{} = adapter, request) when is_map(request),
    do: Services.cleanup_expired_pending_blobs(to_context(adapter), request)

  def cleanup_expired_pending_blobs(%__MODULE__{} = adapter, _request),
    do: cleanup_expired_pending_blobs(adapter, %{})

  defp transaction(%__MODULE__{} = adapter, fun) when is_function(fun, 0) do
    Transaction.run_on_adapter(adapter, fn _tx_adapter -> fun.() end)
  end

  defp choose_revision(_adapter, nil, _revision),
    do: {:error, ElixirDB.Error.document_not_found("document not found")}

  defp choose_revision(adapter, doc, nil) do
    if doc.winning_deleted,
      do:
        {:error,
         ElixirDB.Error.document_not_found("document is deleted", %{
           winning_revision: doc.winning_revision
         })},
      else: Revisions.find(adapter.conn, doc.doc_key, doc.winning_revision)
  end

  defp choose_revision(adapter, doc, revision),
    do: Revisions.find(adapter.conn, doc.doc_key, revision)

  defp validate_changes_limit(limit, database_max),
    do: RequestValidation.validate_changes_limit(limit, database_max)

  defp validate_non_negative_integer(value, label),
    do: RequestValidation.validate_non_negative_integer(value, label)

  defp validate_changes_since_floor(since, identity),
    do: RequestValidation.validate_changes_since_floor(since, identity)

  defp adapter_identity(adapter) do
    case identity(adapter) do
      {:ok, value} -> value
      _ -> %{current_sequence: 0, config: ElixirDB.Config.defaults()}
    end
  end

  # The database UUID from the adapter identity, used by index instrumentation.
  # Falls back to nil when the identity cannot be read so the span is still
  # emitted without the attribute.
  defp adapter_identity_uuid(adapter) do
    case identity(adapter) do
      {:ok, %{database_uuid: uuid}} when is_binary(uuid) -> uuid
      _ -> nil
    end
  end

  defp ensure_writable(%__MODULE__{identity: %{database_kind: :derived}}),
    do:
      {:error,
       ElixirDB.Error.derived_database_read_only(
         "derived databases accept writes only from their materializer"
       )}

  defp ensure_writable(_adapter), do: :ok

  # The candidate count the query runner computes is bound as `examined` on the
  # span/metric. Returns 0 when unavailable.
  defp examined_count({:ok, %{examined: examined}}) when is_integer(examined), do: examined
  defp examined_count(_), do: 0

  defp decode_json(json), do: StrictDecoder.decode(json)

  defp decode_json!(json) do
    case decode_json(json) do
      {:ok, value} -> value
      _ -> nil
    end
  end

  defp decode_identity(identity) do
    config = decode_json!(identity.config_json)

    identity
    |> Map.put(:config, config)
    |> Map.put(:retention_floor, Map.get(identity, :retention_floor_sequence))
    |> Map.put(:retention_floor_sequence, Map.get(identity, :retention_floor_sequence, 0))
    |> Map.put(:compaction_epoch, Map.get(identity, :compaction_epoch, 0))
    |> Map.put(:retention_mode, get_in(config, ["retention", "mode"]) || "disabled")
  end

  defp normalize_error(reason), do: Errors.normalize(reason)

  defp storage_mode(path, options) do
    requested =
      MapAccess.get(options, :storage_mode, if(path == ":memory:", do: :memory, else: :disk))

    case {requested, path == ":memory:"} do
      {:disk, false} ->
        {:ok, :disk}

      {:memory, true} ->
        {:ok, :memory}

      {:memory, false} ->
        {:error,
         ElixirDB.Error.invalid_request("in-memory SQLite databases must use the :memory: path")}

      {:disk, true} ->
        {:error, ElixirDB.Error.invalid_request(":memory: requires storage_mode: :memory")}

      _ ->
        {:error, ElixirDB.Error.invalid_request("SQLite storage mode must be :disk or :memory")}
    end
  end

  defp ensure_parent_directory(_path, :memory), do: :ok

  defp ensure_parent_directory(path, :disk) do
    File.mkdir_p!(Path.dirname(path))
    :ok
  end

  defp connection_path(_path, :memory), do: ":memory:"
  defp connection_path(path, :disk), do: path

  defp valid_uuid?(uuid) when is_binary(uuid) do
    Regex.match?(
      ~r/^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i,
      uuid
    )
  end

  defp valid_uuid?(_), do: false

  defp maybe_put_fault(identity, key, fun) when is_function(fun, 1),
    do: Map.put(identity, key, fun)

  defp maybe_put_fault(identity, _key, _), do: identity
end
