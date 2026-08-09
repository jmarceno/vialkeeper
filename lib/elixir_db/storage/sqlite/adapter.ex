defmodule ElixirDB.Storage.SQLite.Adapter do
  @moduledoc """
  Version 1 SQLite storage adapter.

  Orchestrates transactions and the Storage.Adapter behaviour. Per-concern SQL
  lives in `Documents`, `Revisions`, `Attachments`, `Changes`, `LocalRecords`,
  `ReplicationJobs`, `Integrity`, `QueryRunner`, `IndexCatalog`, `Chains`,
  `Mutations`, and `Import`. Physical index DDL remains in `Indexes`, with thin
  facades in `StructuredIndexes` / `FullTextIndexes`.
  """
  @behaviour ElixirDB.Storage.Adapter

  alias ElixirDB.JSON.{Canonical, StrictDecoder}
  alias ElixirDB.MapAccess
  alias ElixirDB.Observability.Instrumentation.{Query, SQLite}
  alias ElixirDB.Query.{Normalizer, Prepared}

  alias ElixirDB.Storage.SQLite.{
    Attachments,
    Chains,
    Changes,
    Connection,
    Documents,
    Import,
    IndexCatalog,
    Integrity,
    LocalRecords,
    Mutations,
    QueryRunner,
    ReplicationJobs,
    Retention,
    Revisions,
    Schema
  }

  @identity_cache_key :elixir_db_sqlite_identity_cache

  defstruct [:path, :conn, :identity, storage_mode: :disk, retention_fault: nil]
  @type storage_mode :: :disk | :memory
  @type retention_fault :: (atom() -> :ok | {:error, ElixirDB.Error.t()}) | nil
  @query_public_keys [
    :selector,
    :sort,
    :fields,
    :limit,
    :bookmark,
    :index,
    :search,
    "selector",
    "sort",
    "fields",
    "limit",
    "bookmark",
    "index",
    "search"
  ]
  @type t :: %__MODULE__{
          path: binary(),
          conn: Connection.handle(),
          identity: map(),
          storage_mode: storage_mode(),
          retention_fault: retention_fault()
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
         :ok <- Schema.create(conn, uuid, config_json, storage_mode: storage_mode),
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
           "SELECT current_sequence, history_epoch, retention_floor_sequence, compaction_epoch, retention_boundary_digest, config_json FROM db_meta WHERE id = 1"
         ) do
      {:ok, [[sequence, history_epoch, floor, compaction_epoch, boundary_digest, config_json]]} ->
        config =
          if config_json == identity.config_json,
            do: identity.config,
            else: decode_json!(config_json)

        {:ok,
         %{
           identity
           | current_sequence: sequence,
             history_epoch: history_epoch,
             retention_floor_sequence: floor,
             compaction_epoch: compaction_epoch,
             retention_boundary_digest: boundary_digest,
             config: config,
             config_json: config_json,
             retention_mode: get_in(config, ["retention", "mode"]) || "disabled"
         }}

      {:error, reason} ->
        {:error, normalize_error(reason)}
    end
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
  def integrity_check(%__MODULE__{} = adapter, _options) do
    with {:ok, indexes} <- list_indexes(adapter),
         {:ok, report} <- Integrity.run(adapter.conn, indexes, attachment_bundle_root(adapter)) do
      {:ok, Map.merge(%{ok: true, indexes: length(indexes)}, report)}
    else
      {:error, %ElixirDB.Error{} = error} ->
        {:error, error}
    end
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

  @impl true
  def apply_local_mutation(adapter, request) when is_map(request),
    do: transaction(adapter, fn -> Mutations.apply_local_tx(adapter, request) end)

  def apply_local_mutation(_adapter, _request),
    do: {:error, ElixirDB.Error.invalid_request("mutation request must be an object")}

  @impl true
  def apply_bulk_mutation(adapter, %{operations: operations}) do
    with :ok <- Mutations.validate_operation_batch(operations) do
      transaction(adapter, fn -> Mutations.bulk_tx(adapter, operations) end)
    end
  end

  def apply_bulk_mutation(adapter, %{"operations" => operations}),
    do: apply_bulk_mutation(adapter, %{operations: operations})

  def apply_bulk_mutation(_adapter, _request),
    do: {:error, ElixirDB.Error.invalid_request("bulk mutation request must be an object")}

  @impl true
  def resolve_conflict(adapter, request) when is_map(request),
    do: transaction(adapter, fn -> Mutations.resolve_conflict_tx(adapter, request) end)

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

  @impl true
  def diff_revisions(%__MODULE__{} = adapter, request) when is_map(request),
    do: Chains.diff(adapter, request)

  def diff_revisions(_adapter, _request),
    do: {:error, ElixirDB.Error.invalid_request("revision diff request must be an object")}

  @impl true
  def get_revision_chains(%__MODULE__{} = adapter, request) when is_map(request),
    do: Chains.get(adapter, request)

  def get_revision_chains(_adapter, _request),
    do: {:error, ElixirDB.Error.invalid_request("revision chain request must be an object")}

  @impl true
  def import_revision_chains(adapter, request) when is_map(request) do
    chains = MapAccess.get(request, :chains, [])

    with :ok <- Import.validate_chain_batch(chains),
         :ok <-
           Import.validate_purged_boundaries(
             MapAccess.get(request, :purged_boundaries, []),
             MapAccess.get(request, :source_database_uuid)
           ),
         :ok <- Import.ensure_physical_blobs(adapter, chains) do
      transaction(adapter, fn -> Import.import_tx(adapter, request) end)
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
  def retention_state(%__MODULE__{conn: conn} = adapter) do
    case identity(adapter) do
      {:ok, %{config: config}} -> Retention.retention_state(conn, config)
      {:error, error} -> {:error, error}
    end
  end

  @impl true
  def list_peer_positions(%__MODULE__{conn: conn}), do: Retention.list_peer_positions(conn)

  @impl true
  def put_peer_position_cas(%__MODULE__{} = adapter, request) when is_map(request),
    do: transaction(adapter, fn -> Retention.put_peer_position_cas(adapter, request) end)

  def put_peer_position_cas(_adapter, _request),
    do: {:error, ElixirDB.Error.invalid_request("peer position request must be an object")}

  @impl true
  def read_boundary_pages(%__MODULE__{conn: conn}, request) when is_map(request),
    do: Retention.read_boundary_pages(conn, request)

  def read_boundary_pages(_adapter, _request),
    do: {:error, ElixirDB.Error.invalid_request("boundary page request must be an object")}

  @impl true
  def install_boundary_pages(%__MODULE__{} = adapter, request) when is_map(request),
    do: transaction(adapter, fn -> Retention.install_boundary_pages(adapter.conn, request) end)

  def install_boundary_pages(_adapter, _request),
    do: {:error, ElixirDB.Error.invalid_request("boundary page request must be an object")}

  @impl true
  def compact_retention(%__MODULE__{} = adapter, request \\ %{}) when is_map(request),
    do:
      transaction(adapter, fn ->
        with :ok <- retention_fault_check(adapter.retention_fault, :compact_retention_mid_tx) do
          Retention.compact(adapter, request)
        end
      end)

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
  def execute_query(%__MODULE__{} = adapter, request) when is_map(request) do
    # Capture the start in native units for the span (OTel uses native), and in
    # milliseconds for the overrun guard (config is in ms). Reusing one clock
    # per plan §5.5.
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
  def explain_query(%__MODULE__{} = adapter, request) when is_map(request) do
    case prepare_query_request(request) do
      {:ok, normalized} -> QueryRunner.explain(adapter, normalized)
      {:error, _} = error -> error
    end
  end

  def explain_query(_adapter, _request),
    do: {:error, ElixirDB.Error.invalid_request("query explanation must be an object")}

  defp prepare_query_request(%Prepared{} = prepared) do
    {:ok, Prepared.unwrap(prepared)}
  end

  defp prepare_query_request(request) when is_map(request) do
    # Plain maps are always treated as untrusted public input. A client-supplied
    # `:normalized` marker or `:predicate` must never bypass Normalizer.
    normalize_public_query(request)
  end

  defp normalize_public_query(request) do
    request
    |> Map.take(@query_public_keys)
    |> Normalizer.normalize()
    |> preserve_query_cursor(request)
  end

  defp preserve_query_cursor({:ok, normalized}, request) do
    {:ok,
     normalized
     |> maybe_put_internal(:after_id, MapAccess.get(request, :after_id))
     |> maybe_put_internal(:after_ordering, MapAccess.get(request, :after_ordering))}
  end

  defp preserve_query_cursor(error, _request), do: error

  defp maybe_put_internal(request, _key, nil), do: request
  defp maybe_put_internal(request, key, value), do: Map.put(request, key, value)

  @impl true
  def resolve_attachment_ticket(%__MODULE__{} = adapter, request) when is_map(request),
    do: Attachments.resolve_attachment_ticket(adapter, request)

  def resolve_attachment_ticket(_adapter, _request),
    do: {:error, ElixirDB.Error.invalid_request("attachment ticket request must be an object")}

  @impl true
  def resolve_blob_metadata(%__MODULE__{conn: conn}, request) when is_map(request),
    do: Attachments.resolve_blob_metadata(conn, request)

  def resolve_blob_metadata(_adapter, _request),
    do: {:error, ElixirDB.Error.invalid_request("blob metadata request must be an object")}

  @impl true
  def protect_pending_blob(%__MODULE__{} = adapter, request) when is_map(request) do
    transaction(adapter, fn -> Attachments.protect_pending_blob(adapter.conn, request) end)
  end

  def protect_pending_blob(_adapter, _request),
    do:
      {:error, ElixirDB.Error.invalid_request("pending blob protection request must be an object")}

  @impl true
  def remove_pending_blob_protection(%__MODULE__{} = adapter, request) when is_map(request) do
    transaction(adapter, fn ->
      Attachments.remove_pending_blob_protection(adapter.conn, request)
    end)
  end

  def remove_pending_blob_protection(_adapter, _request),
    do:
      {:error,
       ElixirDB.Error.invalid_request("remove pending blob protection request must be an object")}

  @impl true
  def list_live_attachment_digests(%__MODULE__{conn: conn}, request) when is_map(request),
    do: Attachments.list_live_attachment_digests(conn, request)

  def list_live_attachment_digests(_adapter, _request),
    do: {:error, ElixirDB.Error.invalid_request("live attachment digest request must be an object")}

  @impl true
  def cleanup_expired_pending_blobs(%__MODULE__{} = adapter, request) when is_map(request) do
    transaction(adapter, fn ->
      Attachments.cleanup_expired_pending_blobs(adapter.conn, request)
    end)
  end

  def cleanup_expired_pending_blobs(%__MODULE__{} = adapter, _request) do
    cleanup_expired_pending_blobs(adapter, %{})
  end

  defp transaction(%__MODULE__{conn: conn}, fun) do
    case SQLite.trace_sqlite_phase(:transaction_begin, fn ->
           Connection.execute(conn, "BEGIN IMMEDIATE")
         end) do
      :ok ->
        transaction_body(conn, fun)

      {:error, reason} ->
        {:error, normalize_error(reason)}
    end
  rescue
    exception in [
      ArgumentError,
      ArithmeticError,
      BadMapError,
      CaseClauseError,
      ErlangError,
      FunctionClauseError,
      KeyError,
      MatchError,
      Protocol.UndefinedError,
      RuntimeError,
      UndefinedFunctionError,
      WithClauseError
    ] ->
      _ =
        SQLite.trace_sqlite_phase(:transaction_rollback, fn ->
          Connection.execute(conn, "ROLLBACK")
        end)

      invalidate_identity_cache(conn)
      reraise exception, __STACKTRACE__
  end

  defp transaction_body(conn, fun) do
    case fun.() do
      {:ok, value} ->
        commit_transaction(conn, value)

      {:error, error} ->
        _ =
          SQLite.trace_sqlite_phase(:transaction_rollback, fn ->
            Connection.execute(conn, "ROLLBACK")
          end)

        invalidate_identity_cache(conn)
        {:error, error}
    end
  end

  defp commit_transaction(conn, value) do
    case SQLite.trace_sqlite_phase(:transaction_commit, fn ->
           Connection.execute(conn, "COMMIT")
         end) do
      :ok ->
        invalidate_identity_cache(conn)
        {:ok, value}

      {:error, reason} ->
        invalidate_identity_cache(conn)
        {:error, normalize_error(reason)}
    end
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

  defp validate_changes_limit(limit, database_max) when is_integer(limit) and limit > 0 do
    max = min(ElixirDB.Config.host_limits()[:max_changes_batch] || 500, database_max || 500)

    if limit <= max,
      do: :ok,
      else: {:error, ElixirDB.Error.resource_limit("changes batch exceeds the host limit")}
  end

  defp validate_changes_limit(_, _),
    do: {:error, ElixirDB.Error.invalid_request("changes limit must be a positive integer")}

  defp validate_non_negative_integer(value, _label) when is_integer(value) and value >= 0, do: :ok

  defp validate_non_negative_integer(_value, label),
    do: {:error, ElixirDB.Error.invalid_request("#{label} must be a non-negative integer")}

  defp validate_changes_since_floor(since, identity) do
    floor = Map.get(identity, :retention_floor_sequence, 0) || 0

    if since < floor do
      {:error,
       ElixirDB.Error.history_truncated("changes feed is below the retention floor", %{
         database_uuid: identity.database_uuid,
         history_epoch: identity.history_epoch,
         retention_floor: floor,
         compaction_epoch: Map.get(identity, :compaction_epoch, 0)
       })}
    else
      :ok
    end
  end

  defp retention_fault_check(nil, _point), do: :ok

  defp retention_fault_check(fun, point) when is_function(fun, 1) do
    case fun.(point) do
      :ok -> :ok
      {:error, %ElixirDB.Error{} = error} -> {:error, error}
    end
  end

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

  defp attachment_bundle_root(%__MODULE__{storage_mode: :memory}), do: nil

  defp attachment_bundle_root(%__MODULE__{path: path}), do: Path.dirname(Path.expand(path))

  # The candidate count the query runner computes (plan §5.5), bound as
  # `examined` on the span/metric. Returns 0 when unavailable.
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

  # Clears the process-local metadata snapshot after any operation that may
  # have changed sequence, retention, configuration, or other database state.
  defp invalidate_identity_cache(conn),
    do: Process.delete({@identity_cache_key, conn})

  defp normalize_error(%ElixirDB.Error{} = error), do: error

  defp normalize_error(reason),
    do: ElixirDB.Error.internal_error("SQLite operation failed", %{cause: inspect(reason)})

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
end
