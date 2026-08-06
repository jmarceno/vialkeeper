defmodule ElixirDB.Storage.SQLite.Adapter do
  @moduledoc """
  Version 1 SQLite storage adapter.

  Orchestrates transactions and the Storage.Adapter behaviour. Per-concern SQL
  lives in `Documents`, `Revisions`, `Changes`, `LocalRecords`,
  `ReplicationJobs`, `Integrity`, `QueryRunner`, `IndexCatalog`, `Chains`,
  `Mutations`, and `Import`. Physical index DDL remains in `Indexes`, with thin
  facades in `StructuredIndexes` / `FullTextIndexes`.
  """
  @behaviour ElixirDB.Storage.Adapter

  alias ElixirDB.JSON.{Canonical, StrictDecoder}

  alias ElixirDB.Storage.SQLite.{
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
    Revisions,
    Schema
  }

  defstruct [:path, :conn, :identity]
  @type t :: %__MODULE__{path: binary(), conn: Connection.handle(), identity: map()}

  @impl true
  def create(path, options \\ %{}) do
    options = if is_map(options), do: options, else: %{}
    File.mkdir_p!(Path.dirname(path))
    uuid = Map.get(options, :database_uuid, Map.get(options, "database_uuid", ElixirDB.UUID.v4()))
    config = Map.get(options, :config, ElixirDB.Config.defaults())

    with true <- valid_uuid?(uuid),
         {:ok, bounded_config} <- ElixirDB.Config.merge_and_bound(config),
         {:ok, config_json} <- Canonical.encode(bounded_config),
         {:ok, conn} <- Connection.open(path),
         :ok <- Schema.configure(conn),
         :ok <- Schema.create(conn, uuid, config_json),
         {:ok, identity} <- Schema.validate(conn) do
      {:ok, %__MODULE__{path: path, conn: conn, identity: decode_identity(identity)}}
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
  def close(%__MODULE__{conn: conn}), do: Connection.close(conn)

  @impl true
  def identity(%__MODULE__{conn: conn, identity: identity}) do
    case Connection.query(conn, "SELECT current_sequence, config_json FROM db_meta WHERE id = 1") do
      {:ok, [[sequence, config_json]]} ->
        {:ok,
         %{
           identity
           | current_sequence: sequence,
             config: decode_json!(config_json),
             config_json: config_json
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
      {:ok, bounded}
    else
      {:error, error} -> {:error, normalize_error(error)}
    end
  end

  @impl true
  def integrity_check(%__MODULE__{} = adapter, _options) do
    with {:ok, indexes} <- list_indexes(adapter),
         :ok <- Integrity.run(adapter.conn, indexes) do
      {:ok, %{ok: true, indexes: length(indexes)}}
    else
      {:error, %ElixirDB.Error{} = error} ->
        {:error, error}
    end
  end

  @impl true
  def get_document(%__MODULE__{} = adapter, request) when is_map(request) do
    with {:ok, doc} <- Documents.find(adapter.conn, request[:document_id] || request["document_id"]),
         {:ok, revision} <- choose_revision(adapter, doc, request[:revision] || request["revision"]) do
      include_conflicts = request[:include_conflicts] || request["include_conflicts"] || false

      {:ok,
       Documents.to_result(
         doc,
         revision,
         if(include_conflicts, do: Revisions.leaves(adapter.conn, doc.doc_key), else: [])
       )}
    end
  end

  def get_document(_adapter, _request),
    do: {:error, ElixirDB.Error.invalid_request("document request must be an object")}

  @impl true
  def get_revision(%__MODULE__{} = adapter, request) when is_map(request) do
    document_id = request[:document_id] || request["document_id"]
    revision_id = request[:revision_id] || request["revision_id"]

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
    since = request[:since] || request["since"] || 0
    limit = request[:limit] || request["limit"] || 100

    with :ok <- validate_non_negative_integer(since, "since"),
         :ok <-
           validate_changes_limit(
             limit,
             get_in(adapter_identity(adapter), [:config, "changes", "max_batch"])
           ),
         {:ok, rows} <- Changes.fetch_after(conn, since, limit),
         {:ok, results} <- Changes.decode_rows(rows),
         last_sequence <- List.last(results, %{sequence: since}).sequence,
         {:ok, has_more} <- Changes.exists_after?(conn, last_sequence) do
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
    with :ok <- Import.validate_chain_batch(request[:chains] || request["chains"] || []) do
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
  def list_replication_jobs(%__MODULE__{conn: conn}), do: ReplicationJobs.list_all(conn)

  @impl true
  def put_replication_job(%__MODULE__{conn: conn}, job), do: ReplicationJobs.upsert(conn, job)

  @impl true
  def delete_replication_job(%__MODULE__{conn: conn}, job_id),
    do: ReplicationJobs.delete_by_id(conn, job_id)

  @impl true
  def create_index(%__MODULE__{conn: conn}, definition) do
    transaction(%__MODULE__{conn: conn}, fn -> IndexCatalog.create_tx(conn, definition) end)
  end

  @impl true
  def delete_index(%__MODULE__{conn: conn}, index_id) do
    transaction(%__MODULE__{conn: conn}, fn -> IndexCatalog.delete_tx(conn, index_id) end)
  end

  @impl true
  def rebuild_index(%__MODULE__{conn: conn}, index_id) do
    transaction(%__MODULE__{conn: conn}, fn -> IndexCatalog.rebuild_tx(conn, index_id) end)
  end

  @impl true
  def list_indexes(%__MODULE__{conn: conn}), do: IndexCatalog.list(conn)

  @impl true
  def execute_query(%__MODULE__{} = adapter, request) when is_map(request) do
    started = System.monotonic_time(:millisecond)
    result = QueryRunner.execute(adapter, request)
    elapsed = System.monotonic_time(:millisecond) - started
    maximum = get_in(adapter_identity(adapter), [:config, "queries", "max_execution_ms"]) || 5_000

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
  def explain_query(%__MODULE__{} = adapter, request) when is_map(request),
    do: QueryRunner.explain(adapter, request)

  def explain_query(_adapter, _request),
    do: {:error, ElixirDB.Error.invalid_request("query explanation must be an object")}

  defp transaction(%__MODULE__{conn: conn}, fun) do
    with :ok <- Connection.execute(conn, "BEGIN IMMEDIATE") do
      try do
        case fun.() do
          {:ok, value} ->
            case Connection.execute(conn, "COMMIT") do
              :ok -> {:ok, value}
              {:error, reason} -> {:error, normalize_error(reason)}
            end

          {:error, error} ->
            _ = Connection.execute(conn, "ROLLBACK")
            {:error, error}
        end
      rescue
        exception ->
          _ = Connection.execute(conn, "ROLLBACK")
          reraise exception, __STACKTRACE__
      end
    else
      {:error, reason} -> {:error, normalize_error(reason)}
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

  defp adapter_identity(adapter) do
    case identity(adapter) do
      {:ok, value} -> value
      _ -> %{current_sequence: 0, config: ElixirDB.Config.defaults()}
    end
  end

  defp decode_json(json), do: StrictDecoder.decode(json)

  defp decode_json!(json) do
    case decode_json(json) do
      {:ok, value} -> value
      _ -> nil
    end
  end

  defp decode_identity(identity) do
    config = decode_json!(identity.config_json)
    Map.put(identity, :config, config)
  end

  defp normalize_error(%ElixirDB.Error{} = error), do: error

  defp normalize_error(reason),
    do: ElixirDB.Error.internal_error("SQLite operation failed", %{cause: inspect(reason)})

  defp valid_uuid?(uuid) when is_binary(uuid) do
    Regex.match?(
      ~r/^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i,
      uuid
    )
  end

  defp valid_uuid?(_), do: false
end
