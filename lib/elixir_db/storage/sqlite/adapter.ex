defmodule ElixirDB.Storage.SQLite.Adapter do
  @moduledoc """
  Version 1 SQLite storage adapter.

  Orchestrates transactions and the Storage.Adapter behaviour. Per-concern SQL
  lives in `Documents`, `Revisions`, `Changes`, `LocalRecords`,
  `ReplicationJobs`, and `Integrity`. Physical index DDL remains in `Indexes`,
  with thin facades in `StructuredIndexes` / `FullTextIndexes`.
  """
  @behaviour ElixirDB.Storage.Adapter

  alias ElixirDB.Domain.Revision
  alias ElixirDB.JSON.{Canonical, StrictDecoder}
  alias ElixirDB.Query.{Planner, Projection}
  alias ElixirDB.Revisions.{Id, Winner}

  alias ElixirDB.Storage.SQLite.{
    Changes,
    Connection,
    Documents,
    FullTextIndexes,
    Integrity,
    LocalRecords,
    QueryCompiler,
    ReplicationJobs,
    Revisions,
    Schema,
    StructuredIndexes
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

      {:error, reason} ->
        {:error, normalize_error(reason)}
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
    do: transaction(adapter, fn -> apply_local_mutation_tx(adapter, request) end)

  def apply_local_mutation(_adapter, _request),
    do: {:error, ElixirDB.Error.invalid_request("mutation request must be an object")}

  @impl true
  def apply_bulk_mutation(adapter, %{operations: operations}) do
    with :ok <- validate_operation_batch(operations) do
      transaction(adapter, fn -> bulk_tx(adapter, operations) end)
    end
  end

  def apply_bulk_mutation(adapter, %{"operations" => operations}),
    do: apply_bulk_mutation(adapter, %{operations: operations})

  def apply_bulk_mutation(_adapter, _request),
    do: {:error, ElixirDB.Error.invalid_request("bulk mutation request must be an object")}

  @impl true
  def resolve_conflict(adapter, request) when is_map(request),
    do: transaction(adapter, fn -> resolve_conflict_tx(adapter, request) end)

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
  def diff_revisions(%__MODULE__{} = adapter, request) when is_map(request) do
    documents = request[:documents] || request["documents"] || []

    with :ok <- validate_documents_batch(documents),
         {:ok, result} <-
           Enum.reduce_while(documents, {:ok, []}, fn entry, {:ok, acc} ->
             id = entry[:document_id] || entry["document_id"]
             leaves_requested = entry[:leaf_revisions] || entry["leaf_revisions"] || []

             case Documents.find(adapter.conn, id) do
               {:ok, nil} ->
                 {:cont, {:ok, [%{document_id: id, missing_revisions: leaves_requested} | acc]}}

               {:ok, doc} ->
                 existing =
                   Revisions.leaves(adapter.conn, doc.doc_key)
                   |> Enum.map(& &1.revision_id)
                   |> MapSet.new()

                 {:cont,
                  {:ok,
                   [
                     %{
                       document_id: id,
                       missing_revisions:
                         Enum.reject(leaves_requested, &MapSet.member?(existing, &1))
                     }
                     | acc
                   ]}}

               {:error, error} ->
                 {:halt, {:error, error}}
             end
           end)
           |> then(fn
             {:ok, values} -> {:ok, Enum.reverse(values)}
             error -> error
           end) do
      {:ok, %{documents: result}}
    end
  end

  def diff_revisions(_adapter, _request),
    do: {:error, ElixirDB.Error.invalid_request("revision diff request must be an object")}

  @impl true
  def get_revision_chains(%__MODULE__{} = adapter, request) when is_map(request) do
    documents = request[:documents] || request["documents"] || []

    with :ok <- validate_documents_batch(documents),
         {:ok, chains} <-
           Enum.reduce_while(documents, {:ok, []}, fn entry, {:ok, acc} ->
             id = entry[:document_id] || entry["document_id"]
             requested = entry[:leaf_revisions] || entry["leaf_revisions"] || []

             case Documents.find(adapter.conn, id) do
               {:ok, nil} ->
                 {:cont, {:ok, acc}}

               {:ok, doc} ->
                 case Enum.reduce_while(requested, {:ok, []}, fn leaf, {:ok, chain_acc} ->
                        case chain_for_leaf(adapter, doc, leaf) do
                          {:ok, chain} -> {:cont, {:ok, [chain | chain_acc]}}
                          {:error, error} -> {:halt, {:error, error}}
                        end
                      end) do
                   {:ok, values} -> {:cont, {:ok, Enum.reverse(values) ++ acc}}
                   {:error, error} -> {:halt, {:error, error}}
                 end

               {:error, error} ->
                 {:halt, {:error, error}}
             end
           end)
           |> then(fn
             {:ok, values} -> {:ok, Enum.reverse(values)}
             error -> error
           end) do
      {:ok, %{chains: chains}}
    end
  end

  def get_revision_chains(_adapter, _request),
    do: {:error, ElixirDB.Error.invalid_request("revision chain request must be an object")}

  @impl true
  def import_revision_chains(adapter, request) when is_map(request) do
    with :ok <- validate_chain_batch(request[:chains] || request["chains"] || []) do
      transaction(adapter, fn -> import_revision_chains_tx(adapter, request) end)
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
    definition = Map.delete(definition, "definition_digest") |> Map.delete(:definition_digest)

    transaction(%__MODULE__{conn: conn}, fn ->
      with {:ok, definition_json} <- Canonical.encode(definition),
           digest <- :crypto.hash(:sha256, definition_json) |> Base.encode16(case: :lower),
           id <- "idx_" <> binary_part(digest, 0, 24),
           {:ok, existing} <-
             find_index_by_name(conn, index_name(definition), definition_json, digest),
           {:ok, result} <-
             (case existing do
                nil ->
                  with {:ok, metadata} <- create_physical_index(conn, id, definition),
                       {:ok, metadata_json} <- Canonical.encode(metadata),
                       :ok <-
                         Connection.execute(
                           conn,
                           "INSERT INTO index_definitions(index_id, name, index_type, definition_digest, definition_json, lifecycle_state, adapter_metadata_json) VALUES (?, ?, ?, ?, ?, 'ready', ?)",
                           [
                             id,
                             index_name(definition),
                             index_type(definition),
                             digest,
                             definition_json,
                             metadata_json
                           ]
                         ) do
                    {:ok, index_result(definition, id, digest)}
                  end

                existing ->
                  {:ok, existing}
              end) do
        {:ok, result}
      else
        {:error, reason} -> {:error, normalize_error(reason)}
      end
    end)
  end

  @impl true
  def delete_index(%__MODULE__{conn: conn}, index_id) do
    transaction(%__MODULE__{conn: conn}, fn ->
      with {:ok, row} <- find_index(conn, index_id),
           {:ok, metadata} <- decode_index_metadata(row),
           :ok <- drop_physical_index(conn, metadata),
           :ok <-
             Connection.execute(conn, "DELETE FROM index_definitions WHERE index_id = ?", [index_id]) do
        {:ok, %{index_id: index_id, deleted: true}}
      end
    end)
  end

  @impl true
  def rebuild_index(%__MODULE__{conn: conn}, index_id) do
    transaction(%__MODULE__{conn: conn}, fn ->
      with {:ok, row} <- find_index(conn, index_id),
           {:ok, definition} <- decode_json(row.definition_json),
           {:ok, old_metadata} <- decode_index_metadata(row),
           :ok <- drop_physical_index(conn, old_metadata),
           {:ok, new_metadata} <- create_physical_index(conn, index_id, definition),
           {:ok, metadata_json} <- Canonical.encode(new_metadata),
           :ok <-
             Connection.execute(
               conn,
               "UPDATE index_definitions SET lifecycle_state = 'ready', adapter_metadata_json = ? WHERE index_id = ?",
               [metadata_json, index_id]
             ) do
        {:ok, %{index_id: index_id, rebuilt: true}}
      end
    end)
  end

  @impl true
  def list_indexes(%__MODULE__{conn: conn}) do
    with {:ok, rows} <-
           Connection.query(
             conn,
             "SELECT index_id, definition_json, definition_digest, lifecycle_state, adapter_metadata_json FROM index_definitions ORDER BY index_id"
           ) do
      {:ok,
       Enum.map(rows, fn [id, json, digest_value, state, metadata_json] ->
         definition = decode_json!(json)
         metadata = decode_json!(metadata_json)

         definition
         |> Map.merge(%{
           "index_id" => id,
           "definition_digest" => digest_value,
           "lifecycle_state" => state,
           "_metadata" => metadata
         })
       end)}
    else
      {:error, reason} -> {:error, normalize_error(reason)}
    end
  end

  @impl true
  def execute_query(%__MODULE__{} = adapter, request) when is_map(request) do
    started = System.monotonic_time(:millisecond)
    result = execute_query_impl(adapter, request)
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
  def explain_query(%__MODULE__{} = adapter, request) when is_map(request) do
    with {:ok, indexes} <- list_indexes(adapter),
         {:ok, selected} <- Planner.select_index(indexes, request),
         {:ok, [[count]]} <-
           Connection.query(
             adapter.conn,
             "SELECT count(*) FROM documents WHERE winning_deleted = 0"
           ),
         identity <- adapter_identity(adapter),
         scan_threshold <- get_in(identity, [:config, "queries", "scan_threshold"]) || 1_000 do
      {:ok,
       %{
         selected_index: selected && selected["index_id"],
         candidate_indexes: Enum.map(indexes, & &1["index_id"]),
         rejected_index_reasons: rejected_index_reasons(indexes, selected, request),
         full_scan: is_nil(selected),
         candidate_count: count,
         scan_allowed: not is_nil(selected) or count <= scan_threshold,
         selector: request[:selector] || request["selector"] || %{},
         sort: request[:sort] || request["sort"] || [],
         pagination: if(selected, do: :indexed, else: :bounded_scan)
       }}
    else
      {:error, reason} -> {:error, normalize_error(reason)}
    end
  end

  def explain_query(_adapter, _request),
    do: {:error, ElixirDB.Error.invalid_request("query explanation must be an object")}

  defp execute_query_impl(%__MODULE__{} = adapter, request) do
    with {:ok, indexes} <- list_indexes(adapter),
         {:ok, selected} <- Planner.select_index(indexes, request),
         {:ok, documents, examined} <- candidate_documents(adapter, selected, request),
         {:ok, matched} <- filter_query(documents, request),
         identity <- adapter_identity(adapter),
         :ok <- enforce_scan_limit(selected, examined, identity),
         limit <-
           request[:limit] || request["limit"] ||
             get_in(identity, [:config, "queries", "default_limit"]) || 50,
         ordered <- matched |> sort_documents(request) |> apply_after_cursor(request),
         values <- Enum.take(ordered, limit),
         {:ok, projected} <- project_documents(values, request) do
      {:ok,
       %{
         results: projected,
         documents: projected,
         bookmark: nil,
         has_more: length(ordered) > limit,
         examined: examined,
         sequence: identity.current_sequence,
         selected_index: selected && selected["index_id"],
         index_digest: selected && selected["definition_digest"],
         last_ordering_key: ordering_key(List.last(values), request)
       }}
    else
      {:error, %ElixirDB.Error{} = error} -> {:error, error}
      {:error, reason} -> {:error, normalize_error(reason)}
    end
  end

  defp candidate_documents(adapter, nil, _request) do
    with {:ok, [[count]]} <-
           Connection.query(
             adapter.conn,
             "SELECT count(*) FROM documents WHERE winning_deleted = 0"
           ),
         :ok <-
           if(
             count <=
               (get_in(adapter_identity(adapter), [:config, "queries", "scan_threshold"]) || 1_000),
             do: :ok,
             else:
               {:error,
                ElixirDB.Error.index_required("query requires a compatible index", %{
                  candidate_count: count,
                  threshold:
                    get_in(adapter_identity(adapter), [:config, "queries", "scan_threshold"]) ||
                      1_000
                })}
           ),
         {:ok, rows} <-
           Connection.query(
             adapter.conn,
             "SELECT document_id, winning_revision, winning_body_json FROM documents WHERE winning_deleted = 0 ORDER BY document_id"
           ),
         {:ok, documents} <- decode_query_documents(rows) do
      {:ok, documents, count}
    end
  end

  defp candidate_documents(adapter, selected, request) do
    if selected["type"] == "full_text" and (request[:search] || request["search"]) do
      search = request[:search]
      metadata = Map.merge(selected, selected["_metadata"] || %{})

      with {:ok, rows} <-
             FullTextIndexes.search(
               adapter.conn,
               metadata,
               search[:text] || search["text"],
               search[:mode] || search["mode"] || "all"
             ) do
        {:ok,
         Enum.map(rows, fn row ->
           %{id: row.id, revision: row.revision, body: row.body, rank: row.rank}
         end), length(rows)}
      end
    else
      with {:ok, rows} <- structured_candidate_rows(adapter, selected, request),
           {:ok, documents} <- decode_query_documents(rows) do
        {:ok, documents, length(documents)}
      end
    end
  end

  defp structured_candidate_rows(adapter, selected, request) do
    fields = selected["fields"] || []
    selector = request[:selector] || request["selector"] || %{}
    conditions = structured_conditions(selector, fields)

    {where, params} =
      Enum.reduce(conditions, {"winning_deleted = 0", []}, fn {sql, value}, {where, params} ->
        {where <> " AND " <> sql, params ++ List.wrap(value)}
      end)

    Connection.query(
      adapter.conn,
      "SELECT document_id, winning_revision, winning_body_json FROM documents WHERE #{where} ORDER BY document_id",
      params
    )
  end

  defp structured_conditions(selector, fields) do
    Enum.flat_map(selector, fn
      {"$and", clauses} when is_list(clauses) ->
        Enum.flat_map(clauses, &structured_conditions(&1, fields))

      {path, condition} ->
        case Enum.find(fields, fn field -> field["path"] == path end) do
          nil -> []
          field -> field_condition(path, condition, field["type"])
        end
    end)
  end

  defp field_condition(path, condition, type) when is_map(condition) do
    Enum.flat_map(condition, fn
      {operator, value} when operator in ["$eq", "$gt", "$gte", "$lt", "$lte"] ->
        scalar_condition(path, operator, value, type)

      {"$in", values} when is_list(values) ->
        values = Enum.flat_map(values, &scalar_condition(path, "$eq", &1, type))

        case values do
          [] ->
            []

          _ ->
            [
              {"(" <> Enum.map_join(values, " OR ", &elem(&1, 0)) <> ")",
               Enum.map(values, &elem(&1, 1))}
            ]
        end

      _ ->
        []
    end)
  end

  defp field_condition(path, value, type), do: scalar_condition(path, "$eq", value, type)

  defp scalar_condition(path, operator, value, type) do
    if type_matches?(value, type) do
      expression = QueryCompiler.json_expression(path)
      comparison = operator_sql(operator)
      type_sql = json_type_sql(path, type)
      [{"(#{type_sql} AND #{expression} #{comparison} ?)", value}]
    else
      []
    end
  end

  defp rejected_index_reasons(indexes, selected, request) do
    Enum.map(indexes, fn index ->
      {index["index_id"],
       if(selected && selected["index_id"] == index["index_id"], do: :selected, else: :incompatible)}
    end)
    |> Map.new()
    |> Map.put(:request, request[:index] || request["index"])
  end

  defp enforce_scan_limit(selected, _examined, _identity) when not is_nil(selected), do: :ok

  defp enforce_scan_limit(nil, examined, identity) do
    threshold = get_in(identity, [:config, "queries", "scan_threshold"]) || 1_000

    if examined <= threshold,
      do: :ok,
      else:
        {:error,
         ElixirDB.Error.index_required("query requires a compatible index", %{
           candidate_count: examined,
           threshold: threshold
         })}
  end

  defp project_documents(values, request),
    do: {:ok, Enum.map(values, &Projection.project(&1, request))}

  defp apply_local_mutation_tx(adapter, request) do
    operation = request[:operation] || request["operation"] || :put
    document_id = request[:document_id] || request["document_id"]
    if_revision = Map.get(request, :if_revision, Map.get(request, "if_revision"))
    deleted = operation in [:delete, "delete"]
    body = if deleted, do: nil, else: request[:body] || request["body"]

    with :ok <- validate_document_input(adapter, document_id, deleted, body),
         {:ok, doc} <- Documents.find(adapter.conn, document_id),
         {:ok, current} <- current_winner(adapter, doc),
         {:ok, candidate_state} <-
           candidate_revision(adapter, doc, document_id, if_revision, current, deleted, body) do
      case candidate_state do
        {:replayed, candidate} ->
          {:ok, %{revision: candidate.revision_id, sequence: doc.update_sequence, replayed: true}}

        candidate ->
          insert_local_revision(adapter, doc, candidate, if_revision, operation)
      end
    end
  end

  defp validate_document_input(adapter, document_id, deleted, body) do
    max_id =
      get_in(adapter_identity(adapter), [:config, "documents", "max_document_id_bytes"]) || 512

    max_body =
      get_in(adapter_identity(adapter), [:config, "documents", "max_document_bytes"]) || 1_048_576

    body_size_error =
      if not deleted and is_map(body) do
        case Canonical.encode(body) do
          {:ok, json} when byte_size(json) <= max_body -> nil
          {:ok, _json} -> :too_large
          {:error, _error} -> :invalid
        end
      else
        nil
      end

    cond do
      not is_binary(document_id) or document_id == "" or not String.valid?(document_id) ->
        {:error, ElixirDB.Error.invalid_request("document id must be a non-empty UTF-8 string")}

      byte_size(document_id) > max_id ->
        {:error, ElixirDB.Error.resource_limit("document id exceeds the configured limit")}

      String.contains?(document_id, <<0>>) or String.starts_with?(document_id, "_system/") or
          Enum.any?(String.to_charlist(document_id), &(&1 < 0x20)) ->
        {:error, ElixirDB.Error.invalid_request("document id contains a reserved character")}

      deleted and not is_nil(body) ->
        {:error, ElixirDB.Error.invalid_request("deleted revisions must not contain a body")}

      not deleted and not is_map(body) ->
        {:error, ElixirDB.Error.invalid_request("document body must be an object")}

      body_size_error == :too_large ->
        {:error, ElixirDB.Error.resource_limit("document body exceeds the configured limit")}

      body_size_error == :invalid ->
        {:error, ElixirDB.Error.invalid_request("document body must contain canonical JSON values")}

      true ->
        :ok
    end
  end

  defp document_mutation_result(adapter, doc, winner) do
    with {:ok, leaves_now} <- Revisions.load_leaves(adapter.conn, doc.doc_key) do
      {:ok,
       %{
         revision: winner.revision_id,
         sequence: doc.update_sequence,
         document_id: doc.document_id,
         deleted: winner.deleted,
         conflicts: Winner.conflicts(leaves_now, winner)
       }}
    end
  end

  defp candidate_revision(adapter, doc, document_id, if_revision, current, deleted, body) do
    cond do
      is_nil(current) and not is_nil(if_revision) ->
        {:error, ElixirDB.Error.revision_conflict("document does not have the expected parent")}

      true ->
        with {:ok, revision} <- build_candidate(document_id, if_revision, deleted, body) do
          cond do
            is_nil(current) or if_revision == current.revision_id ->
              {:ok, revision}

            true ->
              case Revisions.find(adapter.conn, doc.doc_key, revision.revision_id) do
                {:ok, existing} ->
                  if Revisions.same?(existing, revision) do
                    {:ok, {:replayed, revision}}
                  else
                    stale_revision_error(if_revision, current, revision)
                  end

                _ ->
                  stale_revision_error(if_revision, current, revision)
              end
          end
        end
    end
  end

  defp stale_revision_error(if_revision, current, revision) do
    {:error,
     ElixirDB.Error.revision_conflict("revision is stale", %{
       expected_revision: if_revision,
       observed_revision: current.revision_id,
       candidate_revision: revision.revision_id,
       operation_already_committed: false
     })}
  end

  defp build_candidate(document_id, parent_revision, deleted, body) do
    with {:ok, revision_id} <- Id.calculate(document_id, parent_revision, deleted, body),
         {:ok, generation} <- Id.generation(revision_id) do
      {:ok,
       %Revision{
         document_id: document_id,
         revision_id: revision_id,
         generation: generation,
         parent_revision: parent_revision,
         digest: digest(revision_id),
         deleted: deleted,
         body: body
       }}
    end
  end

  defp insert_local_revision(adapter, nil, %Revision{} = candidate, _if_revision, _operation) do
    with {:ok, doc_key} <- Documents.insert(adapter.conn, candidate.document_id),
         :ok <- Revisions.insert(adapter.conn, doc_key, candidate),
         {:ok, result} <- finalize_document(adapter, doc_key, candidate.document_id, candidate) do
      {:ok, Map.put(result, :replayed, false)}
    end
  end

  defp insert_local_revision(adapter, doc, %Revision{} = candidate, _if_revision, _operation) do
    case Revisions.find(adapter.conn, doc.doc_key, candidate.revision_id) do
      {:ok, existing} ->
        if Revisions.same?(existing, candidate) do
          if doc.winning_revision == candidate.revision_id,
            do:
              {:ok,
               %{revision: candidate.revision_id, sequence: doc.update_sequence, replayed: true}},
            else:
              {:error,
               ElixirDB.Error.revision_conflict("operation was already committed", %{
                 operation_already_committed: true,
                 revision: candidate.revision_id
               })}
        else
          {:error, ElixirDB.Error.integrity_violation("revision id has different content")}
        end

      {:error, %ElixirDB.Error{code: :revision_not_found}} ->
        with :ok <- Revisions.ensure_parent(adapter.conn, doc.doc_key, candidate.parent_revision),
             :ok <- Revisions.insert(adapter.conn, doc.doc_key, candidate),
             {:ok, result} <-
               finalize_document(adapter, doc.doc_key, candidate.document_id, candidate) do
          {:ok, Map.put(result, :replayed, false)}
        end

      {:error, error} ->
        {:error, error}
    end
  end

  defp finalize_document(adapter, doc_key, document_id, _candidate) do
    with {:ok, all_leaves} <- Revisions.load_leaves(adapter.conn, doc_key),
         {:ok, winner} <- Winner.select(all_leaves),
         {:ok, leaf_json} <- Revisions.encode_leaf_set(all_leaves),
         :ok <- Documents.update(adapter.conn, doc_key, winner, 0),
         :ok <- refresh_indexes(adapter, doc_key, winner),
         {:ok, sequence} <- Changes.allocate_sequence(adapter.conn),
         :ok <- Documents.update(adapter.conn, doc_key, winner, sequence),
         :ok <-
           Changes.insert(adapter.conn, sequence, doc_key, document_id, winner, leaf_json, "local") do
      publishless = %{
        revision: winner.revision_id,
        sequence: sequence,
        document_id: document_id,
        deleted: winner.deleted,
        conflicts: Winner.conflicts(all_leaves, winner)
      }

      {:ok, publishless}
    end
  end

  defp bulk_tx(adapter, operations) when is_list(operations) do
    with {:ok, effects, affected} <- prepare_bulk_operations(adapter, operations),
         {:ok, finalized} <- finalize_bulk_documents(adapter, affected) do
      {:ok,
       Enum.map(effects, fn effect ->
         result = Map.get(finalized, effect.doc_key, effect.result)
         Map.put(result, :replayed, effect.replayed)
       end)}
    end
  end

  defp prepare_bulk_operations(adapter, operations) do
    Enum.reduce_while(operations, {:ok, [], %{}}, fn operation, {:ok, effects, affected} ->
      case prepare_bulk_operation(adapter, operation) do
        {:ok, effect} ->
          affected =
            if(effect.changed,
              do: Map.put(affected, effect.doc_key, effect.document_id),
              else: affected
            )

          {:cont, {:ok, [effect | effects], affected}}

        {:error, error} ->
          {:halt, {:error, error}}
      end
    end)
    |> case do
      {:ok, effects, affected} -> {:ok, Enum.reverse(effects), affected}
      error -> error
    end
  end

  defp prepare_bulk_operation(adapter, request) do
    operation = request[:operation] || request["operation"] || :put

    if operation in [:resolve, "resolve"],
      do: prepare_bulk_resolution_operation(adapter, request),
      else: prepare_bulk_mutation_operation(adapter, request)
  end

  defp prepare_bulk_mutation_operation(adapter, request) do
    operation = request[:operation] || request["operation"] || :put
    document_id = request[:document_id] || request["document_id"]
    if_revision = Map.get(request, :if_revision, Map.get(request, "if_revision"))
    deleted = operation in [:delete, "delete"]
    body = if(deleted, do: nil, else: request[:body] || request["body"])

    with :ok <- validate_document_input(adapter, document_id, deleted, body),
         {:ok, doc} <- Documents.find(adapter.conn, document_id),
         {:ok, current} <- current_winner(adapter, doc),
         {:ok, candidate_state} <-
           candidate_revision(adapter, doc, document_id, if_revision, current, deleted, body) do
      case doc do
        nil ->
          candidate = candidate_state

          with {:ok, doc_key} <- Documents.insert(adapter.conn, document_id),
               :ok <- Revisions.insert(adapter.conn, doc_key, candidate),
               :ok <- update_pending_document(adapter, doc_key, candidate) do
            {:ok,
             %{
               doc_key: doc_key,
               document_id: document_id,
               changed: true,
               replayed: false,
               result: %{revision: candidate.revision_id, sequence: 0}
             }}
          end

        doc when is_tuple(candidate_state) ->
          {:replayed, candidate} = candidate_state

          {:ok,
           %{
             doc_key: doc.doc_key,
             document_id: document_id,
             changed: false,
             replayed: true,
             result: %{revision: candidate.revision_id, sequence: doc.update_sequence}
           }}

        doc ->
          candidate = candidate_state

          case Revisions.find(adapter.conn, doc.doc_key, candidate.revision_id) do
            {:ok, existing} ->
              if Revisions.same?(existing, candidate) do
                if doc.winning_revision == candidate.revision_id do
                  {:ok,
                   %{
                     doc_key: doc.doc_key,
                     document_id: document_id,
                     changed: false,
                     replayed: true,
                     result: %{revision: candidate.revision_id, sequence: doc.update_sequence}
                   }}
                else
                  {:error,
                   ElixirDB.Error.revision_conflict("operation was already committed", %{
                     operation_already_committed: true,
                     revision: candidate.revision_id
                   })}
                end
              else
                {:error, ElixirDB.Error.integrity_violation("revision id has different content")}
              end

            {:error, %ElixirDB.Error{code: :revision_not_found}} ->
              with :ok <-
                     Revisions.ensure_parent(adapter.conn, doc.doc_key, candidate.parent_revision),
                   :ok <- Revisions.insert(adapter.conn, doc.doc_key, candidate),
                   :ok <- update_pending_document(adapter, doc.doc_key, candidate) do
                {:ok,
                 %{
                   doc_key: doc.doc_key,
                   document_id: document_id,
                   changed: true,
                   replayed: false,
                   result: %{revision: candidate.revision_id, sequence: 0}
                 }}
              end

            {:error, error} ->
              {:error, error}
          end
      end
    end
  end

  defp prepare_bulk_resolution_operation(adapter, request) do
    document_id = request[:document_id] || request["document_id"]
    expected = request[:expected_live_revisions] || request["expected_live_revisions"] || []
    body = Map.get(request, :body, Map.get(request, "body"))
    delete_all = Map.get(request, :delete_all, Map.get(request, "delete_all", false))

    with true <- is_boolean(delete_all),
         :ok <-
           validate_document_input(
             adapter,
             document_id,
             delete_all,
             if(delete_all, do: nil, else: body)
           ),
         true <- is_list(expected) and Enum.all?(expected, &is_binary/1),
         {:ok, %{doc_key: _} = doc} <- Documents.find(adapter.conn, document_id),
         {:ok, leaves} <- Revisions.load_leaves(adapter.conn, doc.doc_key),
         :ok <- ElixirDB.Revisions.ConflictResolution.validate_leaf_set(leaves, expected),
         {:ok, revisions} <-
           build_resolution_revisions(document_id, leaves, request, body, delete_all),
         {:ok, status} <- resolution_status(adapter, doc.doc_key, revisions) do
      case status do
        :replayed ->
          {:ok, winner} = current_winner(adapter, doc)

          {:ok,
           %{
             doc_key: doc.doc_key,
             document_id: document_id,
             changed: false,
             replayed: true,
             result: %{revision: winner.revision_id, sequence: doc.update_sequence}
           }}

        :new ->
          with :ok <- insert_resolution_revisions(adapter, doc.doc_key, revisions),
               :ok <- update_pending_document(adapter, doc.doc_key, List.first(revisions)) do
            {:ok,
             %{
               doc_key: doc.doc_key,
               document_id: document_id,
               changed: true,
               replayed: false,
               result: %{revision: List.first(revisions).revision_id, sequence: 0}
             }}
          end
      end
    else
      {:ok, nil} ->
        {:error, ElixirDB.Error.document_not_found("document does not exist")}

      false ->
        {:error,
         ElixirDB.Error.invalid_request("bulk conflict resolution requires a revision array")}

      {:error, error} ->
        {:error, error}
    end
  end

  defp update_pending_document(adapter, doc_key, _candidate) do
    with {:ok, leaves_now} <- Revisions.load_leaves(adapter.conn, doc_key),
         {:ok, winner} <- Winner.select(leaves_now),
         :ok <- Documents.update(adapter.conn, doc_key, winner, 0),
         :ok <- refresh_indexes(adapter, doc_key, winner) do
      :ok
    end
  end

  defp finalize_bulk_documents(adapter, affected) do
    affected
    |> Enum.sort_by(fn {_doc_key, document_id} -> document_id end)
    |> Enum.reduce_while({:ok, %{}}, fn {doc_key, document_id}, {:ok, results} ->
      case finalize_document(adapter, doc_key, document_id, nil) do
        {:ok, result} -> {:cont, {:ok, Map.put(results, doc_key, result)}}
        {:error, error} -> {:halt, {:error, error}}
      end
    end)
  end

  defp resolve_conflict_tx(adapter, request) do
    document_id = request[:document_id] || request["document_id"]
    expected = request[:expected_live_revisions] || request["expected_live_revisions"] || []
    body = request[:body] || request["body"]
    delete_all = request[:delete_all] || request["delete_all"] || false

    with true <- is_boolean(delete_all),
         :ok <-
           validate_document_input(
             adapter,
             document_id,
             delete_all,
             if(delete_all, do: nil, else: body)
           ),
         {:ok, %{doc_key: _} = doc} <- Documents.find(adapter.conn, document_id),
         {:ok, current_leaves} <- Revisions.load_leaves(adapter.conn, doc.doc_key),
         :ok <- ElixirDB.Revisions.ConflictResolution.validate_leaf_set(current_leaves, expected),
         {:ok, revisions} <-
           build_resolution_revisions(document_id, current_leaves, request, body, delete_all),
         {:ok, status} <- resolution_status(adapter, doc.doc_key, revisions) do
      case status do
        :replayed ->
          with {:ok, winner} <- current_winner(adapter, doc),
               {:ok, result} <- document_mutation_result(adapter, doc, winner) do
            {:ok, Map.put(result, :replayed, true)}
          end

        :new ->
          with :ok <- insert_resolution_revisions(adapter, doc.doc_key, revisions),
               {:ok, result} <-
                 finalize_document(adapter, doc.doc_key, document_id, List.first(revisions)) do
            {:ok, Map.put(result, :replayed, false)}
          end
      end
    else
      {:ok, nil} ->
        {:error, ElixirDB.Error.document_not_found("document does not exist")}

      {:error, error} ->
        {:error, error}
    end
  end

  defp resolution_status(adapter, doc_key, revisions) do
    statuses =
      Enum.map(revisions, fn revision ->
        case Revisions.find(adapter.conn, doc_key, revision.revision_id) do
          {:ok, existing} ->
            if(Revisions.same?(existing, revision), do: :existing, else: :different)

          {:error, %ElixirDB.Error{code: :revision_not_found}} ->
            :missing

          {:error, _} ->
            :different
        end
      end)

    cond do
      Enum.any?(statuses, &(&1 == :different)) ->
        {:error, ElixirDB.Error.integrity_violation("resolution revision id has different content")}

      statuses != [] and Enum.all?(statuses, &(&1 == :existing)) ->
        {:ok, :replayed}

      Enum.any?(statuses, &(&1 == :existing)) ->
        {:error,
         ElixirDB.Error.revision_conflict("conflict resolution replay is only partially present")}

      true ->
        {:ok, :new}
    end
  end

  defp insert_resolution_revisions(adapter, doc_key, revisions) do
    Enum.reduce_while(revisions, :ok, fn revision, :ok ->
      case Revisions.insert_or_accept(adapter.conn, doc_key, revision) do
        :ok -> {:cont, :ok}
        {:error, error} -> {:halt, {:error, error}}
      end
    end)
  end

  defp build_resolution_revisions(document_id, leaves, request, body, delete_all) do
    chosen = request[:chosen_parent_revision] || request["chosen_parent_revision"]
    live = Enum.reject(leaves, & &1.deleted)

    cond do
      live == [] ->
        {:error, ElixirDB.Error.revision_conflict("document has no current live leaves")}

      delete_all ->
        Enum.map(live, fn leaf -> build_revision(document_id, leaf.revision_id, true, nil) end)
        |> collect_ok()

      true ->
        with true <- chosen in Enum.map(live, & &1.revision_id),
             {:ok, survivor} <- build_revision(document_id, chosen, false, body),
             {:ok, tombstones} <-
               live
               |> Enum.reject(&(&1.revision_id == chosen))
               |> Enum.map(&build_revision(document_id, &1.revision_id, true, nil))
               |> collect_ok() do
          {:ok, [survivor | tombstones]}
        else
          false ->
            {:error, ElixirDB.Error.revision_conflict("chosen parent is not a current live leaf")}

          {:error, error} ->
            {:error, error}
        end
    end
  end

  defp build_revision(document_id, parent, deleted, body) do
    with {:ok, id} <- Id.calculate(document_id, parent, deleted, body),
         {:ok, generation} <- Id.generation(id),
         do:
           {:ok,
            %Revision{
              document_id: document_id,
              revision_id: id,
              generation: generation,
              parent_revision: parent,
              digest: digest(id),
              deleted: deleted,
              body: body
            }}
  end

  defp collect_ok(values) do
    Enum.reduce_while(values, {:ok, []}, fn
      {:ok, value}, {:ok, acc} -> {:cont, {:ok, [value | acc]}}
      {:error, error}, _ -> {:halt, {:error, error}}
    end)
  end

  defp import_revision_chains_tx(adapter, request) do
    chains = request[:chains] || request["chains"] || []

    with {:ok, revisions} <- validate_chains(chains),
         {:ok, %{affected: affected, inserted: inserted}} <-
           insert_imported_revisions(adapter, revisions),
         {:ok, result} <- finalize_imports(adapter, affected, inserted) do
      {:ok, result}
    end
  end

  defp validate_chains(chains) do
    Enum.reduce_while(chains, {:ok, []}, fn chain, {:ok, acc} ->
      if is_map(chain) do
        document_id = chain[:document_id] || chain["document_id"]
        leaf_revision = chain[:leaf_revision] || chain["leaf_revision"]
        revisions = chain[:revisions] || chain["revisions"] || []

        allowed_keys = [
          :document_id,
          :leaf_revision,
          :revisions,
          "document_id",
          "leaf_revision",
          "revisions"
        ]

        if Enum.all?(Map.keys(chain), &(&1 in allowed_keys)) and is_binary(document_id) and
             document_id != "" and is_binary(leaf_revision) and is_list(revisions) and
             revisions != [] do
          case Enum.reduce_while(revisions, {:ok, :root, []}, fn raw,
                                                                 {:ok, expected_parent, built} ->
                 with {:ok, revision} <- imported_revision(document_id, raw),
                      true <-
                        (expected_parent == :root and is_nil(revision.parent_revision)) or
                          revision.parent_revision == expected_parent do
                   {:cont, {:ok, revision.revision_id, [revision | built]}}
                 else
                   false ->
                     {:halt,
                      {:error,
                       ElixirDB.Error.integrity_violation("revision chain is not parent ordered")}}

                   {:error, error} ->
                     {:halt, {:error, error}}
                 end
               end) do
            {:ok, parent, built} when parent == leaf_revision ->
              {:cont, {:ok, Enum.reverse(built) ++ acc}}

            {:ok, _parent, _built} ->
              {:halt,
               {:error,
                ElixirDB.Error.integrity_violation("revision chain leaf does not match payload")}}

            {:error, error} ->
              {:halt, {:error, error}}
          end
        else
          {:halt,
           {:error,
            ElixirDB.Error.invalid_request("revision chain requires document, leaf, and revisions")}}
        end
      else
        {:halt, {:error, ElixirDB.Error.invalid_request("revision chains must be objects")}}
      end
    end)
    |> case do
      {:ok, revisions} -> {:ok, revisions}
      error -> error
    end
  end

  defp imported_revision(document_id, raw) do
    if not is_map(raw) do
      {:error, ElixirDB.Error.invalid_request("revision chain entries must be objects")}
    else
      allowed_keys = [
        :document_id,
        :revision_id,
        :generation,
        :parent_revision,
        :deleted,
        :body,
        "document_id",
        "revision_id",
        "generation",
        "parent_revision",
        "deleted",
        "body"
      ]

      if Enum.all?(Map.keys(raw), &(&1 in allowed_keys)) do
        generation_value = raw[:generation] || raw["generation"]
        revision_id = raw[:revision_id] || raw["revision_id"]
        parent = raw[:parent_revision] || raw["parent_revision"]
        deleted = Map.get(raw, :deleted, Map.get(raw, "deleted", false))
        body = Map.get(raw, :body, Map.get(raw, "body"))

        with {:ok, calculated} <- Id.calculate(document_id, parent, deleted, body),
             true <- calculated == revision_id,
             {:ok, generation} <- Id.generation(revision_id),
             true <- generation_value == generation,
             true <- is_boolean(deleted),
             true <- deleted or is_map(body),
             true <- deleted or body_size_within_limit?(body) do
          {:ok,
           %Revision{
             document_id: document_id,
             revision_id: revision_id,
             generation: generation,
             parent_revision: parent,
             digest: digest(revision_id),
             deleted: deleted,
             body: body
           }}
        else
          false -> {:error, ElixirDB.Error.integrity_violation("revision chain validation failed")}
          {:error, error} -> {:error, error}
        end
      else
        {:error, ElixirDB.Error.invalid_request("revision chain entry contains an unknown field")}
      end
    end
  end

  defp body_size_within_limit?(body) do
    case Canonical.encode(body) do
      {:ok, json} ->
        byte_size(json) <= (ElixirDB.Config.host_limits()[:max_document_bytes] || 1_048_576)

      {:error, _} ->
        false
    end
  end

  defp insert_imported_revisions(adapter, revisions) do
    Enum.reduce_while(revisions, {:ok, MapSet.new(), 0}, fn revision, {:ok, affected, inserted} ->
      case Documents.find(adapter.conn, revision.document_id) do
        {:ok, nil} ->
          with {:ok, doc_key} <- Documents.insert(adapter.conn, revision.document_id),
               :ok <- Revisions.insert(adapter.conn, doc_key, revision) do
            {:cont, {:ok, MapSet.put(affected, {doc_key, revision.document_id}), inserted + 1}}
          end

        {:error, %ElixirDB.Error{code: :document_not_found}} ->
          with {:ok, doc_key} <- Documents.insert(adapter.conn, revision.document_id),
               :ok <- Revisions.insert(adapter.conn, doc_key, revision) do
            {:cont, {:ok, MapSet.put(affected, {doc_key, revision.document_id}), inserted + 1}}
          end

        {:ok, doc} ->
          case Revisions.find(adapter.conn, doc.doc_key, revision.revision_id) do
            {:ok, existing} ->
              if Revisions.same?(existing, revision),
                do: {:cont, {:ok, affected, inserted}},
                else:
                  {:halt,
                   {:error,
                    ElixirDB.Error.integrity_violation(
                      "existing revision differs from imported revision"
                    )}}

            {:error, %ElixirDB.Error{code: :revision_not_found}} ->
              with :ok <-
                     Revisions.ensure_parent(adapter.conn, doc.doc_key, revision.parent_revision),
                   :ok <- Revisions.insert(adapter.conn, doc.doc_key, revision),
                   do:
                     {:cont,
                      {:ok, MapSet.put(affected, {doc.doc_key, revision.document_id}), inserted + 1}}

            {:error, error} ->
              {:halt, {:error, error}}
          end

        {:error, error} ->
          {:halt, {:error, error}}
      end
    end)
    |> case do
      {:ok, affected, inserted} -> {:ok, %{affected: affected, inserted: inserted}}
      {:error, _} = error -> error
    end
  end

  defp finalize_imports(adapter, affected, inserted) do
    affected = Enum.sort_by(affected, fn {_doc_key, document_id} -> document_id end)

    Enum.reduce_while(
      affected,
      {:ok, %{documents_changed: 0, revisions_inserted: inserted, last_sequence: 0}},
      fn {doc_key, document_id}, {:ok, acc} ->
        with {:ok, leaves_now} <- Revisions.load_leaves(adapter.conn, doc_key),
             {:ok, winner} <- Winner.select(leaves_now),
             {:ok, leaf_json} <- Revisions.encode_leaf_set(leaves_now),
             :ok <- Documents.update(adapter.conn, doc_key, winner, 0),
             :ok <- refresh_indexes(adapter, doc_key, winner),
             {:ok, sequence} <- Changes.allocate_sequence(adapter.conn),
             :ok <- Documents.update(adapter.conn, doc_key, winner, sequence),
             :ok <-
               Changes.insert(
                 adapter.conn,
                 sequence,
                 doc_key,
                 document_id,
                 winner,
                 leaf_json,
                 "replication"
               ) do
          {:cont,
           {:ok,
            %{
              acc
              | documents_changed: acc.documents_changed + 1,
                last_sequence: max(acc.last_sequence, sequence)
            }}}
        else
          {:error, error} -> {:halt, {:error, error}}
        end
      end
    )
    |> case do
      {:ok, result} -> {:ok, result}
      error -> error
    end
  end

  defp refresh_indexes(adapter, doc_key, winner) do
    with {:ok, rows} <-
           Connection.query(
             adapter.conn,
             "SELECT index_id, definition_json, adapter_metadata_json FROM index_definitions WHERE lifecycle_state = 'ready' ORDER BY index_id"
           ) do
      Enum.reduce_while(rows, :ok, fn [index_id, definition_json, metadata_json], :ok ->
        with {:ok, definition} <- decode_json(definition_json),
             {:ok, metadata} <- decode_json(metadata_json),
             :ok <-
               FullTextIndexes.refresh_document(
                 adapter.conn,
                 Map.merge(Map.put(metadata, "index_id", index_id), definition),
                 doc_key,
                 winner.body,
                 winner.deleted
               ) do
          {:cont, :ok}
        else
          {:error, error} -> {:halt, {:error, error}}
        end
      end)
    end
  end

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

  defp current_winner(_adapter, nil), do: {:ok, nil}

  defp current_winner(adapter, doc),
    do: Revisions.find(adapter.conn, doc.doc_key, doc.winning_revision)

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

  defp chain_for_leaf(adapter, doc, leaf_id) do
    case Revisions.find(adapter.conn, doc.doc_key, leaf_id) do
      {:ok, leaf} ->
        case chain(adapter, doc.doc_key, leaf, []) do
          {:ok, revisions} ->
            {:ok,
             %{
               document_id: doc.document_id,
               leaf_revision: leaf_id,
               revisions: Enum.map(revisions, &revision_wire/1)
             }}

          {:error, error} ->
            {:error, error}
        end

      {:error, error} ->
        {:error, error}
    end
  end

  defp chain(_adapter, _doc_key, %Revision{parent_revision: nil} = revision, acc),
    do: {:ok, [revision | acc]}

  defp chain(adapter, doc_key, %Revision{parent_revision: parent} = revision, acc) do
    case Revisions.find(adapter.conn, doc_key, parent) do
      {:ok, parent_revision} ->
        chain(adapter, doc_key, parent_revision, [revision | acc])

      {:error, _} ->
        {:error,
         ElixirDB.Error.integrity_violation("revision chain contains a dangling parent", %{
           parent_revision: parent
         })}
    end
  end

  defp validate_operation_batch(operations) when is_list(operations) do
    max = ElixirDB.Config.host_limits()[:max_bulk_operations] || 500

    cond do
      operations == [] ->
        {:error, ElixirDB.Error.invalid_request("bulk operation list must not be empty")}

      length(operations) > max ->
        {:error, ElixirDB.Error.resource_limit("bulk operation count exceeds the host limit")}

      true ->
        if Enum.all?(operations, &is_map/1),
          do: :ok,
          else: {:error, ElixirDB.Error.invalid_request("bulk operations must be objects")}
    end
  end

  defp validate_operation_batch(_),
    do: {:error, ElixirDB.Error.invalid_request("bulk operations must be an array")}

  defp validate_documents_batch(documents) when is_list(documents) do
    max = ElixirDB.Config.host_limits()[:max_bulk_operations] || 500

    cond do
      length(documents) > max ->
        {:error, ElixirDB.Error.resource_limit("replication document count exceeds the host limit")}

      not Enum.all?(documents, &valid_replication_document_request?/1) ->
        {:error, ElixirDB.Error.invalid_request("replication document request is invalid")}

      true ->
        :ok
    end
  end

  defp validate_documents_batch(_),
    do: {:error, ElixirDB.Error.invalid_request("replication documents must be an array")}

  defp valid_replication_document_request?(entry) when is_map(entry) do
    id = entry[:document_id] || entry["document_id"]
    leaves = entry[:leaf_revisions] || entry["leaf_revisions"] || []
    allowed = [:document_id, :leaf_revisions, "document_id", "leaf_revisions"]

    Enum.all?(Map.keys(entry), &(&1 in allowed)) and is_binary(id) and is_list(leaves) and
      length(leaves) <= (ElixirDB.Config.host_limits()[:max_bulk_operations] || 500) and
      Enum.all?(leaves, &(is_binary(&1) and &1 != ""))
  end

  defp valid_replication_document_request?(_), do: false

  defp validate_chain_batch(chains) when is_list(chains) do
    max = ElixirDB.Config.host_limits()[:max_bulk_operations] || 500

    cond do
      length(chains) > max ->
        {:error, ElixirDB.Error.resource_limit("replication chain count exceeds the host limit")}

      not Enum.all?(chains, &is_map/1) ->
        {:error, ElixirDB.Error.invalid_request("replication chains must be objects")}

      not Enum.all?(chains, fn chain ->
        Enum.all?(
          Map.keys(chain),
          &(&1 in [
              :document_id,
              :leaf_revision,
              :revisions,
              "document_id",
              "leaf_revision",
              "revisions"
            ])
        )
      end) ->
        {:error, ElixirDB.Error.invalid_request("replication chains contain an unknown field")}

      true ->
        :ok
    end
  end

  defp validate_chain_batch(_),
    do: {:error, ElixirDB.Error.invalid_request("replication chains must be an array")}

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

  defp revision_wire(%Revision{} = revision),
    do: %{
      document_id: revision.document_id,
      revision_id: revision.revision_id,
      generation: revision.generation,
      parent_revision: revision.parent_revision,
      deleted: revision.deleted,
      body: revision.body
    }

  defp decode_query_documents(rows) do
    Enum.reduce_while(rows, {:ok, []}, fn [id, revision, body_json], {:ok, acc} ->
      case decode_json(body_json) do
        {:ok, body} -> {:cont, {:ok, [%{id: id, revision: revision, body: body} | acc]}}
        {:error, error} -> {:halt, {:error, error}}
      end
    end)
    |> case do
      {:ok, values} -> {:ok, Enum.reverse(values)}
      error -> error
    end
  end

  defp filter_query(documents, request) do
    selector = request[:selector] || request["selector"] || %{}

    result =
      Enum.reduce_while(documents, {:ok, []}, fn document, {:ok, acc} ->
        case ElixirDB.Query.Selector.matches?(document.body, selector) do
          {:ok, true} -> {:cont, {:ok, [document | acc]}}
          {:ok, false} -> {:cont, {:ok, acc}}
          {:error, error} -> {:halt, {:error, error}}
        end
      end)

    with {:ok, values} <- result do
      values = values |> Enum.reverse() |> sort_documents(request)

      {:ok, values}
    end
  end

  defp sort_documents(documents, request) do
    sort = request[:sort] || request["sort"] || []

    if sort == [] and not is_nil(request[:search] || request["search"]) do
      documents
    else
      Enum.sort(documents, fn left, right -> compare_documents(left, right, sort) end)
    end
  end

  defp apply_after_cursor(documents, request) do
    case request[:after_ordering] || request["after_ordering"] do
      after_ordering when is_map(after_ordering) ->
        sort = request[:sort] || request["sort"] || []

        Enum.drop_while(documents, fn document ->
          compare_ordering_keys(ordering_key(document, request), after_ordering, sort) != :gt
        end)

      _ ->
        case request[:after_id] || request["after_id"] do
          nil -> documents
          after_id -> Enum.drop_while(documents, &(&1.id <= after_id))
        end
    end
  end

  defp compare_ordering_keys(left, right, []), do: compare_ids(left["id"], right["id"])

  defp compare_ordering_keys(left, right, [sort | rest]) do
    left_value = ordering_value(Enum.at(left["sort"] || [], 0))
    right_value = ordering_value(Enum.at(right["sort"] || [], 0))

    case compare_values(left_value, right_value) do
      :eq ->
        compare_ordering_keys(
          %{"sort" => Enum.drop(left["sort"] || [], 1), "id" => left["id"]},
          %{"sort" => Enum.drop(right["sort"] || [], 1), "id" => right["id"]},
          rest
        )

      :lt ->
        if((sort[:direction] || sort["direction"] || "asc") == "asc", do: :lt, else: :gt)

      :gt ->
        if((sort[:direction] || sort["direction"] || "asc") == "asc", do: :gt, else: :lt)
    end
  end

  defp compare_ids(left, right) when left == right, do: :eq
  defp compare_ids(left, right) when left < right, do: :lt
  defp compare_ids(_left, _right), do: :gt

  defp ordering_value(%{"present" => true, "value" => value}), do: {:ok, value}
  defp ordering_value(_), do: :missing

  defp ordering_key(nil, _request), do: nil

  defp ordering_key(document, request) do
    sort = request[:sort] || request["sort"] || []

    values =
      Enum.map(sort, fn sort_field ->
        path = sort_field[:path] || sort_field["path"]

        case ElixirDB.JSON.Pointer.get(document.body, path) do
          {:ok, value} -> %{"present" => true, "value" => value}
          :missing -> %{"present" => false}
        end
      end)

    %{"sort" => values, "id" => document.id}
  end

  defp compare_documents(left, right, []), do: left.id <= right.id

  defp compare_documents(left, right, [sort | rest]) do
    path = sort[:path] || sort["path"]
    direction = sort[:direction] || sort["direction"] || "asc"
    left_value = ElixirDB.JSON.Pointer.get(left.body, path)
    right_value = ElixirDB.JSON.Pointer.get(right.body, path)

    case compare_values(left_value, right_value) do
      :eq -> compare_documents(left, right, rest)
      :lt -> direction == "asc"
      :gt -> direction == "desc"
    end
  end

  defp compare_values(:missing, :missing), do: :eq
  defp compare_values(:missing, _), do: :gt
  defp compare_values(_, :missing), do: :lt
  defp compare_values({:ok, left}, {:ok, right}) when left == right, do: :eq
  defp compare_values({:ok, left}, {:ok, right}) when left < right, do: :lt
  defp compare_values({:ok, _}, {:ok, _}), do: :gt
  defp compare_values(_, _), do: :eq

  defp create_physical_index(conn, index_id, definition) do
    case index_type(definition) do
      "full_text" -> FullTextIndexes.create_physical(conn, index_id, definition)
      _ -> StructuredIndexes.create_physical(conn, index_id, definition)
    end
  end

  defp drop_physical_index(conn, metadata) do
    case metadata["index_type"] || metadata[:index_type] || metadata["type"] || metadata[:type] do
      "full_text" -> FullTextIndexes.drop(conn, metadata)
      :full_text -> FullTextIndexes.drop(conn, metadata)
      _ -> StructuredIndexes.drop(conn, metadata)
    end
  end

  defp index_name(definition), do: definition["name"] || definition[:name]

  defp index_type(definition) do
    case definition["type"] || definition[:type] do
      :structured -> "structured"
      :full_text -> "full_text"
      value -> value
    end
  end

  defp index_result(definition, id, digest_value),
    do:
      definition
      |> stringify_definition()
      |> Map.merge(%{
        "index_id" => id,
        "definition_digest" => digest_value,
        "lifecycle_state" => "ready"
      })

  defp stringify_definition(value) when is_map(value),
    do: Map.new(value, fn {key, child} -> {to_string(key), stringify_definition(child)} end)

  defp stringify_definition(value) when is_list(value), do: Enum.map(value, &stringify_definition/1)
  defp stringify_definition(value), do: value

  defp find_index_by_name(conn, name, definition_json, digest_value) do
    case Connection.query(
           conn,
           "SELECT index_id, definition_digest, definition_json, lifecycle_state FROM index_definitions WHERE name = ?",
           [name]
         ) do
      {:ok, []} ->
        {:ok, nil}

      {:ok, [[id, existing_digest, existing_json, state]]} when existing_digest == digest_value ->
        with {:ok, definition} <- decode_json(existing_json) do
          {:ok,
           Map.merge(definition, %{
             "index_id" => id,
             "definition_digest" => digest_value,
             "lifecycle_state" => state
           })}
        end

      {:ok, [[_id, _existing_digest, _existing_json, _state]]} ->
        {:error,
         ElixirDB.Error.index_name_conflict("index name is already used by another definition", %{
           name: name,
           definition: definition_json
         })}

      {:error, reason} ->
        {:error, normalize_error(reason)}
    end
  end

  defp find_index(conn, index_id) do
    case Connection.query(
           conn,
           "SELECT index_id, name, index_type, definition_digest, definition_json, lifecycle_state, adapter_metadata_json FROM index_definitions WHERE index_id = ?",
           [index_id]
         ) do
      {:ok, [[id, name, type, digest_value, definition_json, state, metadata_json]]} ->
        {:ok,
         %{
           index_id: id,
           name: name,
           index_type: type,
           definition_digest: digest_value,
           definition_json: definition_json,
           lifecycle_state: state,
           adapter_metadata_json: metadata_json
         }}

      {:ok, []} ->
        {:error, ElixirDB.Error.index_not_found("index not found", %{index: index_id})}

      {:error, reason} ->
        {:error, normalize_error(reason)}
    end
  end

  defp decode_index_metadata(row) do
    with {:ok, metadata} <- decode_json(row.adapter_metadata_json) do
      {:ok, Map.put(metadata, "index_id", row.index_id)}
    end
  end

  defp adapter_identity(adapter) do
    case identity(adapter) do
      {:ok, value} -> value
      _ -> %{current_sequence: 0, config: ElixirDB.Config.defaults()}
    end
  end

  defp json_type_sql(path, "string"),
    do:
      "json_type(winning_body_json, #{QueryCompiler.quote_literal(QueryCompiler.sqlite_path(path))}) = 'text'"

  defp json_type_sql(path, type) do
    quoted = QueryCompiler.quote_literal(QueryCompiler.sqlite_path(path))

    case type do
      "number" ->
        "json_type(winning_body_json, #{quoted}) IN ('integer', 'real')"

      "boolean" ->
        "json_type(winning_body_json, #{quoted}) IN ('true', 'false')"

      "null" ->
        "json_type(winning_body_json, #{quoted}) = 'null'"

      _ ->
        "json_type(winning_body_json, #{quoted}) = 'text'"
    end
  end

  defp type_matches?(value, "string"), do: is_binary(value)
  defp type_matches?(value, "number"), do: is_number(value) and not is_boolean(value)
  defp type_matches?(value, "boolean"), do: is_boolean(value)
  defp type_matches?(nil, "null"), do: true
  defp type_matches?(_, _), do: false

  defp operator_sql("$eq"), do: "="
  defp operator_sql("$gt"), do: ">"
  defp operator_sql("$gte"), do: ">="
  defp operator_sql("$lt"), do: "<"
  defp operator_sql("$lte"), do: "<="

  defp digest(id), do: id |> String.split("-", parts: 2) |> List.last()
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
