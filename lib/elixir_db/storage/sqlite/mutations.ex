defmodule ElixirDB.Storage.SQLite.Mutations do
  @moduledoc """
  Local mutation and conflict-resolution write workflows for the SQLite adapter.

  Owns apply-local, bulk, and resolve orchestration inside an open IMMEDIATE
  transaction provided by the adapter. Index refresh goes through `IndexCatalog`.
  """

  alias ElixirDB.Attachments.Manifest
  alias ElixirDB.Domain.Revision
  alias ElixirDB.JSON.Canonical
  alias ElixirDB.MapAccess
  alias ElixirDB.Revisions.ConflictResolution
  alias ElixirDB.Revisions.{Id, Winner}
  alias ElixirDB.Storage.SQLite.Adapter

  alias ElixirDB.Storage.SQLite.{
    Attachments,
    Changes,
    Documents,
    IndexCatalog,
    RetentionRecords,
    Revisions
  }

  alias ElixirDB.UUID

  @doc """
  Applies one local put/delete mutation inside an open transaction.
  """
  @spec apply_local_tx(map(), map()) :: {:ok, map()} | {:error, ElixirDB.Error.t()}
  def apply_local_tx(adapter, request) do
    operation = MapAccess.get(request, :operation, :put)
    document_id = MapAccess.get(request, :document_id)
    if_revision = MapAccess.get(request, :if_revision)
    history_id = MapAccess.get(request, :history_id)
    deleted = operation in [:delete, "delete"]
    body = if deleted, do: nil, else: MapAccess.get(request, :body)

    with :ok <- validate_mutation_operation(operation),
         :ok <- validate_document_input(adapter, document_id, deleted, body),
         {:ok, doc} <- Documents.find(adapter.conn, document_id),
         {:ok, current} <- current_winner(adapter, doc),
         {:ok, candidate} <-
           candidate_revision(adapter, doc, %{
             document_id: document_id,
             if_revision: if_revision,
             current: current,
             deleted: deleted,
             body: body,
             history_id: history_id,
             request: request
           }) do
      # TX-006: insert_local_revision owns the single unified replay/winner check. An
      # identical existing revision replays only when it is still the winner; a
      # superseded retry surfaces revision_conflict with operation_already_committed: true.
      insert_local_revision(adapter, doc, candidate, if_revision, operation)
    end
  end

  @doc """
  Applies a bulk mutation/resolve batch inside an open transaction.
  """
  @spec bulk_tx(map(), [map()]) :: {:ok, [map()]} | {:error, ElixirDB.Error.t()}
  def bulk_tx(adapter, operations) when is_list(operations) do
    with {:ok, effects, affected} <- prepare_bulk_operations(adapter, operations),
         {:ok, finalized} <- finalize_bulk_documents(adapter, affected) do
      {:ok,
       Enum.map(effects, fn effect ->
         result = Map.get(finalized, effect.doc_key, effect.result)
         Map.put(result, :replayed, effect.replayed)
       end)}
    end
  end

  @doc """
  Resolves a document conflict inside an open transaction.
  """
  @spec resolve_conflict_tx(map(), map()) :: {:ok, map()} | {:error, ElixirDB.Error.t()}
  def resolve_conflict_tx(adapter, request) do
    document_id = MapAccess.get(request, :document_id)
    expected = MapAccess.get(request, :expected_live_revisions, [])
    body = MapAccess.get(request, :body)
    delete_all = MapAccess.get(request, :delete_all, false)

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
         :ok <- ConflictResolution.validate_leaf_set(current_leaves, expected),
         {:ok, revisions} <-
           build_resolution_revisions(
             adapter,
             document_id,
             current_leaves,
             request,
             body,
             delete_all
           ),
         {:ok, status} <- resolution_status(adapter, doc.doc_key, revisions) do
      resolve_conflict_status(adapter, doc, document_id, revisions, status)
    else
      {:ok, nil} ->
        {:error, ElixirDB.Error.document_not_found("document does not exist")}

      {:error, error} ->
        {:error, error}
    end
  end

  defp resolve_conflict_status(adapter, doc, _document_id, _revisions, :replayed) do
    with {:ok, winner} <- current_winner(adapter, doc),
         {:ok, result} <- document_mutation_result(adapter, doc, winner) do
      {:ok, Map.put(result, :replayed, true)}
    end
  end

  defp resolve_conflict_status(adapter, doc, document_id, revisions, :new) do
    with :ok <- insert_resolution_revisions(adapter, doc.doc_key, revisions),
         :ok <- remove_pending_for_revisions(adapter, revisions),
         {:ok, result} <-
           finalize_document(adapter, doc.doc_key, document_id, List.first(revisions)) do
      {:ok, Map.put(result, :replayed, false)}
    end
  end

  @doc """
  Validates a bulk operation list against host limits.
  """
  @spec validate_operation_batch(term()) :: :ok | {:error, ElixirDB.Error.t()}
  def validate_operation_batch(operations) when is_list(operations) do
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

  def validate_operation_batch(_),
    do: {:error, ElixirDB.Error.invalid_request("bulk operations must be an array")}

  defp validate_document_input(adapter, document_id, deleted, body) do
    max_id =
      get_in(adapter_identity(adapter), [:config, "documents", "max_document_id_bytes"]) || 512

    max_body =
      get_in(adapter_identity(adapter), [:config, "documents", "max_document_bytes"]) || 1_048_576

    validators = [
      fn -> validate_document_id(document_id) end,
      fn -> validate_document_id_size(document_id, max_id) end,
      fn -> validate_document_id_characters(document_id) end,
      fn -> validate_deleted_body(deleted, body) end,
      fn -> validate_live_body(deleted, body) end,
      fn -> validate_body_size(deleted, body, max_body) end
    ]

    case Enum.find_value(validators, & &1.()) do
      nil -> :ok
      error -> {:error, error}
    end
  end

  defp validate_document_id(value) when is_binary(value) and value != "" do
    if String.valid?(value), do: nil, else: invalid_document_id()
  end

  defp validate_document_id(_), do: invalid_document_id()

  defp invalid_document_id,
    do: ElixirDB.Error.invalid_request("document id must be a non-empty UTF-8 string")

  defp validate_document_id_size(value, max) when is_binary(value) and byte_size(value) <= max,
    do: nil

  defp validate_document_id_size(_value, _max),
    do: ElixirDB.Error.resource_limit("document id exceeds the configured limit")

  defp validate_document_id_characters(value) do
    if String.contains?(value, <<0>>) or String.starts_with?(value, "_system/") or
         Enum.any?(String.to_charlist(value), &(&1 < 0x20)),
       do: ElixirDB.Error.invalid_request("document id contains a reserved character"),
       else: nil
  end

  defp validate_deleted_body(true, nil), do: nil

  defp validate_deleted_body(true, _body),
    do: ElixirDB.Error.invalid_request("deleted revisions must not contain a body")

  defp validate_deleted_body(false, _body), do: nil

  defp validate_live_body(false, body) when is_map(body), do: nil
  defp validate_live_body(true, _body), do: nil

  defp validate_live_body(false, _body),
    do: ElixirDB.Error.invalid_request("document body must be an object")

  defp validate_body_size(true, _body, _max), do: nil
  defp validate_body_size(false, body, max) when is_map(body), do: body_size_error(body, max)
  defp validate_body_size(false, _body, _max), do: nil

  defp body_size_error(body, max) do
    case Canonical.encode(body) do
      {:ok, json} when byte_size(json) <= max ->
        nil

      {:ok, _json} ->
        ElixirDB.Error.resource_limit("document body exceeds the configured limit")

      {:error, _error} ->
        ElixirDB.Error.invalid_request("document body must contain canonical JSON values")
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

  defp candidate_revision(adapter, doc, attrs) do
    %{
      document_id: document_id,
      if_revision: if_revision,
      current: current,
      deleted: deleted,
      body: body,
      history_id: history_id,
      request: request
    } = attrs

    if is_nil(current) and not is_nil(if_revision) do
      {:error, ElixirDB.Error.revision_conflict("document does not have the expected parent")}
    else
      with {:ok, {parent, resolved_history_id}} <-
             revision_parent_and_history(
               adapter,
               doc,
               document_id,
               current,
               if_revision,
               deleted,
               history_id
             ),
           {:ok, attachments} <-
             resolve_mutation_attachments(
               adapter,
               doc,
               request,
               deleted,
               parent,
               current,
               :mutation
             ),
           {:ok, revision} <-
             build_revision(
               document_id,
               resolved_history_id,
               parent,
               deleted,
               body,
               attachments
             ) do
        candidate_from_revision(adapter, doc, if_revision, current, revision)
      end
    end
  end

  defp candidate_from_revision(_adapter, _doc, _if_revision, nil, revision), do: {:ok, revision}

  defp candidate_from_revision(_adapter, _doc, if_revision, current, revision)
       when if_revision == current.revision_id,
       do: {:ok, revision}

  defp candidate_from_revision(adapter, doc, if_revision, current, revision) do
    case Revisions.find(adapter.conn, doc.doc_key, revision.revision_id) do
      {:ok, existing} ->
        replay_candidate(existing, revision, if_revision, current)

      {:error, %ElixirDB.Error{code: :revision_not_found}} ->
        stale_revision_error(if_revision, current, revision)

      {:error, error} ->
        {:error, error}
    end
  end

  defp replay_candidate(existing, revision, if_revision, current) do
    if Revisions.same?(existing, revision),
      do: {:ok, revision},
      else: stale_revision_error(if_revision, current, revision)
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

  defp insert_local_revision(adapter, nil, %Revision{} = candidate, _if_revision, _operation) do
    with {:ok, doc_key} <- Documents.insert(adapter.conn, candidate.document_id),
         :ok <- Revisions.insert(adapter.conn, doc_key, candidate),
         :ok <- Attachments.remove_pending_for_manifest(adapter.conn, candidate.attachments),
         {:ok, result} <- finalize_document(adapter, doc_key, candidate.document_id, candidate) do
      {:ok, Map.put(result, :replayed, false)}
    end
  end

  defp insert_local_revision(adapter, doc, %Revision{} = candidate, _if_revision, _operation) do
    case Revisions.find(adapter.conn, doc.doc_key, candidate.revision_id) do
      {:ok, existing} ->
        existing_local_revision_result(existing, doc, candidate)

      {:error, %ElixirDB.Error{code: :revision_not_found}} ->
        with :ok <- Revisions.ensure_parent(adapter.conn, doc.doc_key, candidate.parent_revision),
             :ok <- Revisions.insert(adapter.conn, doc.doc_key, candidate),
             :ok <- Attachments.remove_pending_for_manifest(adapter.conn, candidate.attachments),
             {:ok, result} <-
               finalize_document(adapter, doc.doc_key, candidate.document_id, candidate) do
          {:ok, Map.put(result, :replayed, false)}
        end

      {:error, error} ->
        {:error, error}
    end
  end

  defp existing_local_revision_result(existing, doc, candidate) do
    if Revisions.same?(existing, candidate),
      do: local_replay_result(doc, candidate),
      else: {:error, ElixirDB.Error.integrity_violation("revision id has different content")}
  end

  defp local_replay_result(doc, candidate) when doc.winning_revision == candidate.revision_id,
    do: {:ok, %{revision: candidate.revision_id, sequence: doc.update_sequence, replayed: true}}

  defp local_replay_result(_doc, candidate),
    do:
      {:error,
       ElixirDB.Error.revision_conflict("operation was already committed", %{
         operation_already_committed: true,
         revision: candidate.revision_id
       })}

  defp finalize_document(adapter, doc_key, document_id, _candidate) do
    with {:ok, all_leaves} <- Revisions.load_leaves(adapter.conn, doc_key),
         {:ok, winner} <- Winner.select(all_leaves),
         {:ok, leaf_json} <- Revisions.encode_leaf_set(all_leaves),
         :ok <- Documents.update(adapter.conn, doc_key, winner, 0),
         :ok <- IndexCatalog.refresh_ready(adapter.conn, doc_key, winner),
         {:ok, sequence} <- Changes.allocate_sequence(adapter.conn),
         :ok <- Documents.update(adapter.conn, doc_key, winner, sequence),
         :ok <-
           Changes.insert(adapter.conn, sequence, doc_key, document_id, winner, leaf_json, "local"),
         :ok <- RetentionRecords.mark_pending_local_causal(adapter.conn) do
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

  defp prepare_bulk_operations(adapter, operations) do
    Enum.reduce_while(operations, {:ok, [], %{}}, fn operation, {:ok, effects, affected} ->
      prepare_bulk_effect(adapter, operation, effects, affected)
    end)
    |> case do
      {:ok, effects, affected} -> {:ok, Enum.reverse(effects), affected}
      error -> error
    end
  end

  defp prepare_bulk_effect(adapter, operation, effects, affected) do
    case prepare_bulk_operation(adapter, operation) do
      {:ok, effect} ->
        {:cont, {:ok, [effect | effects], update_affected(affected, effect)}}

      {:error, error} ->
        {:halt, {:error, error}}
    end
  end

  defp update_affected(affected, %{changed: true, doc_key: doc_key, document_id: document_id}),
    do: Map.put(affected, doc_key, document_id)

  defp update_affected(affected, _effect), do: affected

  defp prepare_bulk_operation(adapter, request) do
    operation = MapAccess.get(request, :operation, :put)

    case operation do
      operation when operation in [:resolve, "resolve"] ->
        prepare_bulk_resolution_operation(adapter, request)

      operation when operation in [:put, "put", :delete, "delete"] ->
        prepare_bulk_mutation_operation(adapter, request)

      _ ->
        {:error, ElixirDB.Error.invalid_request("bulk operation type is invalid")}
    end
  end

  defp prepare_bulk_mutation_operation(adapter, request) do
    operation = MapAccess.get(request, :operation, :put)
    document_id = MapAccess.get(request, :document_id)
    if_revision = MapAccess.get(request, :if_revision)
    history_id = MapAccess.get(request, :history_id)
    deleted = operation in [:delete, "delete"]
    body = if(deleted, do: nil, else: MapAccess.get(request, :body))

    with :ok <- validate_mutation_operation(operation),
         :ok <- validate_document_input(adapter, document_id, deleted, body),
         {:ok, doc} <- Documents.find(adapter.conn, document_id),
         {:ok, current} <- current_winner(adapter, doc),
         {:ok, candidate_state} <-
           candidate_revision(adapter, doc, %{
             document_id: document_id,
             if_revision: if_revision,
             current: current,
             deleted: deleted,
             body: body,
             history_id: history_id,
             request: request
           }) do
      prepare_bulk_document(adapter, doc, candidate_state, document_id)
    end
  end

  defp prepare_bulk_document(adapter, nil, candidate, document_id) do
    with {:ok, doc_key} <- Documents.insert(adapter.conn, document_id),
         :ok <- Revisions.insert(adapter.conn, doc_key, candidate),
         :ok <- Attachments.remove_pending_for_manifest(adapter.conn, candidate.attachments),
         :ok <- update_pending_document(adapter, doc_key, candidate) do
      {:ok,
       bulk_effect(doc_key, document_id, true, false, %{
         revision: candidate.revision_id,
         sequence: 0
       })}
    end
  end

  defp prepare_bulk_document(adapter, doc, candidate, document_id) do
    case Revisions.find(adapter.conn, doc.doc_key, candidate.revision_id) do
      {:ok, existing} ->
        existing_bulk_revision(existing, doc, candidate, document_id)

      {:error, %ElixirDB.Error{code: :revision_not_found}} ->
        with :ok <- Revisions.ensure_parent(adapter.conn, doc.doc_key, candidate.parent_revision),
             :ok <- Revisions.insert(adapter.conn, doc.doc_key, candidate),
             :ok <- Attachments.remove_pending_for_manifest(adapter.conn, candidate.attachments),
             :ok <- update_pending_document(adapter, doc.doc_key, candidate) do
          {:ok,
           bulk_effect(
             doc.doc_key,
             document_id,
             true,
             false,
             %{revision: candidate.revision_id, sequence: 0}
           )}
        end

      {:error, error} ->
        {:error, error}
    end
  end

  defp existing_bulk_revision(existing, doc, candidate, document_id) do
    if Revisions.same?(existing, candidate),
      do: bulk_replay_revision(doc, candidate, document_id),
      else: {:error, ElixirDB.Error.integrity_violation("revision id has different content")}
  end

  defp bulk_replay_revision(doc, candidate, document_id)
       when doc.winning_revision == candidate.revision_id do
    {:ok,
     bulk_effect(
       doc.doc_key,
       document_id,
       false,
       true,
       %{revision: candidate.revision_id, sequence: doc.update_sequence}
     )}
  end

  defp bulk_replay_revision(_doc, candidate, _document_id),
    do:
      {:error,
       ElixirDB.Error.revision_conflict("operation was already committed", %{
         operation_already_committed: true,
         revision: candidate.revision_id
       })}

  defp prepare_bulk_resolution_operation(adapter, request) do
    document_id = MapAccess.get(request, :document_id)
    expected = MapAccess.get(request, :expected_live_revisions, [])
    body = MapAccess.get(request, :body)
    delete_all = MapAccess.get(request, :delete_all, false)

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
         :ok <- ConflictResolution.validate_leaf_set(leaves, expected),
         {:ok, revisions} <-
           build_resolution_revisions(adapter, document_id, leaves, request, body, delete_all),
         {:ok, status} <- resolution_status(adapter, doc.doc_key, revisions) do
      prepare_resolution_status(adapter, doc, document_id, revisions, status)
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

  defp prepare_resolution_status(adapter, doc, document_id, _revisions, :replayed) do
    {:ok, winner} = current_winner(adapter, doc)

    {:ok,
     bulk_effect(
       doc.doc_key,
       document_id,
       false,
       true,
       %{revision: winner.revision_id, sequence: doc.update_sequence}
     )}
  end

  defp prepare_resolution_status(adapter, doc, document_id, revisions, :new) do
    with :ok <- insert_resolution_revisions(adapter, doc.doc_key, revisions),
         :ok <- remove_pending_for_revisions(adapter, revisions),
         :ok <- update_pending_document(adapter, doc.doc_key, List.first(revisions)) do
      {:ok,
       bulk_effect(
         doc.doc_key,
         document_id,
         true,
         false,
         %{revision: List.first(revisions).revision_id, sequence: 0}
       )}
    end
  end

  defp bulk_effect(doc_key, document_id, changed, replayed, result),
    do: %{
      doc_key: doc_key,
      document_id: document_id,
      changed: changed,
      replayed: replayed,
      result: result
    }

  defp update_pending_document(adapter, doc_key, _candidate) do
    with {:ok, leaves_now} <- Revisions.load_leaves(adapter.conn, doc_key),
         {:ok, winner} <- Winner.select(leaves_now),
         :ok <- Documents.update(adapter.conn, doc_key, winner, 0) do
      IndexCatalog.refresh_ready(adapter.conn, doc_key, winner)
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

  defp resolution_status(adapter, doc_key, revisions) do
    statuses = Enum.map(revisions, &resolution_revision_status(adapter, doc_key, &1))

    classify_resolution_statuses(statuses)
  end

  defp resolution_revision_status(adapter, doc_key, revision) do
    case Revisions.find(adapter.conn, doc_key, revision.revision_id) do
      {:ok, existing} -> if(Revisions.same?(existing, revision), do: :existing, else: :different)
      {:error, %ElixirDB.Error{code: :revision_not_found}} -> :missing
      {:error, _} -> :different
    end
  end

  defp classify_resolution_statuses(statuses) do
    {has_different, has_existing, has_missing} =
      Enum.reduce(statuses, {false, false, false}, fn
        :different, {_different, existing, missing} -> {true, existing, missing}
        :existing, {different, _existing, missing} -> {different, true, missing}
        :missing, {different, existing, _missing} -> {different, existing, true}
      end)

    cond do
      has_different ->
        {:error, ElixirDB.Error.integrity_violation("resolution revision id has different content")}

      has_existing and not has_missing ->
        {:ok, :replayed}

      has_existing ->
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

  defp build_resolution_revisions(adapter, document_id, leaves, request, body, delete_all) do
    chosen = MapAccess.get(request, :chosen_parent_revision)
    live = Enum.reject(leaves, & &1.deleted)

    cond do
      live == [] ->
        {:error, ElixirDB.Error.revision_conflict("document has no current live leaves")}

      delete_all ->
        Enum.map(live, fn leaf ->
          build_revision(document_id, leaf.history_id, leaf.revision_id, true, nil, %{})
        end)
        |> collect_ok()

      true ->
        with true <- chosen in Enum.map(live, & &1.revision_id),
             chosen_leaf = Enum.find(live, &(&1.revision_id == chosen)),
             {:ok, attachments} <-
               resolve_conflict_attachments(adapter, request, chosen_leaf),
             {:ok, survivor} <-
               build_revision(
                 document_id,
                 chosen_leaf.history_id,
                 chosen,
                 false,
                 body,
                 attachments
               ),
             {:ok, tombstones} <-
               live
               |> Enum.reject(&(&1.revision_id == chosen))
               |> Enum.map(
                 &build_revision(document_id, &1.history_id, &1.revision_id, true, nil, %{})
               )
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

  defp resolve_conflict_attachments(adapter, request, chosen_leaf) do
    case attachment_intent(request) do
      :omitted ->
        Manifest.resolve_inheritance(:resolve_conflict, :omitted, chosen_leaf.attachments)

      :inherit ->
        Manifest.resolve_inheritance(:resolve_conflict, :omitted, chosen_leaf.attachments)

      explicit when is_map(explicit) ->
        with {:ok, coerced} <- coerce_attachment_entries(explicit),
             {:ok, normalized} <- Manifest.normalize(coerced),
             :ok <- Attachments.ensure_reachable(adapter.conn, normalized) do
          {:ok, normalized}
        end

      _ ->
        {:error, ElixirDB.Error.invalid_request("attachments must be an object")}
    end
  end

  defp resolve_mutation_attachments(
         adapter,
         doc,
         request,
         deleted,
         parent_revision_id,
         current,
         _kind
       ) do
    if deleted do
      {:ok, %{}}
    else
      intent = attachment_intent(request)
      operation = attachment_operation(parent_revision_id, current)

      with {:ok, parent_manifest} <-
             parent_manifest_for(adapter, doc, parent_revision_id, intent, operation),
           {:ok, manifest} <- resolve_attachment_intent(operation, intent, parent_manifest),
           :ok <- Attachments.ensure_reachable(adapter.conn, manifest) do
        {:ok, manifest}
      end
    end
  end

  defp attachment_intent(request) when is_map(request) do
    cond do
      Map.has_key?(request, :attachments) ->
        normalize_intent_value(Map.get(request, :attachments))

      Map.has_key?(request, "attachments") ->
        normalize_intent_value(Map.get(request, "attachments"))

      true ->
        :omitted
    end
  end

  defp normalize_intent_value(:inherit), do: :inherit
  defp normalize_intent_value("inherit"), do: :inherit
  defp normalize_intent_value(value), do: value

  defp attachment_operation(nil, nil), do: :create
  defp attachment_operation(nil, _current), do: :create
  defp attachment_operation(_parent, _current), do: :update

  defp parent_manifest_for(_adapter, _doc, _parent, intent, :create)
       when intent in [:omitted, :inherit],
       do: {:ok, nil}

  defp parent_manifest_for(_adapter, _doc, _parent, intent, _operation)
       when is_map(intent),
       do: {:ok, nil}

  defp parent_manifest_for(adapter, doc, parent_revision_id, intent, :update)
       when intent in [:omitted, :inherit] and is_binary(parent_revision_id) do
    case Revisions.find(adapter.conn, doc.doc_key, parent_revision_id) do
      {:ok, parent} -> {:ok, parent.attachments}
      {:error, error} -> {:error, error}
    end
  end

  defp parent_manifest_for(_adapter, _doc, _parent, intent, :update)
       when intent in [:omitted, :inherit],
       do: {:ok, nil}

  defp resolve_attachment_intent(operation, intent, parent)
       when intent in [:omitted, :inherit] do
    Manifest.resolve_inheritance(operation, :omitted, parent)
  end

  defp resolve_attachment_intent(_operation, explicit, _parent) do
    if is_map(explicit) do
      with {:ok, coerced} <- coerce_attachment_entries(explicit) do
        Manifest.normalize(coerced)
      end
    else
      {:error, ElixirDB.Error.invalid_request("attachments must be an object")}
    end
  end

  defp coerce_attachment_entries(manifest) when is_map(manifest) do
    Enum.reduce_while(manifest, {:ok, %{}}, fn {name, entry}, {:ok, acc} ->
      case coerce_attachment_entry(entry) do
        {:ok, coerced} -> {:cont, {:ok, Map.put(acc, name, coerced)}}
        {:error, error} -> {:halt, {:error, error}}
      end
    end)
  end

  defp coerce_attachment_entry(entry) when is_map(entry) do
    digest = MapAccess.get(entry, :digest) || MapAccess.get(entry, :blob)
    length = MapAccess.get_first(entry, [:length, :logical_size])
    content_type = MapAccess.get(entry, :content_type)

    {:ok,
     %{
       "digest" => digest,
       "length" => length,
       "content_type" => content_type
     }}
  end

  defp coerce_attachment_entry(_),
    do: {:error, ElixirDB.Error.invalid_request("attachment entry must be an object")}

  defp remove_pending_for_revisions(adapter, revisions) do
    Enum.reduce_while(revisions, :ok, fn revision, :ok ->
      case Attachments.remove_pending_for_manifest(adapter.conn, revision.attachments) do
        :ok -> {:cont, :ok}
        {:error, error} -> {:halt, {:error, error}}
      end
    end)
  end

  defp revision_parent_and_history(_adapter, nil, _document_id, nil, nil, _deleted, history_id) do
    {:ok, {nil, root_history_id(history_id)}}
  end

  defp revision_parent_and_history(
         _adapter,
         _doc,
         _document_id,
         nil,
         nil,
         _deleted,
         history_id
       ) do
    {:ok, {nil, root_history_id(history_id)}}
  end

  defp revision_parent_and_history(_adapter, _doc, _document_id, current, nil, false, _history_id)
       when not current.deleted do
    {:ok, {nil, current.history_id}}
  end

  defp revision_parent_and_history(
         _adapter,
         _doc,
         _document_id,
         current,
         _if_revision,
         false,
         history_id
       )
       when not is_nil(current) and current.deleted do
    {:ok, {nil, root_history_id(history_id)}}
  end

  defp revision_parent_and_history(
         adapter,
         doc,
         _document_id,
         _current,
         if_revision,
         _deleted,
         _history_id
       )
       when is_binary(if_revision) do
    case Revisions.find(adapter.conn, doc.doc_key, if_revision) do
      {:ok, parent} ->
        {:ok, {if_revision, parent.history_id}}

      {:error, %ElixirDB.Error{code: :revision_not_found}} ->
        {:error, ElixirDB.Error.revision_conflict("document does not have the expected parent")}

      {:error, error} ->
        {:error, error}
    end
  end

  defp revision_parent_and_history(
         _adapter,
         _doc,
         _document_id,
         _current,
         _if_revision,
         _deleted,
         _history_id
       ),
       do: {:error, ElixirDB.Error.revision_conflict("document does not have the expected parent")}

  defp root_history_id(history_id) when is_binary(history_id) and history_id != "", do: history_id
  defp root_history_id(_), do: fresh_history_id()

  # Domain.Revision construction for calculated ids (shared shape for ExDNA).
  defp revision_struct(
         document_id,
         history_id,
         revision_id,
         generation,
         parent,
         deleted,
         body,
         attachments
       ) do
    Revision.assemble(
      document_id: document_id,
      history_id: history_id,
      revision_id: revision_id,
      generation: generation,
      parent_revision: parent,
      digest: digest(revision_id),
      deleted: deleted,
      body: body,
      attachments: attachments
    )
  end

  defp build_revision(document_id, history_id, parent, deleted, body, attachments) do
    with {:ok, id} <- Id.calculate(document_id, history_id, parent, deleted, body, attachments),
         {:ok, generation} <- Id.generation(id),
         {:ok, normalized_attachments} <- normalize_attachments(attachments, deleted) do
      {:ok,
       revision_struct(
         document_id,
         history_id,
         id,
         generation,
         parent,
         deleted,
         body,
         normalized_attachments
       )}
    end
  end

  defp normalize_attachments(_attachments, true), do: {:ok, %{}}

  defp normalize_attachments(attachments, false) do
    Manifest.normalize(attachments)
  end

  defp fresh_history_id, do: UUID.v4()

  defp collect_ok(values) do
    Enum.reduce_while(values, {:ok, []}, fn
      {:ok, value}, {:ok, acc} -> {:cont, {:ok, [value | acc]}}
      {:error, error}, _ -> {:halt, {:error, error}}
    end)
  end

  defp current_winner(_adapter, nil), do: {:ok, nil}

  defp current_winner(_adapter, %{winning_revision: nil}), do: {:ok, nil}

  defp current_winner(adapter, doc),
    do: Revisions.find(adapter.conn, doc.doc_key, doc.winning_revision)

  defp adapter_identity(adapter) do
    case Adapter.identity(adapter) do
      {:ok, value} -> value
      _ -> %{current_sequence: 0, config: ElixirDB.Config.defaults()}
    end
  end

  defp validate_mutation_operation(operation) when operation in [:put, "put", :delete, "delete"],
    do: :ok

  defp validate_mutation_operation(_),
    do: {:error, ElixirDB.Error.invalid_request("mutation operation type is invalid")}

  defp digest(id), do: id |> String.split("-", parts: 2) |> List.last()
end
