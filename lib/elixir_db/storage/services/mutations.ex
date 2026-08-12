defmodule ElixirDB.Storage.Services.Mutations do
  @moduledoc """
  Shared local mutation and conflict-resolution write workflows.

  Orchestrates apply-local, bulk, and resolve flows against storage ports via
  `ElixirDB.Storage.Services.Facts`. Backends supply fact loading and effect
  application only.
  """

  alias ElixirDB.Attachments.Manifest
  alias ElixirDB.Domain.Revision
  alias ElixirDB.JSON.Canonical
  alias ElixirDB.MapAccess
  alias ElixirDB.Revisions.ConflictResolution
  alias ElixirDB.Revisions.{Id, Winner}
  alias ElixirDB.Storage.BackendContext
  alias ElixirDB.Storage.Services.Facts
  alias ElixirDB.UUID

  @doc """
  Applies one local put/delete mutation inside an open transaction.
  """
  @spec apply_local_tx(BackendContext.t(), map()) :: {:ok, map()} | {:error, ElixirDB.Error.t()}
  def apply_local_tx(%BackendContext{} = context, request) do
    operation = MapAccess.get(request, :operation, :put)
    document_id = MapAccess.get(request, :document_id)
    if_revision = MapAccess.get(request, :if_revision)
    history_id = MapAccess.get(request, :history_id)
    deleted = operation in [:delete, "delete"]
    body = if deleted, do: nil, else: MapAccess.get(request, :body)

    with :ok <- validate_mutation_operation(operation),
         :ok <- validate_document_input(context, document_id, deleted, body),
         {:ok, doc} <- Facts.find_document(context, document_id),
         {:ok, current} <- current_winner(context, doc),
         {:ok, candidate} <-
           candidate_revision(context, doc, %{
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
      insert_local_revision(context, doc, candidate, if_revision, operation)
    end
  end

  @doc """
  Applies a bulk mutation/resolve batch inside an open transaction.
  """
  @spec bulk_tx(BackendContext.t(), [map()]) :: {:ok, [map()]} | {:error, ElixirDB.Error.t()}
  def bulk_tx(%BackendContext{} = context, operations) when is_list(operations) do
    materialize_pending? = not independent_bulk_operations?(operations)
    config = Map.get(adapter_identity(context), :config, ElixirDB.Config.defaults())

    with {:ok, ready_indexes} <- Facts.ready_definitions(context),
         {:ok, document_cache} <-
           preload_bulk_documents(context, operations, materialize_pending?),
         {:ok, effects, affected} <-
           prepare_bulk_operations(
             context,
             operations,
             ready_indexes,
             materialize_pending?,
             config,
             document_cache
           ),
         {:ok, finalized} <-
           finalize_bulk_documents(context, affected, ready_indexes),
         :ok <- mark_bulk_pending(context, affected) do
      {:ok,
       Enum.map(effects, fn effect ->
         result = Map.get(finalized, effect.document_id, effect.result)
         Map.put(result, :replayed, effect.replayed)
       end)}
    end
  end

  defp independent_bulk_operations?(operations) do
    document_ids = Enum.map(operations, &MapAccess.get(&1, :document_id))
    length(document_ids) == length(Enum.uniq(document_ids))
  end

  defp preload_bulk_documents(_context, _operations, true), do: {:ok, %{}}

  defp preload_bulk_documents(context, operations, false) do
    document_ids = Enum.map(operations, &MapAccess.get(&1, :document_id))

    if Enum.all?(document_ids, &is_binary/1) do
      Facts.find_documents(context, Enum.uniq(document_ids))
    else
      {:ok, %{}}
    end
  end

  defp bulk_document(context, document_cache, document_id) do
    case Map.fetch(document_cache, document_id) do
      {:ok, document} -> {:ok, document}
      :error -> Facts.find_document(context, document_id)
    end
  end

  defp mark_bulk_pending(_context, affected) when map_size(affected) == 0, do: :ok

  defp mark_bulk_pending(context, _affected),
    do: Facts.mark_pending_local_causal(context)

  @doc """
  Resolves a document conflict inside an open transaction.
  """
  @spec resolve_conflict_tx(BackendContext.t(), map()) ::
          {:ok, map()} | {:error, ElixirDB.Error.t()}
  def resolve_conflict_tx(%BackendContext{} = context, request) do
    document_id = MapAccess.get(request, :document_id)
    expected = MapAccess.get(request, :expected_live_revisions, [])
    body = MapAccess.get(request, :body)
    delete_all = MapAccess.get(request, :delete_all, false)

    with true <- is_boolean(delete_all),
         :ok <-
           validate_document_input(
             context,
             document_id,
             delete_all,
             if(delete_all, do: nil, else: body)
           ),
         {:ok, %{document_id: _} = doc} <- Facts.find_document(context, document_id),
         {:ok, current_leaves} <- Facts.list_leaves(context, doc.document_id),
         :ok <- ConflictResolution.validate_leaf_set(current_leaves, expected),
         {:ok, revisions} <-
           build_resolution_revisions(
             context,
             document_id,
             current_leaves,
             request,
             body,
             delete_all
           ),
         {:ok, status} <- resolution_status(context, doc.document_id, revisions) do
      resolve_conflict_status(context, doc, document_id, revisions, status)
    else
      {:ok, nil} ->
        {:error, ElixirDB.Error.document_not_found("document does not exist")}

      {:error, error} ->
        {:error, error}
    end
  end

  defp resolve_conflict_status(context, doc, _document_id, _revisions, :replayed) do
    with {:ok, winner} <- current_winner(context, doc),
         {:ok, result} <- document_mutation_result(context, doc, winner) do
      {:ok, Map.put(result, :replayed, true)}
    end
  end

  defp resolve_conflict_status(context, doc, document_id, revisions, :new) do
    with :ok <- insert_resolution_revisions(context, doc.document_id, revisions),
         :ok <- remove_pending_for_revisions(context, revisions),
         {:ok, result} <-
           finalize_document(context, document_id, List.first(revisions)) do
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

  defp validate_document_input(context, document_id, deleted, body) do
    config = Map.get(adapter_identity(context), :config, ElixirDB.Config.defaults())
    validate_document_input(context, document_id, deleted, body, config)
  end

  defp validate_document_input(_context, document_id, deleted, body, config) do
    max_id = get_in(config, ["documents", "max_document_id_bytes"]) || 512
    max_body = get_in(config, ["documents", "max_document_bytes"]) || 1_048_576

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

  defp document_mutation_result(context, doc, winner) do
    with {:ok, leaves_now} <- Facts.list_leaves(context, doc.document_id) do
      {:ok,
       publishless_result(
         doc.document_id,
         winner.revision_id,
         doc.update_sequence,
         winner.deleted,
         Winner.conflicts(leaves_now, winner)
       )}
    end
  end

  defp candidate_revision(context, doc, attrs) do
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
               context,
               doc,
               document_id,
               current,
               if_revision,
               deleted,
               history_id
             ),
           {:ok, attachments} <-
             resolve_mutation_attachments(
               context,
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
        candidate_from_revision(context, doc, if_revision, current, revision)
      end
    end
  end

  defp candidate_from_revision(_context, _doc, _if_revision, nil, revision), do: {:ok, revision}

  defp candidate_from_revision(_context, _doc, if_revision, current, revision)
       when if_revision == current.revision_id,
       do: {:ok, revision}

  defp candidate_from_revision(context, doc, if_revision, current, revision) do
    case Facts.find_revision(context, doc.document_id, revision.revision_id) do
      {:ok, existing} ->
        replay_candidate(existing, revision, if_revision, current)

      {:error, %ElixirDB.Error{code: :revision_not_found}} ->
        stale_revision_error(if_revision, current, revision)

      {:error, error} ->
        {:error, error}
    end
  end

  defp replay_candidate(existing, revision, if_revision, current) do
    if Facts.same_revision?(existing, revision),
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

  defp insert_local_revision(context, nil, %Revision{} = candidate, _if_revision, _operation) do
    document_id = candidate.document_id

    with {:ok, _doc} <- Facts.ensure_document(context, document_id),
         :ok <- Facts.insert_revision(context, document_id, candidate),
         :ok <- Facts.clear_pending_for_manifest(context, candidate.attachments),
         {:ok, result} <- finalize_document(context, document_id, candidate) do
      {:ok, Map.put(result, :replayed, false)}
    end
  end

  defp insert_local_revision(context, doc, %Revision{} = candidate, _if_revision, _operation) do
    case Facts.find_revision(context, doc.document_id, candidate.revision_id) do
      {:ok, existing} ->
        existing_local_revision_result(existing, doc, candidate)

      {:error, %ElixirDB.Error{code: :revision_not_found}} ->
        with :ok <- Facts.ensure_parent(context, doc.document_id, candidate.parent_revision),
             :ok <- Facts.insert_revision(context, doc.document_id, candidate),
             :ok <- Facts.clear_pending_for_manifest(context, candidate.attachments),
             {:ok, result} <-
               finalize_document(context, candidate.document_id, candidate) do
          {:ok, Map.put(result, :replayed, false)}
        end

      {:error, error} ->
        {:error, error}
    end
  end

  defp existing_local_revision_result(existing, doc, candidate) do
    if Facts.same_revision?(existing, candidate),
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

  defp finalize_document(context, document_id, _candidate) do
    finalize_document(context, document_id, nil, :load, nil, true)
  end

  defp finalize_document(
         context,
         document_id,
         _candidate,
         ready_indexes,
         allocated_sequence,
         mark_pending?
       ) do
    with {:ok, doc} <- Facts.find_document(context, document_id),
         {:ok, all_leaves} <- Facts.list_leaves(context, document_id),
         {:ok, winner} <- Winner.select(all_leaves),
         {:ok, leaf_json} <- Facts.encode_leaf_set(all_leaves),
         :ok <- Facts.update_winning(context, document_id, winner, 0),
         :ok <- refresh_ready_indexes(context, document_id, winner, ready_indexes),
         {:ok, sequence} <- allocated_or_new_sequence(context, allocated_sequence),
         :ok <- Facts.update_winning(context, document_id, winner, sequence),
         :ok <-
           Facts.append_change(context, %{
             sequence: sequence,
             document_id: document_id,
             winner: winner,
             leaf_json: leaf_json,
             origin: "local",
             backend_meta: Facts.backend_meta(doc)
           }),
         :ok <- mark_pending_if_needed(context, mark_pending?) do
      publishless =
        publishless_result(
          document_id,
          winner.revision_id,
          sequence,
          winner.deleted,
          Winner.conflicts(all_leaves, winner)
        )

      {:ok, publishless}
    end
  end

  defp refresh_ready_indexes(context, document_id, winner, :load),
    do: Facts.refresh_document(context, document_id, winner)

  defp refresh_ready_indexes(_context, _document_id, _winner, []), do: :ok

  defp refresh_ready_indexes(context, document_id, winner, ready_indexes),
    do: Facts.refresh_document(context, document_id, winner, ready_indexes)

  defp allocated_or_new_sequence(_context, sequence) when is_integer(sequence),
    do: {:ok, sequence}

  defp allocated_or_new_sequence(context, nil),
    do: Facts.allocate_sequence(context)

  defp mark_pending_if_needed(_context, false), do: :ok

  defp mark_pending_if_needed(context, true),
    do: Facts.mark_pending_local_causal(context)

  defp prepare_bulk_operations(
         context,
         operations,
         ready_indexes,
         materialize_pending?,
         config,
         document_cache
       ) do
    Enum.reduce_while(operations, {:ok, [], %{}}, fn operation, {:ok, effects, affected} ->
      prepare_bulk_effect(
        context,
        operation,
        effects,
        affected,
        ready_indexes,
        materialize_pending?,
        config,
        document_cache
      )
    end)
    |> case do
      {:ok, effects, affected} -> {:ok, Enum.reverse(effects), affected}
      error -> error
    end
  end

  defp prepare_bulk_effect(
         context,
         operation,
         effects,
         affected,
         ready_indexes,
         materialize_pending?,
         config,
         document_cache
       ) do
    case prepare_bulk_operation(
           context,
           operation,
           ready_indexes,
           materialize_pending?,
           config,
           document_cache
         ) do
      {:ok, effect} ->
        {:cont, {:ok, [effect | effects], update_affected(affected, effect)}}

      {:error, error} ->
        {:halt, {:error, error}}
    end
  end

  defp update_affected(
         affected,
         %{changed: true, document_id: document_id} = effect
       ),
       do:
         Map.put(affected, document_id, %{
           document_id: document_id,
           fast_candidate: Map.get(effect, :fast_candidate)
         })

  defp update_affected(affected, _effect), do: affected

  defp prepare_bulk_operation(
         context,
         request,
         ready_indexes,
         materialize_pending?,
         config,
         document_cache
       ) do
    operation = MapAccess.get(request, :operation, :put)

    case operation do
      operation when operation in [:resolve, "resolve"] ->
        prepare_bulk_resolution_operation(
          context,
          request,
          ready_indexes,
          materialize_pending?,
          config,
          document_cache
        )

      operation when operation in [:put, "put", :delete, "delete"] ->
        prepare_bulk_mutation_operation(
          context,
          request,
          ready_indexes,
          materialize_pending?,
          config,
          document_cache
        )

      _ ->
        {:error, ElixirDB.Error.invalid_request("bulk operation type is invalid")}
    end
  end

  defp prepare_bulk_mutation_operation(
         context,
         request,
         ready_indexes,
         materialize_pending?,
         config,
         document_cache
       ) do
    operation = MapAccess.get(request, :operation, :put)
    document_id = MapAccess.get(request, :document_id)
    if_revision = MapAccess.get(request, :if_revision)
    history_id = MapAccess.get(request, :history_id)
    deleted = operation in [:delete, "delete"]
    body = if(deleted, do: nil, else: MapAccess.get(request, :body))

    with :ok <- validate_mutation_operation(operation),
         :ok <- validate_document_input(context, document_id, deleted, body, config),
         {:ok, doc} <- bulk_document(context, document_cache, document_id),
         {:ok, current} <- current_winner(context, doc),
         {:ok, candidate_state} <-
           candidate_revision(context, doc, %{
             document_id: document_id,
             if_revision: if_revision,
             current: current,
             deleted: deleted,
             body: body,
             history_id: history_id,
             request: request
           }) do
      prepare_bulk_document(
        context,
        doc,
        candidate_state,
        document_id,
        ready_indexes,
        materialize_pending?
      )
    end
  end

  defp prepare_bulk_document(
         context,
         nil,
         candidate,
         document_id,
         ready_indexes,
         true
       ) do
    with {:ok, doc} <- Facts.ensure_document(context, document_id),
         :ok <- Facts.insert_revision_for_document(context, doc, candidate),
         :ok <- Facts.clear_pending_for_manifest(context, candidate.attachments),
         :ok <-
           maybe_update_pending_document(
             context,
             document_id,
             candidate,
             ready_indexes,
             true
           ) do
      {:ok, bulk_effect(document_id, true, false, %{revision: candidate.revision_id, sequence: 0})}
    end
  end

  defp prepare_bulk_document(
         _context,
         nil,
         candidate,
         document_id,
         _ready_indexes,
         false
       ) do
    {:ok,
     bulk_effect(document_id, true, false, %{revision: candidate.revision_id, sequence: 0})
     |> Map.put(:fast_candidate, %{
       revision: candidate,
       body_json: revision_body_json(candidate)
     })}
  end

  defp prepare_bulk_document(
         context,
         doc,
         candidate,
         document_id,
         ready_indexes,
         materialize_pending?
       ) do
    case Facts.find_revision(context, doc.document_id, candidate.revision_id) do
      {:ok, existing} ->
        existing_bulk_revision(existing, doc, candidate, document_id)

      {:error, %ElixirDB.Error{code: :revision_not_found}} ->
        with :ok <- Facts.ensure_parent(context, doc.document_id, candidate.parent_revision),
             :ok <- Facts.insert_revision(context, doc.document_id, candidate),
             :ok <- Facts.clear_pending_for_manifest(context, candidate.attachments),
             :ok <-
               maybe_update_pending_document(
                 context,
                 doc.document_id,
                 candidate,
                 ready_indexes,
                 materialize_pending?
               ) do
          {:ok,
           bulk_effect(
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
    if Facts.same_revision?(existing, candidate),
      do: bulk_replay_revision(doc, candidate, document_id),
      else: {:error, ElixirDB.Error.integrity_violation("revision id has different content")}
  end

  defp bulk_replay_revision(doc, candidate, document_id)
       when doc.winning_revision == candidate.revision_id do
    {:ok,
     bulk_effect(
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

  defp prepare_bulk_resolution_operation(
         context,
         request,
         ready_indexes,
         materialize_pending?,
         config,
         document_cache
       ) do
    document_id = MapAccess.get(request, :document_id)
    expected = MapAccess.get(request, :expected_live_revisions, [])
    body = MapAccess.get(request, :body)
    delete_all = MapAccess.get(request, :delete_all, false)

    with true <- is_boolean(delete_all),
         :ok <-
           validate_document_input(
             context,
             document_id,
             delete_all,
             if(delete_all, do: nil, else: body),
             config
           ),
         true <- is_list(expected) and Enum.all?(expected, &is_binary/1),
         {:ok, %{document_id: _} = doc} <- bulk_document(context, document_cache, document_id),
         {:ok, leaves} <- Facts.list_leaves(context, doc.document_id),
         :ok <- ConflictResolution.validate_leaf_set(leaves, expected),
         {:ok, revisions} <-
           build_resolution_revisions(context, document_id, leaves, request, body, delete_all),
         {:ok, status} <- resolution_status(context, doc.document_id, revisions) do
      prepare_resolution_status(
        context,
        doc,
        document_id,
        revisions,
        status,
        ready_indexes,
        materialize_pending?
      )
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

  defp prepare_resolution_status(
         context,
         doc,
         document_id,
         _revisions,
         :replayed,
         _ready_indexes,
         _materialize_pending?
       ) do
    {:ok, winner} = current_winner(context, doc)

    {:ok,
     bulk_effect(
       document_id,
       false,
       true,
       %{revision: winner.revision_id, sequence: doc.update_sequence}
     )}
  end

  defp prepare_resolution_status(
         context,
         doc,
         document_id,
         revisions,
         :new,
         ready_indexes,
         materialize_pending?
       ) do
    with :ok <- insert_resolution_revisions(context, doc.document_id, revisions),
         :ok <- remove_pending_for_revisions(context, revisions),
         :ok <-
           maybe_update_pending_document(
             context,
             doc.document_id,
             List.first(revisions),
             ready_indexes,
             materialize_pending?
           ) do
      {:ok,
       bulk_effect(
         document_id,
         true,
         false,
         %{revision: List.first(revisions).revision_id, sequence: 0}
       )}
    end
  end

  defp bulk_effect(document_id, changed, replayed, result),
    do: %{
      document_id: document_id,
      changed: changed,
      replayed: replayed,
      result: result
    }

  defp revision_body_json(%Revision{deleted: true}), do: nil
  defp revision_body_json(%Revision{body: body}), do: Canonical.encode!(body)

  defp maybe_update_pending_document(
         _context,
         _document_id,
         _candidate,
         _ready_indexes,
         false
       ),
       do: :ok

  defp maybe_update_pending_document(context, document_id, _candidate, ready_indexes, true),
    do: update_pending_document(context, document_id, ready_indexes)

  defp update_pending_document(context, document_id, ready_indexes) do
    with {:ok, leaves_now} <- Facts.list_leaves(context, document_id),
         {:ok, winner} <- Winner.select(leaves_now),
         :ok <- Facts.update_winning(context, document_id, winner, 0) do
      refresh_ready_indexes(context, document_id, winner, ready_indexes)
    end
  end

  defp finalize_bulk_documents(context, affected, ready_indexes) do
    entries = Enum.sort_by(affected, fn {_document_id, entry} -> entry.document_id end)

    with {:ok, sequences} <- Facts.allocate_sequences(context, length(entries)) do
      finalize_bulk_document_entries(context, entries, sequences, ready_indexes)
    end
  end

  defp finalize_bulk_document_entries(context, entries, sequences, ready_indexes) do
    if all_fast_candidates?(entries) do
      finalize_fast_bulk_documents(context, entries, sequences, ready_indexes)
    else
      Enum.zip(entries, sequences)
      |> Enum.reduce_while({:ok, %{}}, &finalize_bulk_document(&1, &2, context, ready_indexes))
    end
  end

  defp all_fast_candidates?([]), do: false

  defp all_fast_candidates?(entries) do
    Enum.all?(entries, fn
      {_document_id, %{fast_candidate: %{revision: %Revision{}, body_json: _body_json}}} -> true
      _ -> false
    end)
  end

  defp finalize_fast_bulk_documents(context, entries, sequences, ready_indexes) do
    result =
      Enum.zip(entries, sequences)
      |> Enum.reduce_while({:ok, %{}, []}, fn
        {{document_id, %{fast_candidate: %{revision: candidate, body_json: body_json}}}, sequence},
        {:ok, results, changes} ->
          case materialize_new_bulk_document(
                 context,
                 document_id,
                 candidate,
                 body_json,
                 ready_indexes,
                 sequence
               ) do
            {:ok, document, leaf_json} ->
              result =
                publishless_result(
                  document_id,
                  candidate.revision_id,
                  sequence,
                  candidate.deleted,
                  []
                )

              change =
                new_bulk_change(
                  document_id,
                  candidate,
                  sequence,
                  leaf_json,
                  document
                )

              {:cont, {:ok, Map.put(results, document_id, result), [change | changes]}}

            {:error, error} ->
              {:halt, {:error, error}}
          end
      end)

    case result do
      {:ok, results, changes} ->
        with :ok <- Facts.append_changes(context, Enum.reverse(changes)) do
          {:ok, results}
        end

      {:error, _} = error ->
        error
    end
  end

  defp finalize_bulk_document(
         {{document_id, entry}, sequence},
         {:ok, results},
         context,
         ready_indexes
       ) do
    result =
      case entry.fast_candidate do
        %{revision: %Revision{} = candidate, body_json: body_json} ->
          finalize_new_bulk_document(
            context,
            document_id,
            candidate,
            body_json,
            ready_indexes,
            sequence
          )

        _ ->
          finalize_document(
            context,
            document_id,
            nil,
            ready_indexes,
            sequence,
            false
          )
      end

    case result do
      {:ok, result} -> {:cont, {:ok, Map.put(results, document_id, result)}}
      {:error, error} -> {:halt, {:error, error}}
    end
  end

  defp finalize_new_bulk_document(
         context,
         document_id,
         candidate,
         body_json,
         ready_indexes,
         sequence
       ) do
    with {:ok, document, leaf_json} <-
           materialize_new_bulk_document(
             context,
             document_id,
             candidate,
             body_json,
             ready_indexes,
             sequence
           ),
         :ok <-
           Facts.append_change(
             context,
             new_bulk_change(document_id, candidate, sequence, leaf_json, document)
           ) do
      {:ok, publishless_result(document_id, candidate.revision_id, sequence, candidate.deleted, [])}
    end
  end

  defp materialize_new_bulk_document(
         context,
         document_id,
         candidate,
         body_json,
         ready_indexes,
         sequence
       ) do
    with {:ok, leaf_json} <- Facts.encode_leaf_set([candidate]),
         {:ok, document} <-
           Facts.insert_document_with_revision(
             context,
             document_id,
             candidate,
             sequence,
             body_json
           ),
         :ok <- Facts.clear_pending_for_manifest(context, candidate.attachments),
         :ok <- refresh_ready_indexes(context, document_id, candidate, ready_indexes) do
      {:ok, document, leaf_json}
    end
  end

  defp new_bulk_change(document_id, candidate, sequence, leaf_json, document),
    do: %{
      sequence: sequence,
      document_id: document_id,
      winner: candidate,
      leaf_json: leaf_json,
      origin: "local",
      backend_meta: Facts.backend_meta(document)
    }

  defp publishless_result(document_id, revision_id, sequence, deleted, conflicts),
    do: %{
      revision: revision_id,
      sequence: sequence,
      document_id: document_id,
      deleted: deleted,
      conflicts: conflicts
    }

  defp resolution_status(context, document_id, revisions) do
    statuses = Enum.map(revisions, &resolution_revision_status(context, document_id, &1))

    classify_resolution_statuses(statuses)
  end

  defp resolution_revision_status(context, document_id, revision) do
    case Facts.find_revision(context, document_id, revision.revision_id) do
      {:ok, existing} ->
        if(Facts.same_revision?(existing, revision), do: :existing, else: :different)

      {:error, %ElixirDB.Error{code: :revision_not_found}} ->
        :missing

      {:error, _} ->
        :different
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

  defp insert_resolution_revisions(context, document_id, revisions) do
    Enum.reduce_while(revisions, :ok, fn revision, :ok ->
      case Facts.insert_or_accept_revision(context, document_id, revision) do
        :ok -> {:cont, :ok}
        {:error, error} -> {:halt, {:error, error}}
      end
    end)
  end

  defp build_resolution_revisions(context, document_id, leaves, request, body, delete_all) do
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
               resolve_conflict_attachments(context, request, chosen_leaf),
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

  defp resolve_conflict_attachments(context, request, chosen_leaf) do
    case attachment_intent(request) do
      :omitted ->
        Manifest.resolve_inheritance(:resolve_conflict, :omitted, chosen_leaf.attachments)

      :inherit ->
        Manifest.resolve_inheritance(:resolve_conflict, :omitted, chosen_leaf.attachments)

      explicit when is_map(explicit) ->
        with {:ok, coerced} <- coerce_attachment_entries(explicit),
             {:ok, normalized} <- Manifest.normalize(coerced),
             :ok <- Facts.ensure_manifest_reachable(context, normalized) do
          {:ok, normalized}
        end

      _ ->
        {:error, ElixirDB.Error.invalid_request("attachments must be an object")}
    end
  end

  defp resolve_mutation_attachments(
         context,
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
             parent_manifest_for(context, doc, parent_revision_id, intent, operation),
           {:ok, manifest} <- resolve_attachment_intent(operation, intent, parent_manifest),
           :ok <- Facts.ensure_manifest_reachable(context, manifest) do
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

  defp parent_manifest_for(_context, _doc, _parent, intent, :create)
       when intent in [:omitted, :inherit],
       do: {:ok, nil}

  defp parent_manifest_for(_context, _doc, _parent, intent, _operation)
       when is_map(intent),
       do: {:ok, nil}

  defp parent_manifest_for(context, doc, parent_revision_id, intent, :update)
       when intent in [:omitted, :inherit] and is_binary(parent_revision_id) do
    case Facts.find_revision(context, doc.document_id, parent_revision_id) do
      {:ok, parent} -> {:ok, parent.attachments}
      {:error, error} -> {:error, error}
    end
  end

  defp parent_manifest_for(_context, _doc, _parent, intent, :update)
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

  defp remove_pending_for_revisions(context, revisions) do
    Enum.reduce_while(revisions, :ok, fn revision, :ok ->
      case Facts.clear_pending_for_manifest(context, revision.attachments) do
        :ok -> {:cont, :ok}
        {:error, error} -> {:halt, {:error, error}}
      end
    end)
  end

  defp revision_parent_and_history(_context, nil, _document_id, nil, nil, _deleted, history_id) do
    {:ok, {nil, root_history_id(history_id)}}
  end

  defp revision_parent_and_history(
         _context,
         _doc,
         _document_id,
         nil,
         nil,
         _deleted,
         history_id
       ) do
    {:ok, {nil, root_history_id(history_id)}}
  end

  defp revision_parent_and_history(_context, _doc, _document_id, current, nil, false, _history_id)
       when not current.deleted do
    {:ok, {nil, current.history_id}}
  end

  defp revision_parent_and_history(
         _context,
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
         context,
         doc,
         _document_id,
         _current,
         if_revision,
         _deleted,
         _history_id
       )
       when is_binary(if_revision) do
    case Facts.find_revision(context, doc.document_id, if_revision) do
      {:ok, parent} ->
        {:ok, {if_revision, parent.history_id}}

      {:error, %ElixirDB.Error{code: :revision_not_found}} ->
        {:error, ElixirDB.Error.revision_conflict("document does not have the expected parent")}

      {:error, error} ->
        {:error, error}
    end
  end

  defp revision_parent_and_history(
         _context,
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

  defp current_winner(_context, nil), do: {:ok, nil}

  defp current_winner(_context, %{winning_revision: nil}), do: {:ok, nil}

  defp current_winner(context, doc),
    do: Facts.find_revision(context, doc.document_id, doc.winning_revision)

  defp adapter_identity(context) do
    identity = Facts.identity(context)

    if map_size(identity) == 0 do
      %{current_sequence: 0, config: ElixirDB.Config.defaults()}
    else
      identity
    end
  end

  defp validate_mutation_operation(operation) when operation in [:put, "put", :delete, "delete"],
    do: :ok

  defp validate_mutation_operation(_),
    do: {:error, ElixirDB.Error.invalid_request("mutation operation type is invalid")}

  defp digest(id), do: id |> String.split("-", parts: 2) |> List.last()
end
