defmodule ElixirDB.Storage.SQLite.Mutations do
  @moduledoc """
  Local mutation and conflict-resolution write workflows for the SQLite adapter.

  Owns apply-local, bulk, and resolve orchestration inside an open IMMEDIATE
  transaction provided by the adapter. Index refresh goes through `IndexCatalog`.
  """

  alias ElixirDB.Domain.Revision
  alias ElixirDB.JSON.Canonical
  alias ElixirDB.Revisions.{Id, Winner}
  alias ElixirDB.Storage.SQLite.{Changes, Documents, IndexCatalog, Revisions}

  @doc """
  Applies one local put/delete mutation inside an open transaction.
  """
  @spec apply_local_tx(map(), map()) :: {:ok, map()} | {:error, ElixirDB.Error.t()}
  def apply_local_tx(adapter, request) do
    operation = request[:operation] || request["operation"] || :put
    document_id = request[:document_id] || request["document_id"]
    if_revision = Map.get(request, :if_revision, Map.get(request, "if_revision"))
    deleted = operation in [:delete, "delete"]
    body = if deleted, do: nil, else: request[:body] || request["body"]

    with :ok <- validate_document_input(adapter, document_id, deleted, body),
         {:ok, doc} <- Documents.find(adapter.conn, document_id),
         {:ok, current} <- current_winner(adapter, doc),
         {:ok, candidate} <-
           candidate_revision(adapter, doc, document_id, if_revision, current, deleted, body) do
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
              # Stale parent: the candidate is only acceptable if an identical revision
              # already exists (a replay). Defer the winner check to insert_local_revision,
              # which rejects superseded retries with operation_already_committed: true and
              # replays only when the candidate is still the winner. A content mismatch under
              # the same revision id is an integrity violation (caught in the unified path).
              case Revisions.find(adapter.conn, doc.doc_key, revision.revision_id) do
                {:ok, existing} ->
                  if Revisions.same?(existing, revision) do
                    {:ok, revision}
                  else
                    stale_revision_error(if_revision, current, revision)
                  end

                {:error, %ElixirDB.Error{code: :revision_not_found}} ->
                  stale_revision_error(if_revision, current, revision)

                {:error, error} ->
                  {:error, error}
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
         :ok <- IndexCatalog.refresh_ready(adapter.conn, doc_key, winner),
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

        doc ->
          # TX-006: the same unified winner check as insert_local_revision. A stale-parent
          # retry that was superseded fails the batch atomically with
          # operation_already_committed: true; only the still-winning candidate replays.
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
         :ok <- IndexCatalog.refresh_ready(adapter.conn, doc_key, winner) do
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

  defp current_winner(_adapter, nil), do: {:ok, nil}

  defp current_winner(adapter, doc),
    do: Revisions.find(adapter.conn, doc.doc_key, doc.winning_revision)

  defp adapter_identity(adapter) do
    case ElixirDB.Storage.SQLite.Adapter.identity(adapter) do
      {:ok, value} -> value
      _ -> %{current_sequence: 0, config: ElixirDB.Config.defaults()}
    end
  end

  defp digest(id), do: id |> String.split("-", parts: 2) |> List.last()
end
