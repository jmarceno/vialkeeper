defmodule ElixirDB.Storage.SQLite.Import do
  @moduledoc """
  Revision-chain import write workflows for the Version 1 SQLite adapter.

  Owns chain validation, revision insertion, and replication-sourced change
  finalization inside an open IMMEDIATE transaction provided by the adapter.
  """

  alias ElixirDB.Domain.Revision
  alias ElixirDB.JSON.Canonical
  alias ElixirDB.Revisions.{Id, Winner}
  alias ElixirDB.Storage.SQLite.{Changes, Documents, IndexCatalog, Revisions}

  @doc """
  Validates host limits for a replication chain batch.
  """
  @spec validate_chain_batch(term()) :: :ok | {:error, ElixirDB.Error.t()}
  def validate_chain_batch(chains) when is_list(chains) do
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

  def validate_chain_batch(_),
    do: {:error, ElixirDB.Error.invalid_request("replication chains must be an array")}

  @doc """
  Imports revision chains inside an open transaction.
  """
  @spec import_tx(map(), map()) :: {:ok, map()} | {:error, ElixirDB.Error.t()}
  def import_tx(adapter, request) do
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
             :ok <- IndexCatalog.refresh_ready(adapter.conn, doc_key, winner),
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

  defp digest(id), do: id |> String.split("-", parts: 2) |> List.last()
end
