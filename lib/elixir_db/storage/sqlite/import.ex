defmodule ElixirDB.Storage.SQLite.Import do
  @moduledoc """
  Revision-chain import write workflows for the Version 1 SQLite adapter.

  Owns chain validation, revision insertion, and replication-sourced change
  finalization inside an open IMMEDIATE transaction provided by the adapter.
  """

  alias ElixirDB.Domain.Revision
  alias ElixirDB.JSON.Canonical
  alias ElixirDB.MapAccess
  alias ElixirDB.Observability.Instrumentation.Import, as: ImportInstrumentation
  alias ElixirDB.Revisions.{Id, Winner}
  alias ElixirDB.Storage.SQLite.{Changes, Documents, IndexCatalog, RetentionRecords, Revisions}

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
              :history_id,
              :leaf_revision,
              :revisions,
              :truncated,
              "document_id",
              "history_id",
              "leaf_revision",
              "revisions",
              "truncated"
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
    chains = MapAccess.get(request, :chains, [])

    with {:ok, revisions} <- validate_chains(chains),
         {:ok, %{affected: affected, inserted: inserted}} <-
           insert_imported_revisions(adapter, revisions) do
      finalize_imports(adapter, affected, inserted)
    end
  end

  defp validate_chains(chains) do
    Enum.reduce_while(chains, {:ok, []}, fn chain, {:ok, acc} ->
      validate_chain_entry(chain, acc)
    end)
    |> case do
      {:ok, revisions} -> {:ok, revisions}
      error -> error
    end
  end

  defp validate_chain_entry(chain, acc) when is_map(chain) do
    document_id = MapAccess.get(chain, :document_id)
    leaf_revision = MapAccess.get(chain, :leaf_revision)
    revisions = MapAccess.get(chain, :revisions, [])
    truncated = MapAccess.get(chain, :truncated, false)

    if valid_chain_shape?(chain, document_id, leaf_revision, revisions) do
      append_validated_chain(document_id, leaf_revision, revisions, truncated, acc)
    else
      {:halt,
       {:error,
        ElixirDB.Error.invalid_request("revision chain requires document, leaf, and revisions")}}
    end
  end

  defp validate_chain_entry(_chain, _acc),
    do: {:halt, {:error, ElixirDB.Error.invalid_request("revision chains must be objects")}}

  defp append_validated_chain(document_id, leaf_revision, revisions, truncated, acc) do
    case validate_chain_revisions(document_id, revisions, truncated) do
      {:ok, ^leaf_revision, built} ->
        entries =
          built
          |> Enum.reverse()
          |> Enum.with_index()
          |> Enum.map(fn {revision, index} -> {revision, truncated and index == 0} end)

        {:cont, {:ok, acc ++ entries}}

      {:ok, _parent, _built} ->
        {:halt,
         {:error, ElixirDB.Error.integrity_violation("revision chain leaf does not match payload")}}

      {:error, error} ->
        {:halt, {:error, error}}
    end
  end

  defp valid_chain_shape?(chain, document_id, leaf_revision, revisions) do
    allowed_keys = [
      :document_id,
      :history_id,
      :leaf_revision,
      :revisions,
      :truncated,
      "document_id",
      "history_id",
      "leaf_revision",
      "revisions",
      "truncated"
    ]

    Enum.all?(Map.keys(chain), &(&1 in allowed_keys)) and is_binary(document_id) and
      document_id != "" and is_binary(leaf_revision) and is_list(revisions) and revisions != []
  end

  defp validate_chain_revisions(document_id, revisions, truncated) do
    Enum.reduce_while(revisions, {:ok, :root, []}, fn raw, {:ok, expected_parent, built} ->
      allow_any_parent = truncated and built == []
      validate_revision_order(document_id, raw, expected_parent, built, allow_any_parent)
    end)
  end

  defp validate_revision_order(document_id, raw, expected_parent, built, allow_any_parent) do
    with {:ok, revision} <- imported_revision(document_id, raw),
         true <-
           allow_any_parent or
             (expected_parent == :root and is_nil(revision.parent_revision)) or
             revision.parent_revision == expected_parent do
      {:cont, {:ok, revision.revision_id, [revision | built]}}
    else
      false ->
        {:halt,
         {:error, ElixirDB.Error.integrity_violation("revision chain is not parent ordered")}}

      {:error, error} ->
        {:halt, {:error, error}}
    end
  end

  defp imported_revision(_document_id, raw) when not is_map(raw),
    do: {:error, ElixirDB.Error.invalid_request("revision chain entries must be objects")}

  defp imported_revision(document_id, raw) do
    allowed_keys = [
      :document_id,
      :revision_id,
      :generation,
      :parent_revision,
      :history_id,
      :deleted,
      :body,
      "document_id",
      "revision_id",
      "generation",
      "parent_revision",
      "history_id",
      "deleted",
      "body"
    ]

    if Enum.all?(Map.keys(raw), &(&1 in allowed_keys)) do
      generation_value = MapAccess.get(raw, :generation)
      revision_id = MapAccess.get(raw, :revision_id)
      parent = MapAccess.get(raw, :parent_revision)
      history_id = MapAccess.get(raw, :history_id)
      deleted = MapAccess.get(raw, :deleted, false)
      body = MapAccess.get(raw, :body)

      with :ok <- validate_import_history_id(history_id),
           {:ok, calculated} <- Id.calculate(document_id, history_id, parent, deleted, body),
           true <- calculated == revision_id,
           {:ok, generation} <- Id.generation(revision_id),
           true <- generation_value == generation,
           true <- is_boolean(deleted),
           true <- deleted or is_map(body),
           true <- deleted or body_size_within_limit?(body) do
        {:ok,
         %Revision{
           document_id: document_id,
           history_id: history_id,
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

  defp validate_import_history_id(history_id)
       when is_binary(history_id) and history_id != "",
       do: :ok

  defp validate_import_history_id(_),
    do: {:error, ElixirDB.Error.invalid_request("revision chain history_id is required")}

  defp body_size_within_limit?(body) do
    case Canonical.encode(body) do
      {:ok, json} ->
        byte_size(json) <= (ElixirDB.Config.host_limits()[:max_document_bytes] || 1_048_576)

      {:error, _} ->
        false
    end
  end

  defp insert_imported_revisions(adapter, revisions) do
    with {:ok, boundaries} <- RetentionRecords.list_boundaries(adapter.conn) do
      import_revision_batch(adapter, revisions, boundaries)
    end
  end

  defp import_revision_batch(adapter, revisions, boundaries) do
    uuid = Map.get(adapter.identity, :database_uuid)

    Enum.reduce_while(revisions, {:ok, MapSet.new(), 0, 0}, fn {revision, allow_dangling_parent},
                                                               {:ok, affected, inserted, stale} ->
      import_revision_entry(
        adapter,
        revision,
        allow_dangling_parent,
        boundaries,
        affected,
        inserted,
        stale
      )
    end)
    |> finalize_import_batch(uuid)
  end

  defp import_revision_entry(
         adapter,
         revision,
         allow_dangling_parent,
         boundaries,
         affected,
         inserted,
         stale
       ) do
    if compacted_stale?(revision, boundaries),
      do: {:cont, {:ok, affected, inserted, stale + 1}},
      else:
        insert_imported_revision(
          adapter,
          revision,
          allow_dangling_parent,
          affected,
          inserted,
          stale
        )
  end

  defp finalize_import_batch({:ok, affected, inserted, stale}, uuid) do
    ImportInstrumentation.stale_fence_noop(uuid, stale)
    {:ok, %{affected: affected, inserted: inserted}}
  end

  defp finalize_import_batch({:error, _} = error, _uuid), do: error

  defp insert_imported_revision(adapter, revision, allow_dangling_parent, affected, inserted, stale) do
    case Documents.find(adapter.conn, revision.document_id) do
      {:ok, nil} ->
        insert_new_revision(adapter, revision, allow_dangling_parent, affected, inserted, stale)

      {:error, %ElixirDB.Error{code: :document_not_found}} ->
        insert_new_revision(adapter, revision, allow_dangling_parent, affected, inserted, stale)

      {:ok, doc} ->
        insert_existing_revision(
          adapter,
          doc,
          revision,
          allow_dangling_parent,
          affected,
          inserted,
          stale
        )

      {:error, error} ->
        {:halt, {:error, error}}
    end
  end

  defp insert_new_revision(adapter, revision, allow_dangling_parent, affected, inserted, stale) do
    with {:ok, doc_key} <- Documents.insert(adapter.conn, revision.document_id),
         :ok <- ensure_import_parent(adapter, doc_key, revision, allow_dangling_parent),
         :ok <- Revisions.insert(adapter.conn, doc_key, revision) do
      {:cont, {:ok, MapSet.put(affected, {doc_key, revision.document_id}), inserted + 1, stale}}
    else
      {:error, error} -> {:halt, {:error, error}}
    end
  end

  defp insert_existing_revision(
         adapter,
         doc,
         revision,
         allow_dangling_parent,
         affected,
         inserted,
         stale
       ) do
    case Revisions.find(adapter.conn, doc.doc_key, revision.revision_id) do
      {:ok, existing} ->
        existing_revision_result(existing, revision, affected, inserted, stale)

      {:error, %ElixirDB.Error{code: :revision_not_found}} ->
        with :ok <- ensure_import_parent(adapter, doc.doc_key, revision, allow_dangling_parent),
             :ok <- Revisions.insert(adapter.conn, doc.doc_key, revision) do
          {:cont,
           {:ok, MapSet.put(affected, {doc.doc_key, revision.document_id}), inserted + 1, stale}}
        else
          {:error, error} -> {:halt, {:error, error}}
        end

      {:error, error} ->
        {:halt, {:error, error}}
    end
  end

  defp existing_revision_result(existing, revision, affected, inserted, stale) do
    if Revisions.same?(existing, revision) do
      {:cont, {:ok, affected, inserted, stale}}
    else
      {:halt,
       {:error,
        ElixirDB.Error.integrity_violation("existing revision differs from imported revision")}}
    end
  end

  defp ensure_import_parent(_adapter, _doc_key, %Revision{parent_revision: nil}, _allow_dangling),
    do: :ok

  defp ensure_import_parent(adapter, doc_key, revision, true) do
    case revision.parent_revision do
      nil -> :ok
      parent -> parent_present_or_allowed(adapter, doc_key, parent)
    end
  end

  defp ensure_import_parent(adapter, doc_key, revision, false),
    do: Revisions.ensure_parent(adapter.conn, doc_key, revision.parent_revision)

  defp parent_present_or_allowed(adapter, doc_key, parent) do
    case Revisions.find(adapter.conn, doc_key, parent) do
      {:ok, _} -> :ok
      {:error, %ElixirDB.Error{code: :revision_not_found}} -> :ok
      {:error, error} -> {:error, error}
    end
  end

  defp compacted_stale?(revision, boundaries) do
    Enum.any?(boundaries, fn %{boundary: boundary} ->
      boundary.document_id == revision.document_id and boundary.history_id == revision.history_id and
        (boundary.retired or
           (is_integer(boundary.minimum_retained_generation) and
              revision.generation < boundary.minimum_retained_generation))
    end)
  end

  defp finalize_imports(adapter, affected, inserted) do
    affected = Enum.sort_by(affected, fn {_doc_key, document_id} -> document_id end)

    Enum.reduce_while(
      affected,
      {:ok, %{documents_changed: 0, revisions_inserted: inserted, last_sequence: 0}},
      fn {doc_key, document_id}, {:ok, acc} ->
        finalize_import_document(adapter, doc_key, document_id, acc)
      end
    )
    |> case do
      {:ok, result} -> {:ok, result}
      error -> error
    end
  end

  defp finalize_import_document(adapter, doc_key, document_id, acc) do
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

  defp digest(id), do: id |> String.split("-", parts: 2) |> List.last()
end
