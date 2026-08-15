defmodule VialKeeper.Storage.SQLite.DocumentFacts do
  @moduledoc """
  SQLite document/revision fact port.

  Physical identifiers such as `doc_key` remain inside opaque `:backend_meta`
  maps owned by this backend.
  """
  @behaviour VialKeeper.Storage.Ports.DocumentFacts

  alias VialKeeper.Domain.Revision
  alias VialKeeper.JSON.{Canonical, StrictDecoder}
  alias VialKeeper.Storage.BackendContext
  alias VialKeeper.Storage.Ports.Errors
  alias VialKeeper.Storage.SQLite.{Connection, Context, Documents, Revisions}

  @impl true
  def find_document(%BackendContext{} = context, document_id) when is_binary(document_id) do
    with {:ok, adapter} <- Context.unwrap(context),
         {:ok, doc} <- Documents.find(adapter.conn, document_id) do
      {:ok, shape_document(doc)}
    else
      {:error, reason} -> {:error, Errors.normalize(reason)}
    end
  end

  @impl true
  def find_documents(%BackendContext{} = context, document_ids) when is_list(document_ids) do
    with {:ok, adapter} <- Context.unwrap(context),
         {:ok, docs} <- Documents.find_many(adapter.conn, document_ids) do
      {:ok, Map.new(docs, fn {id, doc} -> {id, shape_document(doc)} end)}
    else
      {:error, reason} -> {:error, Errors.normalize(reason)}
    end
  end

  @impl true
  def find_revision(%BackendContext{} = context, document_id, revision_id)
      when is_binary(document_id) and is_binary(revision_id) do
    with {:ok, adapter} <- Context.unwrap(context),
         {:ok, doc} <- Documents.find(adapter.conn, document_id),
         {:ok, doc} <- require_document(doc),
         {:ok, doc_key} <- require_doc_key(doc),
         {:ok, revision} <- Revisions.find(adapter.conn, doc_key, revision_id) do
      {:ok, revision}
    else
      :missing_document ->
        {:ok, nil}

      {:error, reason} ->
        {:error, Errors.normalize(reason)}
    end
  end

  @impl true
  def find_revision_batch(%BackendContext{} = context, requests) when is_list(requests) do
    with {:ok, adapter} <- Context.unwrap(context),
         ids = Enum.map(requests, & &1.document_id),
         {:ok, raw_documents} <- Documents.find_many(adapter.conn, ids),
         {:ok, revisions} <- find_batch_revisions(adapter.conn, requests, raw_documents),
         documents <- Map.new(raw_documents, fn {id, doc} -> {id, shape_document(doc)} end) do
      {:ok,
       Enum.map(requests, fn %{document_id: document_id, revision_id: revision_id} ->
         %{
           document_id: document_id,
           revision_id: revision_id,
           document: Map.get(documents, document_id),
           revision: Map.get(revisions, {document_id, revision_id})
         }
       end)}
    else
      {:error, reason} -> {:error, Errors.normalize(reason)}
    end
  end

  defp find_batch_revisions(_conn, [], _documents), do: {:ok, %{}}

  defp find_batch_revisions(conn, requests, documents) do
    pairs =
      requests
      |> Enum.uniq()
      |> Enum.filter(fn %{document_id: document_id} ->
        not is_nil(Map.get(documents, document_id))
      end)

    Enum.reduce_while(Enum.chunk_every(pairs, 100), {:ok, %{}}, fn chunk, {:ok, acc} ->
      case find_batch_revision_chunk(conn, chunk, documents) do
        {:ok, revisions} -> {:cont, {:ok, Map.merge(acc, revisions)}}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
  end

  defp find_batch_revision_chunk(conn, chunk, documents) do
    conditions = Enum.map_join(chunk, " OR ", fn _ -> "(r.doc_key = ? AND r.revision_id = ?)" end)

    sql =
      "SELECT r.doc_key, r.revision_id, r.generation, r.parent_revision, r.history_id, r.digest, r.deleted, r.body_json, r.body_term, r.insertion_sequence FROM revisions AS r WHERE #{conditions}"

    params =
      Enum.flat_map(chunk, fn %{document_id: document_id, revision_id: revision_id} ->
        [documents[document_id].doc_key, revision_id]
      end)

    case Connection.query(conn, sql, params) do
      {:ok, rows} -> {:ok, Map.new(rows, &batch_revision_from_row(&1, documents))}
      {:error, reason} -> {:error, Errors.normalize(reason)}
    end
  end

  defp batch_revision_from_row(
         [
           doc_key,
           revision_id,
           generation,
           parent,
           history_id,
           digest,
           deleted,
           body_json,
           body_term,
           sequence
         ],
         documents
       ) do
    document_id =
      Enum.find_value(documents, fn {id, doc} -> if doc && doc.doc_key == doc_key, do: id end)

    revision =
      Revisions.from_row(
        [
          revision_id,
          generation,
          parent,
          history_id,
          digest,
          deleted,
          body_json,
          body_term,
          sequence
        ],
        %{}
      )

    {{document_id, revision.revision_id}, revision}
  end

  @impl true
  def list_leaves(%BackendContext{} = context, document_id) when is_binary(document_id) do
    with {:ok, adapter} <- Context.unwrap(context),
         {:ok, doc} <- Documents.find(adapter.conn, document_id),
         {:ok, doc} <- require_document(doc),
         {:ok, doc_key} <- require_doc_key(doc),
         {:ok, leaves} <- Revisions.load_leaves(adapter.conn, doc_key) do
      {:ok, leaves}
    else
      :missing_document ->
        {:error, VialKeeper.Error.document_not_found("document not found")}

      {:error, reason} ->
        {:error, Errors.normalize(reason)}
    end
  end

  @impl true
  def list_ancestors(%BackendContext{} = context, document_id, revision_id)
      when is_binary(document_id) and is_binary(revision_id) do
    with {:ok, adapter} <- Context.unwrap(context),
         {:ok, doc} <- Documents.find(adapter.conn, document_id),
         {:ok, doc} <- require_document(doc),
         {:ok, doc_key} <- require_doc_key(doc),
         {:ok, ancestors} <- Revisions.load_ancestors(adapter.conn, doc_key, revision_id) do
      {:ok, ancestors}
    else
      :missing_document ->
        {:error, VialKeeper.Error.document_not_found("document not found")}

      {:error, reason} ->
        {:error, Errors.normalize(reason)}
    end
  end

  @impl true
  def list_document_page(%BackendContext{} = context, cursor, limit)
      when (is_nil(cursor) or is_binary(cursor)) and is_integer(limit) and limit > 0 do
    with {:ok, adapter} <- Context.unwrap(context),
         {:ok, {ids, next}} <- Documents.list_page(adapter.conn, cursor, limit) do
      {:ok, %{document_ids: ids, next_cursor: next}}
    else
      {:error, reason} -> {:error, Errors.normalize(reason)}
    end
  end

  @impl true
  def ensure_document(%BackendContext{} = context, document_id) when is_binary(document_id) do
    with {:ok, adapter} <- Context.unwrap(context),
         {:ok, existing} <- Documents.find(adapter.conn, document_id) do
      insert_or_return_document(adapter, existing, document_id)
    else
      {:error, reason} -> {:error, Errors.normalize(reason)}
    end
  end

  @impl true
  def insert_document_with_revision(
        %BackendContext{} = context,
        document_id,
        %Revision{} = revision,
        sequence,
        body_json
      )
      when is_binary(document_id) and is_integer(sequence) and sequence >= 0 do
    with {:ok, adapter} <- Context.unwrap(context),
         :ok <- validate_revision_document(document_id, revision),
         body_json <- materialized_body_json(revision, body_json),
         {:ok, doc_key} <-
           Documents.insert_with_winner(
             adapter.conn,
             document_id,
             revision,
             sequence,
             body_json
           ),
         :ok <- Errors.wrap(Revisions.insert(adapter.conn, doc_key, revision, body_json)) do
      {:ok, materialized_document(document_id, doc_key, revision, sequence, body_json)}
    else
      {:error, reason} -> {:error, Errors.normalize(reason)}
    end
  end

  defp insert_or_return_document(_adapter, doc, _document_id) when not is_nil(doc) do
    {:ok, shape_document(doc)}
  end

  defp insert_or_return_document(adapter, nil, document_id) do
    case Documents.insert(adapter.conn, document_id) do
      {:ok, doc_key} ->
        {:ok, placeholder_document(document_id, doc_key)}

      {:error, reason} ->
        {:error, Errors.normalize(reason)}
    end
  end

  defp placeholder_document(document_id, doc_key) do
    shape_document(doc_key, document_id, nil, nil, true, 0)
  end

  @impl true
  def ensure_parent(%BackendContext{} = context, document_id, parent)
      when is_binary(document_id) do
    with {:ok, adapter} <- Context.unwrap(context),
         {:ok, doc} <- Documents.find(adapter.conn, document_id),
         {:ok, doc} <- require_document(doc),
         {:ok, doc_key} <- require_doc_key(doc) do
      Errors.wrap(Revisions.ensure_parent(adapter.conn, doc_key, parent))
    else
      :missing_document ->
        {:error, VialKeeper.Error.document_not_found("document not found")}

      {:error, reason} ->
        {:error, Errors.normalize(reason)}
    end
  end

  @impl true
  def insert_revision(%BackendContext{} = context, document_id, %Revision{} = revision)
      when is_binary(document_id) do
    with {:ok, adapter} <- Context.unwrap(context),
         {:ok, doc} <- Documents.find(adapter.conn, document_id),
         {:ok, doc} <- require_document(doc),
         {:ok, doc_key} <- require_doc_key(doc) do
      Errors.wrap(Revisions.insert(adapter.conn, doc_key, revision))
    else
      :missing_document ->
        {:error, VialKeeper.Error.document_not_found("document not found")}

      {:error, reason} ->
        {:error, Errors.normalize(reason)}
    end
  end

  @impl true
  def insert_revision_with_body(
        %BackendContext{} = context,
        document_id,
        %Revision{} = revision,
        body_json
      )
      when is_binary(document_id) do
    with {:ok, adapter} <- Context.unwrap(context),
         {:ok, doc} <- Documents.find(adapter.conn, document_id),
         {:ok, doc} <- require_document(doc),
         {:ok, doc_key} <- require_doc_key(doc) do
      Errors.wrap(Revisions.insert(adapter.conn, doc_key, revision, body_json))
    else
      :missing_document -> {:error, VialKeeper.Error.document_not_found("document not found")}
      {:error, reason} -> {:error, Errors.normalize(reason)}
    end
  end

  @impl true
  def insert_revision_for_document(
        %BackendContext{} = context,
        document,
        %Revision{} = revision
      ) do
    with {:ok, adapter} <- Context.unwrap(context),
         {:ok, doc_key} <- fact_doc_key(document, revision.document_id) do
      Errors.wrap(Revisions.insert(adapter.conn, doc_key, revision))
    else
      {:error, reason} -> {:error, Errors.normalize(reason)}
    end
  end

  @impl true
  def insert_or_accept_revision(%BackendContext{} = context, document_id, %Revision{} = revision)
      when is_binary(document_id) do
    with {:ok, adapter} <- Context.unwrap(context),
         {:ok, doc} <- Documents.find(adapter.conn, document_id),
         {:ok, doc} <- require_document(doc),
         {:ok, doc_key} <- require_doc_key(doc) do
      Errors.wrap(Revisions.insert_or_accept(adapter.conn, doc_key, revision))
    else
      :missing_document ->
        {:error, VialKeeper.Error.document_not_found("document not found")}

      {:error, reason} ->
        {:error, Errors.normalize(reason)}
    end
  end

  @impl true
  def update_winning(%BackendContext{} = context, document_id, %Revision{} = winner, sequence)
      when is_binary(document_id) and is_integer(sequence) and sequence >= 0 do
    with {:ok, adapter} <- Context.unwrap(context),
         {:ok, doc} <- Documents.find(adapter.conn, document_id),
         {:ok, doc} <- require_document(doc),
         {:ok, doc_key} <- require_doc_key(doc) do
      Errors.wrap(Documents.update(adapter.conn, doc_key, winner, sequence))
    else
      :missing_document ->
        {:error, VialKeeper.Error.document_not_found("document not found")}

      {:error, reason} ->
        {:error, Errors.normalize(reason)}
    end
  end

  @impl true
  def update_winning_with_body(
        %BackendContext{} = context,
        document_id,
        %Revision{} = winner,
        sequence,
        body_json
      )
      when is_binary(document_id) and is_integer(sequence) and sequence >= 0 do
    with {:ok, adapter} <- Context.unwrap(context),
         {:ok, doc} <- Documents.find(adapter.conn, document_id),
         {:ok, doc} <- require_document(doc),
         {:ok, doc_key} <- require_doc_key(doc) do
      Errors.wrap(Documents.update(adapter.conn, doc_key, winner, sequence, body_json))
    else
      :missing_document -> {:error, VialKeeper.Error.document_not_found("document not found")}
      {:error, reason} -> {:error, Errors.normalize(reason)}
    end
  end

  @impl true
  def update_winning_for_document(
        %BackendContext{} = context,
        document,
        %Revision{} = winner,
        sequence
      )
      when is_integer(sequence) and sequence >= 0 do
    with {:ok, adapter} <- Context.unwrap(context),
         {:ok, doc_key} <- fact_doc_key(document, winner.document_id) do
      Errors.wrap(Documents.update(adapter.conn, doc_key, winner, sequence))
    else
      {:error, reason} -> {:error, Errors.normalize(reason)}
    end
  end

  @impl true
  def empty_document(%BackendContext{} = context, document_id) when is_binary(document_id) do
    with {:ok, adapter} <- Context.unwrap(context),
         {:ok, doc} <- Documents.find(adapter.conn, document_id),
         {:ok, doc} <- require_document(doc),
         {:ok, doc_key} <- require_doc_key(doc) do
      Errors.wrap(Documents.empty(adapter.conn, doc_key))
    else
      :missing_document ->
        {:error, VialKeeper.Error.document_not_found("document not found")}

      {:error, reason} ->
        {:error, Errors.normalize(reason)}
    end
  end

  @impl true
  def delete_history(%BackendContext{} = context, document_id, history_id)
      when is_binary(document_id) and is_binary(history_id) do
    with {:ok, adapter} <- Context.unwrap(context),
         {:ok, doc} <- Documents.find(adapter.conn, document_id),
         {:ok, doc} <- require_document(doc),
         {:ok, doc_key} <- require_doc_key(doc) do
      case Connection.execute(
             adapter.conn,
             "DELETE FROM revisions WHERE doc_key = ? AND history_id = ?",
             [doc_key, history_id]
           ) do
        :ok -> :ok
        {:error, reason} -> {:error, Errors.normalize(reason)}
      end
    else
      :missing_document ->
        :ok

      {:error, reason} ->
        {:error, Errors.normalize(reason)}
    end
  end

  @impl true
  def list_compaction_documents(%BackendContext{} = context, candidate_floor)
      when is_integer(candidate_floor) and candidate_floor >= 0 do
    with {:ok, adapter} <- Context.unwrap(context),
         {:ok, rows} <-
           Connection.query(
             adapter.conn,
             "SELECT doc_key, document_id, winning_revision, update_sequence FROM documents WHERE update_sequence <= ?",
             [candidate_floor]
           ),
         {:ok, documents} <- compaction_documents(adapter.conn, rows) do
      {:ok, documents}
    else
      {:error, reason} ->
        {:error, Errors.normalize(reason)}
    end
  end

  @impl true
  def delete_revisions(%BackendContext{} = context, document_id, revision_ids)
      when is_binary(document_id) and is_list(revision_ids) do
    with {:ok, adapter} <- Context.unwrap(context),
         {:ok, doc} <- Documents.find(adapter.conn, document_id) do
      delete_document_revisions(adapter.conn, doc, revision_ids)
    else
      {:error, reason} -> {:error, Errors.normalize(reason)}
    end
  end

  defp compaction_documents(_conn, []), do: {:ok, []}

  defp compaction_documents(conn, rows) do
    doc_keys =
      Enum.map(rows, fn [doc_key, _document_id, _winning_revision, _update_sequence] -> doc_key end)

    doc_key_to_id =
      Map.new(rows, fn [doc_key, document_id, _winning_revision, _update_sequence] ->
        {doc_key, document_id}
      end)

    with {:ok, revision_rows} <- load_compaction_revisions(conn, doc_keys, doc_key_to_id) do
      metadata =
        Enum.group_by(revision_rows, fn {doc_key, _revision} -> doc_key end, fn {
                                                                                  _doc_key,
                                                                                  revision
                                                                                } ->
          revision
        end)

      {:ok,
       Enum.map(rows, fn [doc_key, document_id, winning_revision, update_sequence] ->
         %{
           document_id: document_id,
           latest_change_sequence: update_sequence,
           winning_revision: winning_revision,
           revisions: Map.get(metadata, doc_key, [])
         }
       end)}
    end
  end

  defp load_compaction_revisions(conn, doc_keys, doc_key_to_id) do
    placeholders = Enum.map_join(doc_keys, ",", fn _ -> "?" end)

    case Connection.query(
           conn,
           "SELECT doc_key, revision_id, generation, parent_revision, history_id, digest, deleted, insertion_sequence FROM revisions WHERE doc_key IN (#{placeholders})",
           doc_keys
         ) do
      {:ok, rows} ->
        {:ok,
         Enum.map(rows, fn [
                             doc_key,
                             revision_id,
                             generation,
                             parent,
                             history_id,
                             digest,
                             deleted,
                             sequence
                           ] ->
           {doc_key,
            %Revision{
              document_id: Map.fetch!(doc_key_to_id, doc_key),
              revision_id: revision_id,
              generation: generation,
              parent_revision: parent,
              history_id: history_id,
              digest: digest,
              deleted: deleted == 1,
              body: nil,
              attachments: %{},
              insertion_sequence: sequence
            }}
         end)}

      {:error, reason} ->
        {:error, Errors.normalize(reason)}
    end
  end

  defp delete_document_revisions(_conn, nil, _revision_ids), do: :ok

  defp delete_document_revisions(conn, doc, revision_ids) do
    with {:ok, doc_key} <- require_doc_key(doc) do
      delete_revision_ids(conn, doc_key, revision_ids)
    end
  end

  defp delete_revision_ids(_conn, _doc_key, []), do: :ok

  defp delete_revision_ids(conn, doc_key, revision_ids) do
    Enum.reduce_while(Enum.chunk_every(revision_ids, 100), :ok, fn chunk, :ok ->
      placeholders = Enum.map_join(chunk, ",", fn _ -> "?" end)

      case Connection.execute(
             conn,
             "DELETE FROM revisions WHERE doc_key = ? AND revision_id IN (#{placeholders})",
             [doc_key | chunk]
           ) do
        :ok -> {:cont, :ok}
        {:error, reason} -> {:halt, {:error, Errors.normalize(reason)}}
      end
    end)
  end

  defp require_document(nil), do: :missing_document
  defp require_document(doc), do: {:ok, doc}

  defp require_doc_key(%{doc_key: doc_key}) when not is_nil(doc_key), do: {:ok, doc_key}

  defp require_doc_key(_),
    do: {:error, VialKeeper.Error.internal_error("document fact is missing backend metadata")}

  defp fact_doc_key(%{document_id: document_id}, revision_document_id)
       when document_id != revision_document_id do
    {:error, VialKeeper.Error.integrity_violation("document fact does not match revision")}
  end

  defp fact_doc_key(%{backend_meta: %{doc_key: doc_key}}, _revision_document_id)
       when is_integer(doc_key),
       do: {:ok, doc_key}

  defp fact_doc_key(_document, _revision_document_id),
    do: {:error, VialKeeper.Error.internal_error("document fact is missing backend metadata")}

  defp validate_revision_document(document_id, %Revision{document_id: document_id}), do: :ok

  defp validate_revision_document(_document_id, _revision),
    do: {:error, VialKeeper.Error.integrity_violation("revision document does not match document")}

  defp materialized_body_json(%Revision{deleted: true}, _body_json), do: nil
  defp materialized_body_json(%Revision{}, body_json) when is_binary(body_json), do: body_json

  defp materialized_body_json(%Revision{body: body}, _body_json),
    do: Canonical.encode!(body)

  defp materialized_document(document_id, doc_key, revision, sequence, body_json) do
    shape_document(
      doc_key,
      document_id,
      revision.revision_id,
      body_json,
      revision.deleted,
      sequence
    )
  end

  defp shape_document(nil), do: nil

  defp shape_document(doc) do
    shape_document(
      doc.doc_key,
      doc.document_id,
      doc.winning_revision,
      doc.winning_body_json,
      doc.winning_deleted,
      doc.update_sequence
    )
  end

  defp shape_document(
         doc_key,
         document_id,
         winning_revision,
         winning_body_json,
         winning_deleted,
         update_sequence
       ) do
    body =
      case winning_body_json do
        nil ->
          nil

        json when is_binary(json) ->
          case StrictDecoder.decode(json) do
            {:ok, value} -> value
            {:error, _} -> nil
          end
      end

    %{
      document_id: document_id,
      winning_revision: winning_revision,
      winning_deleted: winning_deleted,
      update_sequence: update_sequence,
      body: body,
      backend_meta: %{doc_key: doc_key}
    }
  end
end
