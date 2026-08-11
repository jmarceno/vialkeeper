defmodule ElixirDB.Storage.SQLite.DocumentFacts do
  @moduledoc """
  SQLite document/revision fact port.

  Physical identifiers such as `doc_key` remain inside opaque `:backend_meta`
  maps owned by this backend.
  """
  @behaviour ElixirDB.Storage.Ports.DocumentFacts

  alias ElixirDB.Domain.Revision
  alias ElixirDB.JSON.StrictDecoder
  alias ElixirDB.Storage.BackendContext
  alias ElixirDB.Storage.Ports.Errors
  alias ElixirDB.Storage.SQLite.{Context, Documents, Revisions}

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
  def list_leaves(%BackendContext{} = context, document_id) when is_binary(document_id) do
    with {:ok, adapter} <- Context.unwrap(context),
         {:ok, doc} <- Documents.find(adapter.conn, document_id),
         {:ok, doc} <- require_document(doc),
         {:ok, doc_key} <- require_doc_key(doc),
         {:ok, leaves} <- Revisions.load_leaves(adapter.conn, doc_key) do
      {:ok, leaves}
    else
      :missing_document ->
        {:error, ElixirDB.Error.document_not_found("document not found")}

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
        {:error, ElixirDB.Error.document_not_found("document not found")}

      {:error, reason} ->
        {:error, Errors.normalize(reason)}
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

  defp insert_or_return_document(_adapter, doc, _document_id) when not is_nil(doc) do
    {:ok, shape_document(doc)}
  end

  defp insert_or_return_document(adapter, nil, document_id) do
    with {:ok, _doc_key} <- Documents.insert(adapter.conn, document_id),
         {:ok, doc} <- Documents.find(adapter.conn, document_id) do
      {:ok, shape_document(doc)}
    else
      {:error, reason} -> {:error, Errors.normalize(reason)}
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
        {:error, ElixirDB.Error.document_not_found("document not found")}

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
        {:error, ElixirDB.Error.document_not_found("document not found")}

      {:error, reason} ->
        {:error, Errors.normalize(reason)}
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
        {:error, ElixirDB.Error.document_not_found("document not found")}

      {:error, reason} ->
        {:error, Errors.normalize(reason)}
    end
  end

  defp require_document(nil), do: :missing_document
  defp require_document(doc), do: {:ok, doc}

  defp require_doc_key(%{doc_key: doc_key}) when not is_nil(doc_key), do: {:ok, doc_key}

  defp require_doc_key(_),
    do: {:error, ElixirDB.Error.internal_error("document fact is missing backend metadata")}

  defp shape_document(nil), do: nil

  defp shape_document(doc) do
    body =
      case Map.get(doc, :winning_body_json) do
        nil ->
          nil

        json when is_binary(json) ->
          case StrictDecoder.decode(json) do
            {:ok, value} -> value
            {:error, _} -> nil
          end
      end

    %{
      document_id: doc.document_id,
      winning_revision: doc.winning_revision,
      winning_deleted: doc.winning_deleted,
      update_sequence: doc.update_sequence,
      body: body,
      backend_meta: %{doc_key: doc.doc_key}
    }
  end
end
