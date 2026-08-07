defmodule ElixirDB.Storage.SQLite.Documents do
  @moduledoc """
  Document-row SQL helpers for the Version 1 SQLite adapter.

  Owns find/insert/update against the `documents` table and winner materialization
  result shaping. Mutation orchestration and transaction boundaries remain in the
  adapter.
  """

  alias ElixirDB.Domain.Revision
  alias ElixirDB.JSON.Canonical
  alias ElixirDB.Revisions.Winner
  alias ElixirDB.Storage.SQLite.Connection

  @doc false
  def get(adapter, request), do: ElixirDB.Storage.SQLite.Adapter.get_document(adapter, request)

  def put(adapter, request),
    do:
      ElixirDB.Storage.SQLite.Adapter.apply_local_mutation(
        adapter,
        Map.put(request, :operation, :put)
      )

  def delete(adapter, request),
    do:
      ElixirDB.Storage.SQLite.Adapter.apply_local_mutation(
        adapter,
        Map.put(request, :operation, :delete)
      )

  @doc """
  Loads one document row by logical document id, or `nil` when absent.
  """
  @spec find(Connection.handle(), binary() | nil) ::
          {:ok, nil | map()} | {:error, ElixirDB.Error.t()}
  def find(_conn, nil), do: {:error, ElixirDB.Error.invalid_request("document_id is required")}

  def find(conn, document_id) do
    case Connection.query(
           conn,
           "SELECT doc_key, document_id, winning_revision, winning_body_json, winning_deleted, update_sequence FROM documents WHERE document_id = ?",
           [document_id]
         ) do
      {:ok, [[key, id, winning, body, deleted, sequence]]} ->
        {:ok,
         %{
           doc_key: key,
           document_id: id,
           winning_revision: winning,
           winning_body_json: body,
           winning_deleted: deleted == 1,
           update_sequence: sequence
         }}

      {:ok, []} ->
        {:ok, nil}

      {:error, reason} ->
        {:error, normalize_error(reason)}
    end
  end

  @doc """
  Inserts a placeholder document row and returns its `doc_key`.
  """
  @spec insert(Connection.handle(), binary()) :: {:ok, integer()} | {:error, ElixirDB.Error.t()}
  def insert(conn, id) do
    case Connection.execute(
           conn,
           "INSERT INTO documents(document_id, winning_revision, winning_body_json, winning_deleted, update_sequence) VALUES (?, NULL, NULL, 1, 0)",
           [id]
         ) do
      :ok ->
        case Connection.query(conn, "SELECT doc_key FROM documents WHERE document_id = ?", [id]) do
          {:ok, [[key]]} -> {:ok, key}
          {:error, reason} -> {:error, normalize_error(reason)}
        end

      {:error, reason} ->
        {:error, normalize_error(reason)}
    end
  end

  @doc """
  Materializes the winning revision onto the document row.
  """
  @spec update(Connection.handle(), integer(), Revision.t(), integer()) ::
          :ok | {:error, term()}
  def update(conn, doc_key, %Revision{} = winner, sequence) do
    body = if winner.deleted, do: nil, else: Canonical.encode!(winner.body)

    Connection.execute(
      conn,
      "UPDATE documents SET winning_revision = ?, winning_body_json = ?, winning_deleted = ?, update_sequence = ? WHERE doc_key = ?",
      [winner.revision_id, body, if(winner.deleted, do: 1, else: 0), sequence, doc_key]
    )
  end

  @doc """
  Shapes a document + revision (+ optional conflict leaves) into the adapter result map.
  """
  @spec to_result(map(), Revision.t(), [Revision.t()]) :: map()
  def to_result(doc, %Revision{} = revision, leaves) do
    result = %{
      id: doc.document_id,
      revision: revision.revision_id,
      deleted: revision.deleted,
      body: revision.body,
      sequence: doc.update_sequence
    }

    if leaves == [],
      do: result,
      else: Map.put(result, :conflicts, Winner.conflicts(leaves, revision))
  end

  # Pass already-typed domain errors through unchanged; only wrap raw driver reasons.
  defp normalize_error(%ElixirDB.Error{} = error), do: error

  defp normalize_error(reason),
    do: ElixirDB.Error.internal_error("SQLite operation failed", %{cause: inspect(reason)})
end
