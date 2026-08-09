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
  alias ElixirDB.Storage.SQLite.Adapter
  alias ElixirDB.Storage.SQLite.Connection
  alias ElixirDB.Storage.SQLite.TermBlob
  alias Exqlite.Sqlite3
  @doc false
  def get(adapter, request), do: Adapter.get_document(adapter, request)

  def put(adapter, request),
    do:
      Adapter.apply_local_mutation(
        adapter,
        Map.put(request, :operation, :put)
      )

  def delete(adapter, request),
    do:
      Adapter.apply_local_mutation(
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
        {:ok, document_from_row([key, id, winning, body, deleted, sequence])}

      {:ok, []} ->
        {:ok, nil}

      {:error, reason} ->
        {:error, normalize_error(reason)}
    end
  end

  @doc "Loads a set of document rows in one query for bulk mutation preparation."
  @spec find_many(Connection.handle(), [binary()]) ::
          {:ok, %{optional(binary()) => map() | nil}} | {:error, ElixirDB.Error.t()}
  def find_many(_conn, []), do: {:ok, %{}}

  def find_many(conn, document_ids) when is_list(document_ids) do
    placeholders = Enum.map_join(document_ids, ",", fn _id -> "?" end)

    case Connection.query(
           conn,
           "SELECT doc_key, document_id, winning_revision, winning_body_json, winning_deleted, update_sequence FROM documents WHERE document_id IN (" <>
             placeholders <> ")",
           document_ids
         ) do
      {:ok, rows} ->
        documents =
          Map.new(document_ids, &{&1, nil})
          |> Map.merge(
            Map.new(rows, fn row ->
              document = document_from_row(row)
              {document.document_id, document}
            end)
          )

        {:ok, documents}

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
        Sqlite3.last_insert_rowid(conn)

      {:error, reason} ->
        {:error, normalize_error(reason)}
    end
  end

  @doc """
  Materializes the winning revision onto the document row.
  """
  @spec update(Connection.handle(), integer(), Revision.t(), integer()) ::
          :ok | {:error, term()}
  def update(conn, doc_key, %Revision{} = winner, sequence),
    do: update(conn, doc_key, winner, sequence, nil)

  @spec update(Connection.handle(), integer(), Revision.t(), integer(), binary() | nil) ::
          :ok | {:error, term()}
  def update(conn, doc_key, %Revision{} = winner, sequence, body_json) do
    body = if winner.deleted, do: nil, else: body_json || Canonical.encode!(winner.body)

    with {:ok, body_term} <- materialized_body_term(winner, body) do
      Connection.execute(
        conn,
        "UPDATE documents SET winning_revision = ?, winning_body_json = ?, winning_body_term = ?, winning_deleted = ?, update_sequence = ? WHERE doc_key = ?",
        [
          winner.revision_id,
          body,
          TermBlob.bind(body_term),
          if(winner.deleted, do: 1, else: 0),
          sequence,
          doc_key
        ]
      )
    end
  end

  @doc "Marks a document whose complete history has been purged as an empty placeholder."
  @spec empty(Connection.handle(), integer()) :: :ok | {:error, term()}
  def empty(conn, doc_key) do
    Connection.execute(
      conn,
      "UPDATE documents SET winning_revision = NULL, winning_body_json = NULL, winning_body_term = NULL, winning_deleted = 1, update_sequence = 0 WHERE doc_key = ?",
      [doc_key]
    )
  end

  @doc """
  Lists document ids in stable order for paginated bootstrap pages.
  """
  @spec list_page(Connection.handle(), binary() | nil, pos_integer()) ::
          {:ok, {list(binary()), binary() | nil}} | {:error, ElixirDB.Error.t()}
  def list_page(conn, cursor, limit) when is_integer(limit) and limit > 0 do
    query =
      case cursor do
        nil ->
          {"SELECT document_id FROM documents ORDER BY document_id LIMIT ?", [limit + 1]}

        cursor when is_binary(cursor) ->
          {"SELECT document_id FROM documents WHERE document_id > ? ORDER BY document_id LIMIT ?",
           [cursor, limit + 1]}
      end

    case Connection.query(conn, elem(query, 0), elem(query, 1)) do
      {:ok, rows} ->
        ids = Enum.map(rows, fn [id] -> id end)
        page = Enum.take(ids, limit)

        next_cursor =
          case Enum.drop(ids, limit) do
            [next | _] -> next
            _ -> nil
          end

        {:ok, {page, next_cursor}}

      {:error, reason} ->
        {:error, normalize_error(reason)}
    end
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
      sequence: doc.update_sequence,
      attachments: revision.attachments || %{}
    }

    if leaves == [],
      do: result,
      else: Map.put(result, :conflicts, Winner.conflicts(leaves, revision))
  end

  defp normalize_error(reason),
    do: ElixirDB.Error.internal_error("SQLite operation failed", %{cause: inspect(reason)})

  defp materialized_body_term(%Revision{deleted: true}, _body), do: {:ok, nil}

  defp materialized_body_term(%Revision{body: body}, body_json) when is_binary(body_json),
    do: TermBlob.encode(body, body_json)

  defp document_from_row([key, id, winning, body, deleted, sequence]) do
    %{
      doc_key: key,
      document_id: id,
      winning_revision: winning,
      winning_body_json: body,
      winning_deleted: deleted == 1,
      update_sequence: sequence
    }
  end
end
