defmodule ElixirDB.Storage.SQLite.Documents do
  @moduledoc """
  Document-row SQL helpers for the Version 1 SQLite adapter.

  Owns find/insert/update against the `documents` table and winner materialization
  result shaping. Mutation orchestration and transaction boundaries remain in the
  adapter.
  """

  alias ElixirDB.Attachments.Manifest
  alias ElixirDB.Domain.Revision
  alias ElixirDB.JSON.{Canonical, StrictDecoder}
  alias ElixirDB.Storage.Results
  alias ElixirDB.Storage.SQLite.Adapter
  alias ElixirDB.Storage.SQLite.Connection
  alias ElixirDB.Storage.SQLite.TermBlob
  alias Exqlite.Sqlite3

  @winner_body_term_cache_limit 256

  @winner_sql """
  SELECT d.document_id, d.winning_revision, d.winning_body_json, d.winning_body_term,
         d.winning_deleted, d.update_sequence,
         a.attachment_name, a.blob_digest, a.logical_size, a.content_type
  FROM documents AS d
  LEFT JOIN revision_attachments AS a
    ON a.doc_key = d.doc_key AND a.revision_id = d.winning_revision
  WHERE d.document_id = ?
  """
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

  @doc """
  Loads the materialized winner and attachment manifest in one query.

  Used by the common get-document path that does not request a historical
  revision or conflict leaves. The documents row is the winner; the revisions
  table is not consulted.
  """
  @spec load_winner(Connection.handle(), binary() | nil) ::
          {:ok, nil | {:deleted, binary() | nil} | map()}
          | {:error, ElixirDB.Error.t()}
  def load_winner(_conn, nil),
    do: {:error, ElixirDB.Error.invalid_request("document_id is required")}

  def load_winner(conn, document_id) when is_binary(document_id) do
    case Connection.query(conn, @winner_sql, [document_id]) do
      {:ok, []} ->
        {:ok, nil}

      {:ok, rows} ->
        winner_from_rows(rows)

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

  @doc "Inserts a document row with its first winning revision materialized."
  @spec insert_with_winner(
          Connection.handle(),
          binary(),
          Revision.t(),
          non_neg_integer(),
          binary() | nil
        ) :: {:ok, integer()} | {:error, term()}
  def insert_with_winner(conn, id, %Revision{} = winner, sequence, body_json)
      when is_binary(id) and is_integer(sequence) and sequence >= 0 do
    body = if winner.deleted, do: nil, else: body_json || Canonical.encode!(winner.body)

    with {:ok, body_term} <- materialized_body_term(winner, body),
         :ok <-
           Connection.execute(
             conn,
             "INSERT INTO documents(document_id, winning_revision, winning_body_json, winning_body_term, winning_deleted, update_sequence) VALUES (?, ?, ?, ?, ?, ?)",
             [
               id,
               winner.revision_id,
               body,
               TermBlob.bind(body_term),
               if(winner.deleted, do: 1, else: 0),
               sequence
             ]
           ) do
      Sqlite3.last_insert_rowid(conn)
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
    Results.document_map(doc, revision, leaves)
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

  defp winner_from_rows(
         [
           [document_id, winning, body_json, body_term, deleted, sequence | _]
           | _
         ] = rows
       ) do
    cond do
      deleted == 1 ->
        {:ok, {:deleted, winning}}

      is_nil(winning) ->
        {:ok, nil}

      true ->
        with {:ok, attachments} <- winner_attachments(rows),
             {:ok, body} <- winner_body(body_json, body_term) do
          {:ok,
           %{
             id: document_id,
             revision: winning,
             deleted: false,
             body: body,
             sequence: sequence,
             attachments: attachments
           }}
        end
    end
  end

  defp winner_body(body_json, body_term) when is_binary(body_json) do
    case TermBlob.decode_with_cache(
           body_term,
           body_json,
           :winner_body_term,
           @winner_body_term_cache_limit
         ) do
      {:ok, value} ->
        {:ok, value}

      {:fallback, _reason} ->
        StrictDecoder.decode(body_json)
    end
  end

  defp winner_body(_body_json, _body_term),
    do: {:error, ElixirDB.Error.integrity_violation("winning document body is missing")}

  defp winner_attachments(rows) do
    rows
    |> Enum.flat_map(&attachment_from_winner_row/1)
    |> Map.new()
    |> Manifest.normalize()
  end

  defp attachment_from_winner_row([
         _id,
         _winning,
         _json,
         _term,
         _deleted,
         _sequence,
         name,
         digest,
         logical_size,
         content_type | _
       ])
       when not is_nil(name),
       do: [{name, Manifest.entry(digest, logical_size, content_type)}]

  defp attachment_from_winner_row(_row), do: []
end
