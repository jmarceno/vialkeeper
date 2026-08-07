defmodule ElixirDB.Storage.SQLite.Integrity do
  @moduledoc """
  Logical and SQLite integrity checks for a Version 1 database file.

  Owns pragma checks, required-table presence, and revision/document/change
  consistency validators. Index physical probes still call
  `ElixirDB.Storage.SQLite.Indexes.integrity/2`; further document/revision SQL
  centralization remains in the adapter.
  """

  alias ElixirDB.JSON.{Canonical, StrictDecoder}
  alias ElixirDB.MapAccess
  alias ElixirDB.Revisions.Id
  alias ElixirDB.Storage.SQLite.Adapter
  alias ElixirDB.Storage.SQLite.{Connection, Indexes}
  @doc false
  def check(adapter), do: Adapter.integrity_check(adapter, %{})

  @doc """
  Runs structural and logical integrity validators for an open connection.
  """
  @spec run(Connection.handle(), [map()]) :: :ok | {:error, ElixirDB.Error.t()}
  def run(conn, indexes) when is_list(indexes) do
    with {:ok, [["ok"]]} <- Connection.pragma(conn, "integrity_check"),
         {:ok, []} <- Connection.pragma(conn, "foreign_key_check"),
         :ok <- required_tables_present(conn),
         :ok <- validate_revision_rows(conn),
         :ok <- validate_document_rows(conn),
         :ok <- validate_change_rows(conn),
         :ok <- validate_index_rows(conn, indexes) do
      :ok
    else
      {:ok, rows} ->
        {:error,
         ElixirDB.Error.integrity_violation("SQLite integrity check failed", %{results: rows})}

      {:error, %ElixirDB.Error{} = error} ->
        {:error, error}

      {:error, reason} ->
        {:error, normalize_error(reason)}
    end
  end

  defp required_tables_present(conn) do
    required =
      ~w(db_meta documents revisions changes local_records replication_jobs index_definitions)

    with {:ok, rows} <-
           Connection.query(
             conn,
             "SELECT name FROM sqlite_master WHERE type = 'table' AND name IN ('db_meta', 'documents', 'revisions', 'changes', 'local_records', 'replication_jobs', 'index_definitions')"
           ) do
      present = MapSet.new(rows, &List.first/1)

      if Enum.all?(required, &MapSet.member?(present, &1)),
        do: :ok,
        else: {:error, ElixirDB.Error.integrity_violation("required SQLite tables are missing")}
    end
  end

  defp validate_revision_rows(conn) do
    with {:ok, rows} <-
           Connection.query(
             conn,
             "SELECT d.document_id, r.revision_id, r.generation, r.parent_revision, r.deleted, r.body_json, r.is_leaf FROM revisions AS r JOIN documents AS d ON d.doc_key = r.doc_key ORDER BY d.document_id, r.revision_id"
           ) do
      Enum.reduce_while(rows, :ok, fn [
                                        document_id,
                                        revision_id,
                                        generation,
                                        parent,
                                        deleted,
                                        body_json,
                                        leaf
                                      ],
                                      :ok ->
        validate_revision_row(
          conn,
          document_id,
          revision_id,
          generation,
          parent,
          deleted,
          body_json,
          leaf
        )
      end)
    end
  end

  defp validate_revision_row(
         conn,
         document_id,
         revision_id,
         generation,
         parent,
         deleted,
         body_json,
         leaf
       ) do
    body = revision_body(deleted, body_json)

    with {:ok, calculated} <- Id.calculate(document_id, parent, deleted == 1, body),
         true <- calculated == revision_id,
         {:ok, expected_generation} <- Id.generation(revision_id),
         true <- expected_generation == generation,
         :ok <- validate_parent_row(conn, document_id, revision_id, parent),
         :ok <- validate_leaf_row(conn, revision_id, leaf) do
      {:cont, :ok}
    else
      false ->
        {:halt,
         {:error,
          ElixirDB.Error.integrity_violation("revision identity or generation is invalid", %{
            revision: revision_id
          })}}

      {:error, error} ->
        {:halt, {:error, error}}
    end
  end

  defp revision_body(1, _body_json), do: nil
  defp revision_body(_deleted, body_json), do: StrictDecoder.decode_or_nil(body_json)

  defp validate_parent_row(_conn, _document_id, _revision_id, nil), do: :ok

  defp validate_parent_row(conn, document_id, revision_id, parent) do
    case Connection.query(
           conn,
           "SELECT 1 FROM revisions AS r JOIN documents AS d ON d.doc_key = r.doc_key WHERE d.document_id = ? AND r.revision_id = ?",
           [document_id, parent]
         ) do
      {:ok, [[1]]} ->
        :ok

      {:ok, []} ->
        {:error,
         ElixirDB.Error.integrity_violation("revision has a dangling parent", %{
           revision: revision_id,
           parent_revision: parent
         })}

      {:error, reason} ->
        {:error, normalize_error(reason)}
    end
  end

  defp validate_leaf_row(conn, revision_id, leaf) do
    with {:ok, [[children]]} <-
           Connection.query(
             conn,
             "SELECT EXISTS(SELECT 1 FROM revisions WHERE parent_revision = ?)",
             [revision_id]
           ),
         true <- leaf == 1 == (children == 0) do
      :ok
    else
      false ->
        {:error,
         ElixirDB.Error.integrity_violation("revision leaf marker is stale", %{
           revision: revision_id
         })}

      {:error, reason} ->
        {:error, normalize_error(reason)}
    end
  end

  defp validate_document_rows(conn) do
    with {:ok, rows} <-
           Connection.query(
             conn,
             "SELECT doc_key, document_id, winning_revision, winning_body_json, winning_deleted, update_sequence FROM documents"
           ) do
      Enum.reduce_while(rows, :ok, fn [doc_key, document_id, winning, body_json, deleted, sequence],
                                      :ok ->
        validate_document_row(conn, doc_key, document_id, winning, body_json, deleted, sequence)
      end)
    end
  end

  defp validate_document_row(_conn, _doc_key, _document_id, nil, nil, 1, 0), do: {:cont, :ok}

  defp validate_document_row(_conn, _doc_key, document_id, nil, _body_json, _deleted, _sequence),
    do:
      {:halt,
       {:error,
        ElixirDB.Error.integrity_violation("document has no winning revision", %{
          document_id: document_id
        })}}

  defp validate_document_row(conn, doc_key, document_id, winning, body_json, deleted, _sequence) do
    case Connection.query(
           conn,
           "SELECT deleted, body_json FROM revisions WHERE doc_key = ? AND revision_id = ?",
           [doc_key, winning]
         ) do
      {:ok, [[revision_deleted, revision_body]]} ->
        validate_materialized_winner(
          body_matches?(revision_deleted, revision_body, body_json, deleted),
          document_id
        )

      {:ok, []} ->
        {:halt,
         {:error,
          ElixirDB.Error.integrity_violation("document winner is missing", %{
            document_id: document_id
          })}}

      {:error, reason} ->
        {:halt, {:error, normalize_error(reason)}}
    end
  end

  defp body_matches?(1, _revision_body, body_json, deleted), do: is_nil(body_json) and deleted == 1

  defp body_matches?(_revision_deleted, revision_body, body_json, deleted) do
    is_binary(body_json) and deleted == 0 and
      Canonical.encode!(StrictDecoder.decode_or_nil(revision_body)) == body_json
  end

  defp validate_materialized_winner(true, _document_id), do: {:cont, :ok}

  defp validate_materialized_winner(false, document_id),
    do:
      {:halt,
       {:error,
        ElixirDB.Error.integrity_violation("materialized winner is inconsistent", %{
          document_id: document_id
        })}}

  defp validate_change_rows(conn) do
    with {:ok, rows} <-
           Connection.query(
             conn,
             "SELECT document_id, winning_revision, leaf_set_json FROM changes ORDER BY sequence"
           ) do
      Enum.reduce_while(rows, :ok, fn [document_id, winning, leaf_json], :ok ->
        validate_change_row(conn, document_id, winning, leaf_json)
      end)
    end
  end

  defp validate_change_row(conn, document_id, winning, leaf_json) do
    with {:ok, leaves} <- StrictDecoder.decode(leaf_json),
         true <- is_list(leaves),
         :ok <- validate_change_leaves(conn, document_id, winning, leaves) do
      {:cont, :ok}
    else
      false ->
        {:halt,
         {:error,
          ElixirDB.Error.integrity_violation("change leaf set is invalid", %{
            document_id: document_id
          })}}

      {:error, error} ->
        {:halt, {:error, error}}
    end
  end

  defp validate_change_leaves(conn, document_id, winning, leaves) do
    Enum.reduce_while(leaves, :ok, fn leaf, :ok ->
      validate_change_leaf(conn, document_id, leaf)
    end)
    |> case do
      :ok ->
        case Connection.query(
               conn,
               "SELECT 1 FROM revisions AS r JOIN documents AS d ON d.doc_key = r.doc_key WHERE d.document_id = ? AND r.revision_id = ?",
               [document_id, winning]
             ) do
          {:ok, [[1]]} ->
            :ok

          {:ok, []} ->
            {:error,
             ElixirDB.Error.integrity_violation("change winner is missing", %{revision: winning})}

          {:error, reason} ->
            {:error, normalize_error(reason)}
        end

      error ->
        error
    end
  end

  defp validate_change_leaf(_conn, document_id, leaf) when not is_map(leaf),
    do:
      {:halt,
       {:error,
        ElixirDB.Error.integrity_violation("change leaf entry is not an object", %{
          document_id: document_id
        })}}

  defp validate_change_leaf(conn, document_id, leaf) do
    revision = MapAccess.get(leaf, :revision)

    if is_binary(revision) and revision != "",
      do: validate_change_leaf_revision(conn, document_id, leaf, revision),
      else:
        {:halt,
         {:error,
          ElixirDB.Error.integrity_violation("change leaf revision is invalid", %{
            document_id: document_id
          })}}
  end

  defp validate_change_leaf_revision(conn, document_id, leaf, revision) do
    case Connection.query(
           conn,
           "SELECT r.revision_id, r.deleted FROM revisions AS r JOIN documents AS d ON d.doc_key = r.doc_key WHERE d.document_id = ? AND r.revision_id = ?",
           [document_id, revision]
         ) do
      {:ok, [[^revision, deleted]]} ->
        supplied_deleted = MapAccess.get(leaf, :deleted)
        validate_change_leaf_marker(supplied_deleted == (deleted == 1), supplied_deleted, revision)

      {:ok, []} ->
        {:halt,
         {:error,
          ElixirDB.Error.integrity_violation("change references a missing revision", %{
            revision: revision
          })}}

      {:error, reason} ->
        {:halt, {:error, normalize_error(reason)}}
    end
  end

  defp validate_change_leaf_marker(true, supplied_deleted, _revision)
       when is_boolean(supplied_deleted),
       do: {:cont, :ok}

  defp validate_change_leaf_marker(_matches, _supplied_deleted, revision),
    do:
      {:halt,
       {:error,
        ElixirDB.Error.integrity_violation("change leaf deletion marker is stale", %{
          revision: revision
        })}}

  defp validate_index_rows(conn, indexes) do
    Enum.reduce_while(indexes, :ok, fn index, :ok ->
      metadata = Map.merge(index, index["_metadata"] || %{})

      case Indexes.integrity(conn, metadata) do
        {:ok, _} -> {:cont, :ok}
        {:error, error} -> {:halt, {:error, error}}
      end
    end)
  end

  defp normalize_error(%ElixirDB.Error{} = error), do: error

  defp normalize_error(reason),
    do: ElixirDB.Error.internal_error("SQLite operation failed", %{cause: inspect(reason)})
end
