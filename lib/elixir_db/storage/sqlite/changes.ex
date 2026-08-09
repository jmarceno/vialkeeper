defmodule ElixirDB.Storage.SQLite.Changes do
  @moduledoc """
  Change-feed SQL helpers for the Version 1 SQLite adapter.

  Owns sequence allocation, change-row insertion, and row decoding. Public
  `read/2` still routes through the adapter so mutation and transaction
  orchestration remain centralized until further Track A extraction.
  """

  alias ElixirDB.JSON.StrictCache
  alias ElixirDB.Storage.SQLite.Adapter
  alias ElixirDB.Storage.SQLite.Connection

  @leaf_json_cache_limit 256
  @doc false
  def read(adapter, request), do: Adapter.read_changes(adapter, request)

  @doc """
  Allocates the next monotonic change sequence for the open connection.
  """
  @spec allocate_sequence(Connection.handle()) :: {:ok, integer()} | {:error, ElixirDB.Error.t()}
  def allocate_sequence(conn) do
    case allocate_sequences(conn, 1) do
      {:ok, [sequence]} -> {:ok, sequence}
      {:error, _} = error -> error
    end
  end

  @doc "Allocates one contiguous sequence range for a bulk mutation."
  @spec allocate_sequences(Connection.handle(), non_neg_integer()) ::
          {:ok, [integer()]} | {:error, ElixirDB.Error.t()}
  def allocate_sequences(_conn, 0), do: {:ok, []}

  def allocate_sequences(conn, count) when is_integer(count) and count > 0 do
    with :ok <-
           Connection.execute(
             conn,
             "UPDATE db_meta SET current_sequence = current_sequence + ? WHERE id = 1",
             [count]
           ),
         {:ok, [[sequence]]} <-
           Connection.query(conn, "SELECT current_sequence FROM db_meta WHERE id = 1") do
      {:ok, Enum.to_list((sequence - count + 1)..sequence)}
    else
      {:error, reason} -> {:error, normalize_error(reason)}
    end
  end

  @doc """
  Inserts one change-feed row for an affected document.
  """
  @spec insert(
          Connection.handle(),
          integer(),
          integer(),
          binary(),
          ElixirDB.Domain.Revision.t(),
          binary(),
          binary()
        ) :: :ok | {:error, term()}
  def insert(conn, sequence, doc_key, document_id, winner, leaf_json, origin) do
    Connection.execute(
      conn,
      "INSERT INTO changes(sequence, doc_key, document_id, winning_revision, winning_deleted, leaf_set_json, origin) VALUES (?, ?, ?, ?, ?, ?, ?)",
      [
        sequence,
        doc_key,
        document_id,
        winner.revision_id,
        if(winner.deleted, do: 1, else: 0),
        leaf_json,
        origin
      ]
    )
  end

  @doc """
  Loads change-feed rows after `since`, limited to `limit` rows.
  """
  @spec fetch_after(Connection.handle(), integer(), integer()) ::
          {:ok, [[term()]]} | {:error, ElixirDB.Error.t()}
  def fetch_after(conn, since, limit) do
    case Connection.query(
           conn,
           "SELECT sequence, document_id, winning_revision, winning_deleted, leaf_set_json, origin FROM changes WHERE sequence > ? ORDER BY sequence LIMIT ?",
           [since, limit]
         ) do
      {:ok, rows} -> {:ok, rows}
      {:error, reason} -> {:error, normalize_error(reason)}
    end
  end

  @doc "Loads a changes page and derives `has_more` from one ordered query."
  @spec fetch_page(Connection.handle(), integer(), integer()) ::
          {:ok, {[[term()]], boolean()}} | {:error, ElixirDB.Error.t()}
  def fetch_page(conn, since, limit) do
    case Connection.query(
           conn,
           "SELECT sequence, document_id, winning_revision, winning_deleted, leaf_set_json, origin FROM changes WHERE sequence > ? ORDER BY sequence LIMIT ?",
           [since, limit + 1]
         ) do
      {:ok, rows} ->
        {page, extra} = Enum.split(rows, limit)
        {:ok, {page, extra != []}}

      {:error, reason} ->
        {:error, normalize_error(reason)}
    end
  end

  @doc """
  Returns whether any change-feed row exists after `sequence`.
  """
  @spec exists_after?(Connection.handle(), integer()) ::
          {:ok, boolean()} | {:error, ElixirDB.Error.t()}
  def exists_after?(conn, sequence) do
    case Connection.query(conn, "SELECT EXISTS(SELECT 1 FROM changes WHERE sequence > ?)", [
           sequence
         ]) do
      {:ok, [[has_more]]} -> {:ok, has_more == 1}
      {:error, reason} -> {:error, normalize_error(reason)}
    end
  end

  @doc """
  Returns whether any local-origin change-feed row exists.

  Used by replication safe-report probing; storage errors surface as `{:error, _}`
  so callers can treat uncertainty conservatively.
  """
  @spec has_local_origin_changes?(Connection.handle()) ::
          {:ok, boolean()} | {:error, ElixirDB.Error.t()}
  def has_local_origin_changes?(conn), do: has_local_origin_changes?(conn, nil)

  @spec has_local_origin_changes?(Connection.handle(), binary() | nil) ::
          {:ok, boolean()} | {:error, ElixirDB.Error.t()}
  def has_local_origin_changes?(conn, peer_database_uuid) do
    alias ElixirDB.Storage.SQLite.RetentionRecords
    RetentionRecords.pending_local_causal?(conn, peer_database_uuid)
  end

  @doc """
  Decodes ordered change-feed SQL rows into protocol maps.
  """
  @spec decode_rows([[term()]]) :: {:ok, [map()]} | {:error, ElixirDB.Error.t()}
  def decode_rows(rows) do
    max_depth = ElixirDB.Config.host_limits()[:max_json_nesting_depth] || 100
    decode_rows(rows, [], max_depth)
  end

  defp decode_rows([], acc, _max_depth), do: {:ok, :lists.reverse(acc)}

  defp decode_rows(
         [[sequence, document_id, winning, deleted, leaf_json, origin] | rows],
         acc,
         max_depth
       ) do
    case StrictCache.decode_with_cache(
           leaf_json,
           max_depth,
           :changes_leaf_json,
           @leaf_json_cache_limit
         ) do
      {:ok, leaves} ->
        decode_rows(
          rows,
          [change_entry(sequence, document_id, winning, deleted, leaves, origin) | acc],
          max_depth
        )

      {:error, error} ->
        {:error, error}
    end
  end

  defp change_entry(sequence, document_id, winning, deleted, leaves, origin),
    do: %{
      sequence: sequence,
      document_id: document_id,
      winning_revision: winning,
      deleted: deleted == 1,
      leaf_revisions: leaves,
      origin: origin
    }

  defp normalize_error(reason),
    do: ElixirDB.Error.internal_error("SQLite operation failed", %{cause: inspect(reason)})
end
