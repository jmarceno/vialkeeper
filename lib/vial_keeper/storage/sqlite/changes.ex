defmodule VialKeeper.Storage.SQLite.Changes do
  @moduledoc """
  Change-feed SQL helpers for the Version 1 SQLite adapter.

  Owns sequence allocation, change-row insertion, and row decoding. Public
  `read/2` still routes through the adapter so mutation and transaction
  orchestration remain centralized until further Track A extraction.
  """

  alias VialKeeper.Domain.Change
  alias VialKeeper.JSON.StrictDecoder
  alias VialKeeper.Storage.SQLite.Adapter
  alias VialKeeper.Storage.SQLite.{Connection, TermBlob}

  @leaf_term_cache_limit 256
  @doc false
  def read(adapter, request), do: Adapter.read_changes(adapter, request)

  @doc """
  Allocates the next monotonic change sequence for the open connection.
  """
  @spec allocate_sequence(Connection.handle()) :: {:ok, integer()} | {:error, VialKeeper.Error.t()}
  def allocate_sequence(conn) do
    case allocate_sequences(conn, 1) do
      {:ok, [sequence]} -> {:ok, sequence}
      {:error, _} = error -> error
    end
  end

  @doc "Allocates one contiguous sequence range for a bulk mutation."
  @spec allocate_sequences(Connection.handle(), non_neg_integer()) ::
          {:ok, [integer()]} | {:error, VialKeeper.Error.t()}
  def allocate_sequences(_conn, 0), do: {:ok, []}

  def allocate_sequences(conn, count) when is_integer(count) and count > 0 do
    case Connection.query(
           conn,
           "UPDATE db_meta SET current_sequence = current_sequence + ? WHERE id = 1 RETURNING current_sequence",
           [count]
         ) do
      {:ok, [[sequence]]} ->
        {:ok, Enum.to_list((sequence - count + 1)..sequence)}

      {:error, reason} ->
        {:error, normalize_error(reason)}
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
          VialKeeper.Domain.Revision.t(),
          binary(),
          binary()
        ) :: :ok | {:error, term()}
  def insert(conn, sequence, doc_key, document_id, winner, leaf_json, origin) do
    with {:ok, leaves} <- StrictDecoder.decode(leaf_json),
         {:ok, leaf_term} <- TermBlob.encode(leaves, leaf_json) do
      Connection.execute(
        conn,
        "INSERT INTO changes(sequence, doc_key, document_id, winning_revision, winning_deleted, leaf_set_json, leaf_set_term, origin) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        [
          sequence,
          doc_key,
          document_id,
          winner.revision_id,
          if(winner.deleted, do: 1, else: 0),
          leaf_json,
          TermBlob.bind(leaf_term),
          origin
        ]
      )
    end
  end

  @doc "Inserts an ordered batch of change-feed rows."
  @spec insert_many(Connection.handle(), [
          {integer(), integer() | nil, binary(), VialKeeper.Domain.Revision.t(), binary(), binary()}
        ]) :: :ok | {:error, term()}
  def insert_many(_conn, []), do: :ok

  def insert_many(conn, entries) when is_list(entries) do
    entries
    |> Enum.chunk_every(100)
    |> Enum.reduce_while(:ok, fn chunk, :ok ->
      with {:ok, rows} <- encode_change_rows(chunk),
           :ok <- insert_rows(conn, rows) do
        {:cont, :ok}
      else
        {:error, _} = error -> {:halt, error}
      end
    end)
  end

  @doc """
  Loads change-feed rows after `since`, limited to `limit` rows.
  """
  @spec fetch_after(Connection.handle(), integer(), integer()) ::
          {:ok, [[term()]]} | {:error, VialKeeper.Error.t()}
  def fetch_after(conn, since, limit) do
    case Connection.query(
           conn,
           "SELECT sequence, document_id, winning_revision, winning_deleted, leaf_set_term, origin FROM changes WHERE sequence > ? ORDER BY sequence LIMIT ?",
           [since, limit]
         ) do
      {:ok, rows} -> {:ok, rows}
      {:error, reason} -> {:error, normalize_error(reason)}
    end
  end

  @doc "Loads a changes page and derives `has_more` from one ordered query."
  @spec fetch_page(Connection.handle(), integer(), integer()) ::
          {:ok, {[[term()]], boolean()}} | {:error, VialKeeper.Error.t()}
  def fetch_page(conn, since, limit) do
    case Connection.query(
           conn,
           "SELECT sequence, document_id, winning_revision, winning_deleted, leaf_set_term, origin FROM changes WHERE sequence > ? ORDER BY sequence LIMIT ?",
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
          {:ok, boolean()} | {:error, VialKeeper.Error.t()}
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
          {:ok, boolean()} | {:error, VialKeeper.Error.t()}
  def has_local_origin_changes?(conn), do: has_local_origin_changes?(conn, nil)

  @spec has_local_origin_changes?(Connection.handle(), binary() | nil) ::
          {:ok, boolean()} | {:error, VialKeeper.Error.t()}
  def has_local_origin_changes?(conn, peer_database_uuid) do
    alias VialKeeper.Storage.SQLite.RetentionRecords
    RetentionRecords.pending_local_causal?(conn, peer_database_uuid)
  end

  @doc """
  Decodes ordered change-feed SQL rows into protocol maps.
  """
  @spec decode_rows([[term()]]) :: {:ok, [map()]} | {:error, VialKeeper.Error.t()}
  def decode_rows(rows) do
    max_depth = VialKeeper.Config.host_limits()[:max_json_nesting_depth] || 100
    decode_rows(rows, [], max_depth)
  end

  defp decode_rows([], acc, _max_depth), do: {:ok, :lists.reverse(acc)}

  defp decode_rows(
         [[sequence, document_id, winning, deleted, leaf_term, origin] | rows],
         acc,
         max_depth
       ) do
    case TermBlob.decode_trusted_with_cache(
           leaf_term,
           :changes_leaf_term,
           max_depth,
           @leaf_term_cache_limit
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
    do:
      Change.public(
        sequence,
        document_id,
        winning,
        deleted == 1,
        leaves,
        origin
      )

  defp encode_change_rows(entries) do
    Enum.reduce_while(entries, {:ok, []}, fn
      {sequence, doc_key, document_id, winner, leaf_json, origin}, {:ok, rows} ->
        with {:ok, leaves} <- StrictDecoder.decode(leaf_json),
             {:ok, leaf_term} <- TermBlob.encode(leaves, leaf_json) do
          {:cont,
           {:ok,
            [
              [
                sequence,
                doc_key,
                document_id,
                winner.revision_id,
                if(winner.deleted, do: 1, else: 0),
                leaf_json,
                TermBlob.bind(leaf_term),
                origin
              ]
              | rows
            ]}}
        else
          {:error, _} = error -> {:halt, error}
        end
    end)
    |> reverse_rows()
  end

  defp reverse_rows({:ok, rows}), do: {:ok, Enum.reverse(rows)}
  defp reverse_rows(error), do: error

  defp insert_rows(_conn, []), do: :ok

  defp insert_rows(conn, rows) do
    placeholders = Enum.map_join(rows, ",", fn _row -> "(?, ?, ?, ?, ?, ?, ?, ?)" end)

    Connection.execute(
      conn,
      "INSERT INTO changes(sequence, doc_key, document_id, winning_revision, winning_deleted, leaf_set_json, leaf_set_term, origin) VALUES " <>
        placeholders,
      List.flatten(rows)
    )
  end

  defp normalize_error(reason),
    do: VialKeeper.Error.internal_error("SQLite operation failed", %{cause: inspect(reason)})
end
