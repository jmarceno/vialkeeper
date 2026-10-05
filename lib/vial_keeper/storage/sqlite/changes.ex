defmodule VialKeeper.Storage.SQLite.Changes do
  @moduledoc """
  Change-feed SQL helpers for the Version 1 SQLite adapter.

  Owns the sequence reservation row, change-row insertion, and row decoding. Public
  `read/2` still routes through the adapter so mutation and transaction
  orchestration remain centralized until further Track A extraction.
  """

  alias VialKeeper.Domain.Change
  alias VialKeeper.JSON.StrictDecoder
  alias VialKeeper.Revisions.Compare
  alias VialKeeper.Storage.SQLite.Adapter
  alias VialKeeper.Storage.SQLite.{Connection, TermBlob}

  @leaf_term_cache_limit 256
  @high_water_key :vial_keeper_sqlite_sequence_high_water
  @doc false
  def read(adapter, request), do: Adapter.read_changes(adapter, request)

  @doc """
  Returns `db_meta.sequence_reserved_through`, the highest sequence that may
  be in use.

  The value is cached per connection in the calling process: it changes only
  through `persist_reservation/2`, which keeps the cache current, and a
  rolled-back write forgets it (`forget_high_water/1`).
  """
  @spec high_water(Connection.handle()) ::
          {:ok, non_neg_integer()} | {:error, VialKeeper.Error.t()}
  def high_water(conn) do
    case Process.get({@high_water_key, conn}) do
      sequence when is_integer(sequence) -> {:ok, sequence}
      nil -> load_high_water(conn)
    end
  end

  @doc "Caches a reserved-through value just read from `db_meta` for `conn`."
  @spec remember_high_water(Connection.handle(), non_neg_integer()) :: :ok
  def remember_high_water(conn, sequence) when is_integer(sequence) and sequence >= 0 do
    _ = Process.put({@high_water_key, conn}, sequence)
    :ok
  end

  @doc "Forgets the cached reserved-through value for `conn`."
  @spec forget_high_water(Connection.handle()) :: :ok
  def forget_high_water(conn) do
    _ = Process.delete({@high_water_key, conn})
    :ok
  end

  defp load_high_water(conn) do
    case Connection.query(conn, "SELECT sequence_reserved_through FROM db_meta WHERE id = 1") do
      {:ok, [[sequence]]} when is_integer(sequence) and sequence >= 0 ->
        :ok = remember_high_water(conn, sequence)
        {:ok, sequence}

      {:ok, _} ->
        {:error, VialKeeper.Error.integrity_violation("sequence reservation is invalid")}

      {:error, reason} ->
        {:error, normalize_error(reason)}
    end
  end

  @doc """
  Durably raises `db_meta.sequence_reserved_through` to at least `through`.
  This is the only statement that writes the column.

  It runs in a transaction of its own, or joins this process's write
  transaction on `conn` (storage used without a sequence ledger reserves
  inside the write that uses the numbers).
  """
  @spec persist_reservation(Connection.handle(), non_neg_integer()) ::
          :ok | {:error, VialKeeper.Error.t()}
  def persist_reservation(conn, through) when is_integer(through) and through >= 0 do
    if Connection.in_write_transaction?(conn),
      do: raise_reservation(conn, through),
      else: persist_reservation_transaction(conn, through)
  end

  defp persist_reservation_transaction(conn, through) do
    with :ok <- control(conn, "BEGIN IMMEDIATE") do
      case raise_reservation(conn, through) do
        :ok ->
          commit_reservation(conn)

        {:error, _} = error ->
          _ = Connection.exec(conn, "ROLLBACK")
          error
      end
    end
  end

  defp raise_reservation(conn, through) do
    case Connection.point_query(
           conn,
           "UPDATE db_meta SET sequence_reserved_through = max(sequence_reserved_through, ?) WHERE id = 1",
           [through]
         ) do
      {:ok, _no_rows} ->
        remember_raised(conn, through)

      {:error, reason} ->
        _ = forget_high_water(conn)
        {:error, normalize_error(reason)}
    end
  end

  defp remember_raised(conn, through) do
    case Process.get({@high_water_key, conn}) do
      cached when is_integer(cached) -> remember_high_water(conn, max(cached, through))
      nil -> :ok
    end
  end

  defp commit_reservation(conn) do
    case control(conn, "COMMIT") do
      :ok ->
        :ok

      {:error, _} = error ->
        _ = Connection.exec(conn, "ROLLBACK")
        error
    end
  end

  defp control(conn, sql) do
    case Connection.exec(conn, sql) do
      :ok -> :ok
      {:error, reason} -> {:error, normalize_error(reason)}
    end
  end

  @doc """
  Inserts one change-feed row for an affected document.

  `leaves` carries the decoded leaf-set term when the caller already holds it;
  the row BLOB is then derived without re-decoding `leaf_json`.
  """
  @spec insert(
          Connection.handle(),
          integer(),
          integer(),
          binary(),
          VialKeeper.Domain.Revision.t(),
          binary(),
          binary(),
          [VialKeeper.Domain.Revision.t()] | nil
        ) :: :ok | {:error, term()}
  def insert(conn, sequence, doc_key, document_id, winner, leaf_json, origin, leaves \\ nil) do
    with {:ok, leaf_term} <- leaf_term(leaves, leaf_json) do
      Connection.point_execute(
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
          {integer(), integer() | nil, binary(), VialKeeper.Domain.Revision.t(), binary(), binary(),
           [VialKeeper.Domain.Revision.t()] | nil}
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
  Loads the change-feed rows with `since < sequence <= through`, at most
  `limit`, and derives `has_more` from the same ordered query.
  """
  @spec fetch_page(Connection.handle(), integer(), integer(), integer()) ::
          {:ok, {[[term()]], boolean()}} | {:error, VialKeeper.Error.t()}
  def fetch_page(conn, since, through, limit) do
    case Connection.query(
           conn,
           "SELECT sequence, document_id, winning_revision, winning_deleted, leaf_set_term, origin FROM changes WHERE sequence > ? AND sequence <= ? ORDER BY sequence LIMIT ?",
           [since, through, limit + 1]
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
      {sequence, doc_key, document_id, winner, leaf_json, origin, leaves}, {:ok, rows} ->
        case leaf_term(leaves, leaf_json) do
          {:ok, leaf_term} ->
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

          {:error, _} = error ->
            {:halt, error}
        end
    end)
    |> reverse_rows()
  end

  defp leaf_term(nil, leaf_json) do
    with {:ok, leaves} <- StrictDecoder.decode(leaf_json) do
      TermBlob.encode(leaves, leaf_json)
    end
  end

  defp leaf_term(leaves, leaf_json) when is_list(leaves),
    do: TermBlob.encode(Compare.leaf_maps(leaves), leaf_json)

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
