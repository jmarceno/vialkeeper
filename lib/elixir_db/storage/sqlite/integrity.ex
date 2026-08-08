defmodule ElixirDB.Storage.SQLite.Integrity do
  @moduledoc """
  Logical and SQLite integrity checks for a Version 1 database file.

  Owns pragma checks, required-table presence, and revision/document/change
  consistency validators. Index physical probes still call
  `ElixirDB.Storage.SQLite.Indexes.integrity/2`; further document/revision SQL
  centralization remains in the adapter.
  """

  alias ElixirDB.Domain.{BoundaryPage, Checkpoint, PeerPosition, RetentionBoundary}
  alias ElixirDB.JSON.{Canonical, StrictDecoder}
  alias ElixirDB.MapAccess
  alias ElixirDB.Revisions.Id
  alias ElixirDB.Storage.SQLite.Adapter
  alias ElixirDB.Storage.SQLite.{Connection, Indexes, Meta, RetentionRecords}
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
         {:ok, meta} <- Meta.load(conn),
         :ok <- validate_db_meta(meta),
         {:ok, boundaries} <-
           RetentionRecords.list_boundaries(conn, source_database_uuid: meta.database_uuid),
         :ok <- validate_boundary_records(boundaries, meta),
         {:ok, peers} <- RetentionRecords.list_peers(conn),
         :ok <- validate_peer_records(peers, meta),
         :ok <- validate_retention_maintenance(conn),
         :ok <- validate_revision_rows(conn, boundaries),
         :ok <- validate_document_rows(conn),
         :ok <- validate_change_rows(conn, meta.retention_floor_sequence),
         :ok <- validate_checkpoints(conn, meta),
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

  defp validate_revision_rows(conn, boundaries) do
    with {:ok, rows} <-
           Connection.query(
             conn,
             "SELECT d.document_id, r.revision_id, r.generation, r.parent_revision, r.history_id, r.digest, r.deleted, r.body_json, r.is_leaf FROM revisions AS r JOIN documents AS d ON d.doc_key = r.doc_key ORDER BY d.document_id, r.revision_id"
           ) do
      Enum.reduce_while(rows, :ok, fn row, :ok ->
        validate_revision_row(conn, boundaries, revision_row(row))
      end)
    end
  end

  defp revision_row([
         document_id,
         revision_id,
         generation,
         parent,
         history_id,
         digest,
         deleted,
         body_json,
         leaf
       ]) do
    %{
      document_id: document_id,
      revision_id: revision_id,
      generation: generation,
      parent: parent,
      history_id: history_id,
      digest: digest,
      deleted: deleted,
      body_json: body_json,
      leaf: leaf
    }
  end

  defp validate_revision_row(conn, boundaries, row) do
    %{
      document_id: document_id,
      revision_id: revision_id,
      generation: generation,
      parent: parent,
      history_id: history_id,
      digest: digest,
      deleted: deleted,
      body_json: body_json,
      leaf: leaf
    } = row

    body = revision_body(deleted, body_json)

    with true <- is_binary(history_id) and history_id != "",
         {:ok, calculated} <- Id.calculate(document_id, history_id, parent, deleted == 1, body, %{}),
         true <- calculated == revision_id,
         true <- digest == revision_digest_part(revision_id),
         {:ok, expected_generation} <- Id.generation(revision_id),
         true <- expected_generation == generation,
         :ok <-
           validate_parent_row(
             conn,
             boundaries,
             document_id,
             revision_id,
             generation,
             history_id,
             parent
           ),
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

  defp revision_digest_part(revision_id) do
    case String.split(revision_id, "-", parts: 2) do
      [_generation, digest] when is_binary(digest) and digest != "" -> digest
      _ -> nil
    end
  end

  defp validate_parent_row(
         _conn,
         _boundaries,
         _document_id,
         _revision_id,
         _generation,
         _history_id,
         nil
       ),
       do: :ok

  defp validate_parent_row(
         conn,
         boundaries,
         document_id,
         revision_id,
         generation,
         history_id,
         parent
       ) do
    case Connection.query(
           conn,
           "SELECT 1 FROM revisions AS r JOIN documents AS d ON d.doc_key = r.doc_key WHERE d.document_id = ? AND r.revision_id = ?",
           [document_id, parent]
         ) do
      {:ok, [[1]]} ->
        :ok

      {:ok, []} ->
        if truncated_parent_allowed?(boundaries, document_id, history_id, generation),
          do: :ok,
          else:
            {:error,
             ElixirDB.Error.integrity_violation("revision has a dangling parent", %{
               revision: revision_id,
               parent_revision: parent
             })}

      {:error, reason} ->
        {:error, normalize_error(reason)}
    end
  end

  defp truncated_parent_allowed?(boundaries, document_id, history_id, generation) do
    Enum.any?(boundaries, fn %{boundary: boundary} ->
      boundary.document_id == document_id and boundary.history_id == history_id and
        (boundary.retired or
           (is_integer(boundary.minimum_retained_generation) and
              generation >= boundary.minimum_retained_generation))
    end)
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

  defp validate_change_rows(conn, floor) do
    with {:ok, rows} <-
           Connection.query(
             conn,
             "SELECT sequence, document_id, winning_revision, leaf_set_json FROM changes ORDER BY sequence"
           ) do
      Enum.reduce_while(rows, {:ok, nil}, fn [sequence, document_id, winning, leaf_json], acc ->
        validate_change_row(conn, floor, sequence, document_id, winning, leaf_json, acc)
      end)
      |> case do
        {:ok, _} -> :ok
        {:error, error} -> {:error, error}
      end
    end
  end

  defp validate_change_row(conn, floor, sequence, document_id, winning, leaf_json, {:ok, previous}) do
    with true <- sequence > floor,
         true <- previous == nil or sequence > previous,
         {:ok, leaves} <- StrictDecoder.decode(leaf_json),
         true <- is_list(leaves),
         :ok <- validate_change_leaves(conn, document_id, winning, leaves) do
      {:cont, {:ok, sequence}}
    else
      false ->
        {:halt,
         {:error,
          ElixirDB.Error.integrity_violation("change row is at or below the retention floor", %{
            sequence: sequence,
            floor: floor
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
           "SELECT r.revision_id, r.deleted, r.history_id FROM revisions AS r JOIN documents AS d ON d.doc_key = r.doc_key WHERE d.document_id = ? AND r.revision_id = ?",
           [document_id, revision]
         ) do
      {:ok, [[^revision, deleted, history_id]]} ->
        supplied_deleted = MapAccess.get(leaf, :deleted)
        supplied_history_id = MapAccess.get(leaf, :history_id)

        validate_change_leaf_marker(
          supplied_deleted == (deleted == 1) and supplied_history_id == history_id,
          supplied_deleted,
          revision
        )

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

  defp validate_db_meta(meta) do
    validators = [
      fn -> validate_uuid(meta.database_uuid) end,
      fn -> validate_history_epoch(meta.history_epoch) end,
      fn -> validate_non_negative(meta.current_sequence, "current_sequence") end,
      fn -> validate_non_negative(meta.retention_floor_sequence, "retention_floor_sequence") end,
      fn -> validate_non_negative(meta.compaction_epoch, "compaction_epoch") end,
      fn -> validate_floor_within_sequence(meta) end
    ]

    case Enum.find_value(validators, & &1.()) do
      nil -> :ok
      error -> error
    end
  end

  defp validate_uuid(uuid) when is_binary(uuid) and uuid != "", do: :ok

  defp validate_uuid(_),
    do: {:error, ElixirDB.Error.integrity_violation("database UUID is invalid")}

  defp validate_history_epoch(epoch) when is_binary(epoch) and epoch != "", do: :ok

  defp validate_history_epoch(_),
    do: {:error, ElixirDB.Error.integrity_violation("history epoch is invalid")}

  defp validate_non_negative(value, _field) when is_integer(value) and value >= 0, do: :ok

  defp validate_non_negative(_, field),
    do: {:error, ElixirDB.Error.integrity_violation("metadata field is invalid", %{field: field})}

  defp validate_floor_within_sequence(%{
         current_sequence: sequence,
         retention_floor_sequence: floor
       }) do
    if floor <= sequence,
      do: :ok,
      else:
        {:error,
         ElixirDB.Error.integrity_violation("retention floor exceeds current sequence", %{
           floor: floor,
           current_sequence: sequence
         })}
  end

  defp validate_boundary_records(boundaries, meta) do
    computed =
      boundaries
      |> Enum.map(& &1.boundary)
      |> BoundaryPage.digest_for()

    stored = meta.retention_boundary_digest

    with :ok <- validate_boundary_digest(stored, computed, boundaries) do
      validate_boundary_entries(boundaries)
    end
  end

  defp validate_boundary_digest(stored, computed, boundaries) do
    cond do
      boundaries == [] and stored in [nil, ""] ->
        :ok

      is_binary(stored) and stored == computed ->
        :ok

      true ->
        {:error, ElixirDB.Error.integrity_violation("retention boundary digest mismatch")}
    end
  end

  defp validate_boundary_entries(boundaries) do
    Enum.reduce_while(boundaries, :ok, fn %{boundary: boundary, compaction_epoch: epoch}, :ok ->
      with true <- is_integer(epoch) and epoch >= 0,
           true <- is_binary(boundary.history_id) and boundary.history_id != "",
           true <- is_list(boundary.retired_branch_roots),
           :ok <- validate_retired_branch_roots(boundary) do
        {:cont, :ok}
      else
        false ->
          {:halt,
           {:error, ElixirDB.Error.integrity_violation("retention boundary record is invalid")}}

        {:error, error} ->
          {:halt, {:error, error}}
      end
    end)
  end

  defp validate_retired_branch_roots(%RetentionBoundary{retired_branch_roots: roots}) do
    if Enum.all?(roots, &(is_binary(&1) and &1 != "")),
      do: :ok,
      else: {:error, ElixirDB.Error.integrity_violation("retention boundary roots are invalid")}
  end

  defp validate_peer_records(peers, meta) do
    Enum.reduce_while(peers, :ok, fn peer, :ok ->
      case validate_peer_record(peer, meta) do
        :ok -> {:cont, :ok}
        {:error, error} -> {:halt, {:error, error}}
      end
    end)
  end

  defp validate_peer_record(%PeerPosition{} = peer, meta) do
    cond do
      peer.source_database_uuid != meta.database_uuid ->
        {:error, ElixirDB.Error.integrity_violation("peer source database UUID mismatch")}

      peer.source_history_epoch != meta.history_epoch ->
        {:error, ElixirDB.Error.integrity_violation("peer source history epoch mismatch")}

      peer.safe_source_sequence > meta.current_sequence ->
        {:error, ElixirDB.Error.integrity_violation("peer safe sequence exceeds source sequence")}

      peer.installed_source_compaction_epoch > meta.compaction_epoch ->
        {:error, ElixirDB.Error.integrity_violation("peer installed compaction epoch is invalid")}

      peer.safe_source_sequence < meta.retention_floor_sequence and peer.status == :active ->
        {:error,
         ElixirDB.Error.integrity_violation("active peer safe sequence is below retention floor")}

      true ->
        :ok
    end
  end

  defp validate_retention_maintenance(conn) do
    case RetentionRecords.maintenance_counter(conn) do
      {:ok, _counter} -> :ok
      {:error, error} -> {:error, error}
    end
  end

  defp validate_checkpoints(conn, meta) do
    with {:ok, rows} <- RetentionRecords.list_by_namespace(conn, "checkpoints") do
      Enum.reduce_while(rows, :ok, &validate_checkpoint_row(&1, meta, &2))
    end
  end

  defp validate_checkpoint_row(%{value: value}, meta, :ok) do
    case validate_checkpoint_value(value, meta) do
      :ok -> {:cont, :ok}
      {:error, error} -> {:halt, {:error, error}}
    end
  end

  defp validate_checkpoint_value(value, meta) when is_map(value) do
    with {:ok, checkpoint} <- Checkpoint.from_wire(value) do
      validate_checkpoint_fields(checkpoint, meta)
    end
  end

  defp validate_checkpoint_value(_value, _meta),
    do: {:error, ElixirDB.Error.integrity_violation("checkpoint record is invalid")}

  defp validate_checkpoint_fields(%Checkpoint{} = checkpoint, _meta) do
    validators = [
      fn -> validate_checkpoint_safe_sequence(checkpoint) end,
      fn -> validate_checkpoint_installed_epoch(checkpoint) end,
      fn -> validate_checkpoint_history(checkpoint) end
    ]

    case Enum.find_value(validators, & &1.()) do
      nil -> :ok
      error -> error
    end
  end

  defp validate_checkpoint_safe_sequence(checkpoint) do
    if checkpoint.safe_source_sequence > checkpoint.source_sequence,
      do:
        {:error,
         ElixirDB.Error.integrity_violation("checkpoint safe sequence exceeds source sequence")}
  end

  defp validate_checkpoint_installed_epoch(checkpoint) do
    if checkpoint.installed_source_compaction_epoch > checkpoint.source_compaction_epoch,
      do:
        {:error,
         ElixirDB.Error.integrity_violation("checkpoint installed compaction epoch regressed")}
  end

  defp validate_checkpoint_history(checkpoint) do
    if checkpoint_history_monotonic?(checkpoint.history, checkpoint.source_sequence),
      do: nil,
      else: {:error, ElixirDB.Error.integrity_violation("checkpoint history regressed")}
  end

  defp checkpoint_history_monotonic?(history, source_sequence) when is_list(history) do
    history
    |> Enum.map(&checkpoint_history_sequence/1)
    |> descending_through?(source_sequence)
  end

  defp descending_through?(sequences, source_sequence) do
    Enum.reduce_while(sequences, nil, fn
      seq, _previous when not is_integer(seq) or seq < 0 or seq > source_sequence ->
        {:halt, :invalid}

      seq, nil ->
        {:cont, seq}

      seq, previous when seq <= previous ->
        {:cont, seq}

      _seq, _previous ->
        {:halt, :invalid}
    end) != :invalid
  end

  defp checkpoint_history_sequence(entry) when is_map(entry) do
    MapAccess.get(entry, :source_sequence)
  end

  defp checkpoint_history_sequence(_), do: nil

  defp normalize_error(%ElixirDB.Error{} = error), do: error

  defp normalize_error(reason),
    do: ElixirDB.Error.internal_error("SQLite operation failed", %{cause: inspect(reason)})
end
