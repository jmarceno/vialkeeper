defmodule ElixirDB.Storage.SQLite.Integrity do
  @moduledoc """
  Logical and SQLite integrity checks for a Version 1 database file.

  Owns pragma checks, required-table presence, and revision/document/change
  consistency validators. Index physical probes still call
  `ElixirDB.Storage.SQLite.Indexes.integrity/2`; further document/revision SQL
  centralization remains in the adapter.
  """

  alias ElixirDB.Attachments.{FilesystemStore, Manifest}
  alias ElixirDB.Domain.{BoundaryPage, Checkpoint, PeerPosition, RetentionBoundary}
  alias ElixirDB.JSON.{Canonical, StrictDecoder}
  alias ElixirDB.MapAccess
  alias ElixirDB.Revisions.Id
  alias ElixirDB.Storage.SQLite.Adapter
  alias ElixirDB.Storage.SQLite.{Connection, Indexes, Meta, RetentionRecords, TermBlob}
  @doc false
  def check(adapter), do: Adapter.integrity_check(adapter, %{})

  @doc """
  Runs structural and logical integrity validators for an open connection.

  On success returns a report map. Unreferenced final blobs without unexpired
  pending protection are counted as `reclaimable_blobs` (garbage, not corruption).
  """
  @spec run(Connection.handle(), [map()], binary() | nil) ::
          {:ok, map()} | {:error, ElixirDB.Error.t()}
  def run(conn, indexes, bundle_root \\ nil) when is_list(indexes) do
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
         :ok <- validate_revision_attachments(conn),
         {:ok, attachment_report} <- validate_physical_attachment_blobs(conn, bundle_root),
         :ok <- validate_pending_blobs(conn),
         :ok <- validate_document_rows(conn),
         :ok <- validate_change_rows(conn, meta.retention_floor_sequence),
         :ok <- validate_checkpoints(conn, meta),
         :ok <- validate_index_rows(conn, indexes),
         {:ok, view_report} <- view_metadata(conn) do
      {:ok, Map.put(attachment_report, :views, view_report)}
    else
      {:ok, rows} when is_list(rows) ->
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
      ~w(db_meta documents revisions changes local_records replication_jobs index_definitions revision_attachments pending_blobs view_definitions view_state view_rows)

    with {:ok, rows} <-
           Connection.query(
             conn,
             "SELECT name FROM sqlite_master WHERE type = 'table' AND name IN ('db_meta', 'documents', 'revisions', 'changes', 'local_records', 'replication_jobs', 'index_definitions', 'revision_attachments', 'pending_blobs', 'view_definitions', 'view_state', 'view_rows')"
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
             "SELECT d.document_id, r.doc_key, r.revision_id, r.generation, r.parent_revision, r.history_id, r.digest, r.deleted, r.body_json, r.body_term, r.is_leaf FROM revisions AS r JOIN documents AS d ON d.doc_key = r.doc_key ORDER BY d.document_id, r.revision_id"
           ) do
      Enum.reduce_while(rows, :ok, fn row, :ok ->
        validate_revision_row(conn, boundaries, revision_row(row))
      end)
    end
  end

  defp revision_row([
         document_id,
         doc_key,
         revision_id,
         generation,
         parent,
         history_id,
         digest,
         deleted,
         body_json,
         body_term,
         leaf
       ]) do
    %{
      document_id: document_id,
      doc_key: doc_key,
      revision_id: revision_id,
      generation: generation,
      parent: parent,
      history_id: history_id,
      digest: digest,
      deleted: deleted,
      body_json: body_json,
      body_term: body_term,
      leaf: leaf
    }
  end

  defp validate_revision_row(conn, boundaries, row) do
    document_id = row.document_id
    doc_key = row.doc_key
    revision_id = row.revision_id
    generation = row.generation
    parent = row.parent
    history_id = row.history_id
    digest = row.digest
    deleted = row.deleted
    body_json = row.body_json
    body_term = row.body_term
    leaf = row.leaf

    body = revision_body(deleted, body_json)

    with true <- is_binary(history_id) and history_id != "",
         :ok <- validate_revision_term(body_json, body_term, deleted, body),
         {:ok, attachments} <- load_revision_attachments(conn, doc_key, revision_id, deleted),
         {:ok, calculated} <-
           Id.calculate(document_id, history_id, parent, deleted == 1, body, attachments),
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

  defp load_revision_attachments(conn, doc_key, revision_id, 1) do
    case query_revision_attachments(conn, doc_key, revision_id) do
      {:ok, []} ->
        {:ok, %{}}

      {:ok, _rows} ->
        {:error,
         ElixirDB.Error.integrity_violation(
           "tombstone revisions must have an empty attachment manifest",
           %{revision: revision_id}
         )}

      {:error, _} = error ->
        error
    end
  end

  defp load_revision_attachments(conn, doc_key, revision_id, _deleted) do
    case query_revision_attachments(conn, doc_key, revision_id) do
      {:ok, rows} ->
        {:ok,
         Map.new(rows, fn [name, digest, logical_size, content_type] ->
           {name, Manifest.entry(digest, logical_size, content_type)}
         end)}

      {:error, _} = error ->
        error
    end
  end

  defp query_revision_attachments(conn, doc_key, revision_id) do
    case Connection.query(
           conn,
           """
           SELECT attachment_name, blob_digest, logical_size, content_type
           FROM revision_attachments
           WHERE doc_key = ? AND revision_id = ?
           ORDER BY attachment_name
           """,
           [doc_key, revision_id]
         ) do
      {:ok, rows} -> {:ok, rows}
      {:error, reason} -> {:error, normalize_error(reason)}
    end
  end

  defp validate_revision_attachments(conn) do
    with {:ok, orphan_rows} <-
           Connection.query(
             conn,
             """
             SELECT ra.revision_id FROM revision_attachments AS ra
             LEFT JOIN revisions AS r
               ON r.doc_key = ra.doc_key AND r.revision_id = ra.revision_id
             WHERE r.revision_id IS NULL
             LIMIT 1
             """
           ),
         :ok <-
           (case orphan_rows do
              [] ->
                :ok

              _ ->
                {:error,
                 ElixirDB.Error.integrity_violation(
                   "revision_attachments row references a missing revision"
                 )}
            end),
         {:ok, invalid_rows} <-
           Connection.query(
             conn,
             """
             SELECT attachment_name, blob_digest, logical_size, content_type
             FROM revision_attachments
             WHERE logical_size < 0
                OR length(blob_digest) != 64
                OR content_type = ''
             LIMIT 1
             """
           ) do
      case invalid_rows do
        [] ->
          validate_revision_attachment_digests(conn)

        _ ->
          {:error,
           ElixirDB.Error.integrity_violation("revision_attachments row fields are invalid")}
      end
    end
  end

  defp validate_revision_attachment_digests(conn) do
    case Connection.query(conn, "SELECT blob_digest FROM revision_attachments") do
      {:ok, rows} ->
        Enum.reduce_while(rows, :ok, &validate_attachment_digest_row/2)

      {:error, reason} ->
        {:error, normalize_error(reason)}
    end
  end

  defp validate_physical_attachment_blobs(_conn, nil), do: {:ok, %{reclaimable_blobs: 0}}

  defp validate_physical_attachment_blobs(conn, bundle_root) when is_binary(bundle_root) do
    with {:ok, referenced} <- referenced_attachment_sizes(conn),
         :ok <- verify_referenced_blobs(bundle_root, referenced),
         {:ok, live} <-
           live_attachment_digests(
             conn,
             MapSet.new(Enum.map(referenced, fn {digest, _size} -> digest end))
           ),
         {:ok, physical} <- inventory_physical_blobs(bundle_root) do
      reclaimable =
        physical
        |> MapSet.difference(live)
        |> MapSet.size()

      {:ok, %{reclaimable_blobs: reclaimable}}
    end
  end

  defp referenced_attachment_sizes(conn) do
    case Connection.query(
           conn,
           """
           SELECT blob_digest, logical_size
           FROM revision_attachments
           ORDER BY blob_digest, logical_size
           """
         ) do
      {:ok, rows} ->
        pairs = Enum.map(rows, fn [digest, size] -> {digest, size} end)

        case inconsistent_digest_sizes(pairs) do
          :ok -> {:ok, pairs}
          {:error, _} = error -> error
        end

      {:error, reason} ->
        {:error, normalize_error(reason)}
    end
  end

  defp inconsistent_digest_sizes(pairs) do
    pairs
    |> Enum.group_by(fn {digest, _} -> digest end, fn {_, size} -> size end)
    |> Enum.reduce_while(:ok, fn {digest, sizes}, :ok ->
      case Enum.uniq(sizes) do
        [_] ->
          {:cont, :ok}

        uniq ->
          {:halt,
           {:error,
            ElixirDB.Error.integrity_violation(
              "attachment digest has inconsistent logical sizes across retained manifests",
              %{digest: digest, sizes: uniq}
            )}}
      end
    end)
  end

  defp verify_referenced_blobs(bundle_root, referenced) do
    Enum.reduce_while(referenced, :ok, fn {digest, logical_size}, :ok ->
      validate_physical_attachment_row(bundle_root, digest, logical_size)
    end)
  end

  defp validate_physical_attachment_row(bundle_root, digest, logical_size) do
    case FilesystemStore.verify(bundle_root, digest, logical_size) do
      :ok ->
        {:cont, :ok}

      {:error, %ElixirDB.Error{} = error} ->
        {:halt,
         {:error,
          ElixirDB.Error.integrity_violation(
            "referenced attachment blob failed physical verification",
            %{digest: digest, cause: error.message}
          )}}
    end
  end

  defp live_attachment_digests(conn, referenced) do
    now_iso = DateTime.utc_now() |> DateTime.to_iso8601()

    case Connection.query(
           conn,
           """
           SELECT blob_digest FROM pending_blobs
           WHERE expires_at > ?
           """,
           [now_iso]
         ) do
      {:ok, rows} ->
        pending = MapSet.new(rows, &List.first/1)
        {:ok, MapSet.union(referenced, pending)}

      {:error, reason} ->
        {:error, normalize_error(reason)}
    end
  end

  defp inventory_physical_blobs(bundle_root) do
    blobs_path = Path.join(Path.expand(bundle_root), "blobs")

    case File.ls(blobs_path) do
      {:ok, entries} ->
        Enum.reduce_while(entries, {:ok, MapSet.new()}, fn entry, {:ok, digests} ->
          inventory_blob_prefix(blobs_path, entry, digests)
        end)

      {:error, :enoent} ->
        {:ok, MapSet.new()}

      {:error, reason} ->
        {:error,
         ElixirDB.Error.integrity_violation("attachment blobs directory is unreadable", %{
           reason: inspect(reason)
         })}
    end
  end

  defp inventory_blob_prefix(blobs_path, entry, digests) do
    path = Path.join(blobs_path, entry)

    case File.lstat(path) do
      {:ok, %File.Stat{type: :symlink}} ->
        {:halt,
         {:error, ElixirDB.Error.integrity_violation("attachment blob path contains a symlink")}}

      {:ok, %File.Stat{type: :directory}} ->
        inventory_directory_prefix(path, entry, digests)

      {:ok, %File.Stat{type: :regular}} ->
        {:halt,
         {:error,
          ElixirDB.Error.integrity_violation("attachment blobs root contains a non-directory entry")}}

      {:error, reason} ->
        {:halt,
         {:error,
          ElixirDB.Error.integrity_violation("attachment blob path is unreadable", %{
            reason: inspect(reason)
          })}}
    end
  end

  defp inventory_directory_prefix(path, entry, digests) do
    if Regex.match?(~r/^[0-9a-f]{2}$/, entry) do
      continue_inventory(inventory_blob_files(path, entry, digests))
    else
      {:halt,
       {:error, ElixirDB.Error.integrity_violation("attachment blob prefix directory is malformed")}}
    end
  end

  defp continue_inventory({:ok, next}), do: {:cont, {:ok, next}}
  defp continue_inventory({:error, _} = error), do: {:halt, error}

  defp inventory_blob_files(prefix_path, prefix, digests) do
    case File.ls(prefix_path) do
      {:ok, files} ->
        Enum.reduce_while(files, {:ok, digests}, &reduce_blob_file(prefix_path, prefix, &1, &2))

      {:error, reason} ->
        {:error,
         ElixirDB.Error.integrity_violation("attachment blob prefix is unreadable", %{
           reason: inspect(reason)
         })}
    end
  end

  defp reduce_blob_file(prefix_path, prefix, file, {:ok, acc}) do
    case inventory_blob_file(prefix_path, prefix, file, acc) do
      {:ok, next} -> {:cont, {:ok, next}}
      {:error, _} = error -> {:halt, error}
    end
  end

  defp inventory_blob_file(prefix_path, prefix, file, digests) do
    path = Path.join(prefix_path, file)

    case File.lstat(path) do
      {:ok, %File.Stat{type: :symlink}} ->
        {:error, ElixirDB.Error.integrity_violation("attachment blob representation is a symlink")}

      {:ok, %File.Stat{type: :regular}} ->
        accept_blob_filename(prefix, file, digests)

      {:ok, _} ->
        {:error, ElixirDB.Error.integrity_violation("attachment blob entry has an invalid type")}

      {:error, reason} ->
        {:error,
         ElixirDB.Error.integrity_violation("attachment blob entry is unreadable", %{
           reason: inspect(reason)
         })}
    end
  end

  defp accept_blob_filename(prefix, file, digests) do
    case parse_blob_filename(prefix, file) do
      {:ok, digest} ->
        record_physical_digest(digests, digest)

      :error ->
        {:error, ElixirDB.Error.integrity_violation("attachment blob filename is malformed")}
    end
  end

  defp record_physical_digest(digests, digest) do
    if MapSet.member?(digests, digest) do
      {:error,
       ElixirDB.Error.integrity_violation("attachment has multiple physical representations")}
    else
      {:ok, MapSet.put(digests, digest)}
    end
  end

  defp parse_blob_filename(prefix, file) do
    case Regex.run(~r/^([0-9a-f]{64})\.(raw|zst)$/, file) do
      [_, digest, _ext] ->
        if String.slice(digest, 0, 2) == prefix, do: {:ok, digest}, else: :error

      _ ->
        :error
    end
  end

  defp validate_attachment_digest_row([digest], :ok) do
    if valid_digest?(digest) do
      {:cont, :ok}
    else
      {:halt,
       {:error,
        ElixirDB.Error.integrity_violation("revision_attachments digest is invalid", %{
          digest: digest
        })}}
    end
  end

  defp validate_pending_blobs(conn) do
    case Connection.query(
           conn,
           """
           SELECT blob_digest, logical_size, expires_at, updated_at FROM pending_blobs
           """
         ) do
      {:ok, rows} ->
        Enum.reduce_while(rows, :ok, &validate_pending_blob_row/2)

      {:error, reason} ->
        {:error, normalize_error(reason)}
    end
  end

  defp validate_pending_blob_row([digest, size, expires_at, updated_at], :ok) do
    if valid_digest?(digest) and is_integer(size) and size >= 0 and valid_iso8601?(expires_at) and
         valid_iso8601?(updated_at) do
      {:cont, :ok}
    else
      {:halt,
       {:error,
        ElixirDB.Error.integrity_violation("pending_blobs row fields are invalid", %{
          digest: digest
        })}}
    end
  end

  defp valid_digest?(digest) when is_binary(digest),
    do: Regex.match?(~r/^[0-9a-f]{64}$/, digest)

  defp valid_digest?(_), do: false

  defp valid_iso8601?(value) when is_binary(value) do
    match?({:ok, _, _}, DateTime.from_iso8601(value))
  end

  defp valid_iso8601?(_), do: false

  defp revision_body(1, _body_json), do: nil
  defp revision_body(_deleted, body_json), do: StrictDecoder.decode_or_nil(body_json)

  defp validate_revision_term(nil, nil, 1, _body), do: :ok

  defp validate_revision_term(body_json, body_term, 1, _body)
       when not is_nil(body_json) or not is_nil(body_term),
       do: {:error, ElixirDB.Error.integrity_violation("deleted revision has a body term")}

  defp validate_revision_term(body_json, body_term, _deleted, body) do
    case {StrictDecoder.decode(body_json), TermBlob.decode(body_term, body_json)} do
      {{:ok, ^body}, {:ok, ^body}} ->
        :ok

      _ ->
        {:error, ElixirDB.Error.integrity_violation("revision JSON and body term differ")}
    end
  end

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
             "SELECT doc_key, document_id, winning_revision, winning_body_json, winning_body_term, winning_deleted, update_sequence FROM documents"
           ) do
      Enum.reduce_while(
        rows,
        :ok,
        fn [doc_key, document_id, winning, body_json, body_term, deleted, sequence], :ok ->
          validate_document_row(
            conn,
            doc_key,
            document_id,
            winning,
            body_json,
            body_term,
            deleted,
            sequence
          )
        end
      )
    end
  end

  defp validate_document_row(_conn, _doc_key, _document_id, nil, nil, nil, 1, 0),
    do: {:cont, :ok}

  defp validate_document_row(
         _conn,
         _doc_key,
         document_id,
         nil,
         _body_json,
         _body_term,
         _deleted,
         _sequence
       ),
       do:
         {:halt,
          {:error,
           ElixirDB.Error.integrity_violation("document has no winning revision", %{
             document_id: document_id
           })}}

  defp validate_document_row(
         conn,
         doc_key,
         document_id,
         winning,
         body_json,
         body_term,
         deleted,
         _sequence
       ) do
    case Connection.query(
           conn,
           "SELECT deleted, body_json, body_term FROM revisions WHERE doc_key = ? AND revision_id = ?",
           [doc_key, winning]
         ) do
      {:ok, [[revision_deleted, revision_body, revision_term]]} ->
        validate_materialized_winner(
          body_matches?(
            revision_deleted,
            revision_body,
            revision_term,
            body_json,
            body_term,
            deleted
          ),
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

  defp body_matches?(1, _revision_body, _revision_term, body_json, body_term, deleted),
    do: is_nil(body_json) and is_nil(body_term) and deleted == 1

  defp body_matches?(_revision_deleted, revision_body, revision_term, body_json, body_term, deleted) do
    is_binary(body_json) and deleted == 0 and
      is_binary(body_term) and
      match?({:ok, _}, TermBlob.decode(revision_term, revision_body)) and
      match?({:ok, _}, TermBlob.decode(body_term, body_json)) and
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
             "SELECT sequence, document_id, winning_revision, leaf_set_json, leaf_set_term FROM changes ORDER BY sequence"
           ) do
      Enum.reduce_while(rows, {:ok, nil}, fn [sequence, document_id, winning, leaf_json, leaf_term],
                                             acc ->
        validate_change_row(conn, floor, sequence, document_id, winning, leaf_json, leaf_term, acc)
      end)
      |> case do
        {:ok, _} -> :ok
        {:error, error} -> {:error, error}
      end
    end
  end

  defp validate_change_row(
         conn,
         floor,
         sequence,
         document_id,
         winning,
         leaf_json,
         leaf_term,
         {:ok, previous}
       ) do
    with :ok <- validate_change_sequence(sequence, previous, floor),
         {:ok, leaves} <- StrictDecoder.decode(leaf_json),
         {:ok, term_leaves} <- TermBlob.decode(leaf_term, leaf_json),
         :ok <- validate_change_terms(leaves, term_leaves),
         :ok <- validate_change_leaves(conn, document_id, winning, leaves) do
      {:cont, {:ok, sequence}}
    else
      {:error, error} ->
        {:halt, {:error, error}}
    end
  end

  defp validate_change_sequence(sequence, _previous, floor) when sequence <= floor,
    do:
      {:error,
       ElixirDB.Error.integrity_violation("change row is at or below the retention floor", %{
         sequence: sequence,
         floor: floor
       })}

  defp validate_change_sequence(sequence, previous, _floor)
       when not is_nil(previous) and sequence <= previous,
       do:
         {:error,
          ElixirDB.Error.integrity_violation("change sequences are not strictly increasing", %{
            sequence: sequence,
            previous: previous
          })}

  defp validate_change_sequence(_sequence, _previous, _floor), do: :ok

  defp validate_change_terms(leaves, leaves) when is_list(leaves), do: :ok

  defp validate_change_terms(_leaves, _term_leaves),
    do: {:error, ElixirDB.Error.integrity_violation("change JSON and term differ")}

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

  defp view_metadata(conn) do
    case Connection.query(
           conn,
           """
           SELECT d.view_id, d.name, d.definition_digest, s.status, s.indexed_through,
                  s.active_generation, s.building_generation
           FROM view_definitions AS d
           JOIN view_state AS s ON s.view_id = d.view_id
           ORDER BY d.name
           """
         ) do
      {:ok, rows} ->
        {:ok,
         Enum.map(rows, fn [
                             view_id,
                             name,
                             definition_digest,
                             status,
                             indexed_through,
                             active_generation,
                             building_generation
                           ] ->
           %{
             view_id: view_id,
             name: name,
             definition_digest: definition_digest,
             status: status,
             indexed_through: indexed_through,
             active_generation: active_generation,
             building_generation: building_generation
           }
         end)}

      {:error, reason} ->
        {:error, normalize_error(reason)}
    end
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
