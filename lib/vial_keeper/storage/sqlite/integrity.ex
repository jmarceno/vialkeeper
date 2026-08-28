defmodule VialKeeper.Storage.SQLite.Integrity do
  @moduledoc """
  SQLite integrity loading and physical probes for a Version 1 database file.

  Logical revision/document/change consistency is validated by
  `VialKeeper.Integrity.Rules` over a normalized snapshot. This module owns
  PRAGMA checks, required-table presence, dual JSON/term encoding probes,
  filesystem blob verification, index physical probes, and view metadata.
  """

  alias VialKeeper.Attachments.{FilesystemStore, Manifest}
  alias VialKeeper.Integrity.Rules
  alias VialKeeper.JSON.StrictDecoder
  alias VialKeeper.Storage.SQLite.Adapter

  alias VialKeeper.Storage.SQLite.{
    Attachments,
    Connection,
    Indexes,
    Meta,
    RetentionRecords,
    Schema,
    TermBlob
  }

  alias VialKeeper.Error

  @doc false
  @spec check(map()) :: {:ok, map()} | {:error, Error.t()}
  def check(adapter), do: Adapter.integrity_check(adapter, %{})

  @doc """
  Runs structural and logical integrity validators for an open connection.

  On success returns a report map. Unreferenced final blobs without unexpired
  pending protection are counted as `reclaimable_blobs` (garbage, not corruption).
  """
  @spec run(Connection.handle(), [map()], binary() | nil) ::
          {:ok, map()} | {:error, Error.t()}
  def run(conn, indexes, bundle_root \\ nil) when is_list(indexes) do
    with {:ok, snapshot} <- load_integrity_snapshot(conn),
         :ok <- Rules.validate(snapshot) do
      physical_integrity_check(conn, indexes, bundle_root)
    end
  end

  @doc """
  Loads a normalized logical integrity snapshot from SQLite.

  Dual JSON/term encodings are verified while decoding; physical PRAGMA and
  filesystem probes are not included.
  """
  @spec load_integrity_snapshot(Connection.handle()) ::
          {:ok, map()} | {:error, Error.t()}
  def load_integrity_snapshot(conn) do
    with {:ok, meta} <- Meta.load(conn),
         {:ok, boundaries} <-
           RetentionRecords.list_boundaries(conn, source_database_uuid: meta.database_uuid),
         {:ok, peers} <- RetentionRecords.list_peers(conn),
         {:ok, maintenance_counter} <- RetentionRecords.maintenance_counter(conn),
         {:ok, revision_attachments} <- load_all_revision_attachments(conn),
         {:ok, revisions} <- load_revisions(conn, revision_attachments),
         {:ok, documents} <- load_documents(conn),
         {:ok, changes} <- load_changes(conn),
         {:ok, pending_blobs} <- load_pending_blobs(conn),
         {:ok, checkpoints} <- load_checkpoints(conn) do
      {:ok,
       %{
         meta: meta,
         boundaries: boundaries,
         peers: peers,
         maintenance_counter: maintenance_counter,
         documents: documents,
         revisions: revisions,
         changes: changes,
         pending_blobs: pending_blobs,
         checkpoints: checkpoints,
         revision_attachments: revision_attachments
       }}
    else
      {:error, %Error{} = error} ->
        {:error, error}

      {:error, reason} ->
        {:error, normalize_error(reason)}
    end
  end

  @doc """
  Runs SQLite-owned physical integrity probes and returns backend details.
  """
  @spec physical_integrity_check(Connection.handle(), [map()], binary() | nil) ::
          {:ok, map()} | {:error, Error.t()}
  def physical_integrity_check(conn, indexes, bundle_root \\ nil) when is_list(indexes) do
    with {:ok, [["ok"]]} <- Connection.pragma(conn, "integrity_check"),
         {:ok, []} <- Connection.pragma(conn, "foreign_key_check"),
         :ok <- Schema.required_tables_present(conn),
         {:ok, attachment_report} <- validate_physical_attachment_blobs(conn, bundle_root),
         :ok <- validate_index_rows(conn, indexes),
         {:ok, view_report} <- view_metadata(conn) do
      {:ok,
       Map.merge(attachment_report, %{
         indexes: length(indexes),
         views: view_report,
         backend_details: %{
           engine: "sqlite",
           indexes: length(indexes),
           pragma_integrity_check: :ok,
           foreign_key_check: :ok
         }
       })}
    else
      {:ok, rows} when is_list(rows) ->
        {:error, Error.integrity_violation("SQLite integrity check failed", %{results: rows})}

      {:error, :missing_tables} ->
        {:error, Error.integrity_violation("required SQLite tables are missing")}

      {:error, %Error{} = error} ->
        {:error, error}

      {:error, reason} ->
        {:error, normalize_error(reason)}
    end
  end

  defp load_all_revision_attachments(conn) do
    case Connection.query(
           conn,
           """
           SELECT d.document_id, ra.revision_id, ra.attachment_name, ra.blob_digest,
                  ra.logical_size, ra.content_type
           FROM revision_attachments AS ra
           LEFT JOIN documents AS d ON d.doc_key = ra.doc_key
           ORDER BY ra.revision_id, ra.attachment_name
           """
         ) do
      {:ok, rows} ->
        {:ok,
         Enum.map(rows, fn [document_id, revision_id, name, digest, size, content_type] ->
           %{
             document_id: document_id,
             revision_id: revision_id,
             name: name,
             digest: digest,
             logical_size: size,
             content_type: content_type
           }
         end)}

      {:error, reason} ->
        {:error, normalize_error(reason)}
    end
  end

  defp load_revisions(conn, revision_attachments) do
    attachments_by_revision =
      Enum.group_by(revision_attachments, fn row -> {row.document_id, row.revision_id} end)

    with {:ok, rows} <-
           Connection.query(
             conn,
             "SELECT d.document_id, r.revision_id, r.generation, r.parent_revision, r.history_id, r.digest, r.deleted, r.body_json, r.body_term, r.is_leaf FROM revisions AS r JOIN documents AS d ON d.doc_key = r.doc_key ORDER BY d.document_id, r.revision_id"
           ) do
      collect_normalized_rows(rows, &normalize_revision_row(&1, attachments_by_revision))
    end
  end

  defp normalize_revision_row(
         [
           document_id,
           revision_id,
           generation,
           parent,
           history_id,
           digest,
           deleted,
           body_json,
           body_term,
           leaf
         ],
         attachments_by_revision
       ) do
    body = revision_body(deleted, body_json)
    attachments = attachments_for(attachments_by_revision, document_id, revision_id, deleted)

    with :ok <- validate_revision_term(body_json, body_term, deleted, body),
         {:ok, attachment_map} <- attachments do
      {:ok,
       %{
         document_id: document_id,
         revision_id: revision_id,
         generation: generation,
         parent: parent,
         history_id: history_id,
         digest: digest,
         deleted: deleted == 1,
         body: body,
         attachments: attachment_map,
         is_leaf: leaf == 1
       }}
    end
  end

  defp attachments_for(by_revision, document_id, revision_id, _deleted) do
    rows = Map.get(by_revision, {document_id, revision_id}, [])

    {:ok,
     Map.new(rows, fn row ->
       {row.name, Manifest.entry(row.digest, row.logical_size, row.content_type)}
     end)}
  end

  defp load_documents(conn) do
    case Connection.query(
           conn,
           "SELECT document_id, winning_revision, winning_body_json, winning_body_term, winning_deleted, update_sequence FROM documents"
         ) do
      {:ok, rows} ->
        collect_normalized_rows(rows, &normalize_document_row/1)

      {:error, reason} ->
        {:error, normalize_error(reason)}
    end
  end

  defp normalize_document_row([
         document_id,
         winning,
         body_json,
         body_term,
         deleted,
         sequence
       ]) do
    with :ok <- validate_document_term(body_json, body_term, deleted) do
      body =
        cond do
          deleted == 1 -> nil
          is_binary(body_json) -> StrictDecoder.decode_or_nil(body_json)
          true -> nil
        end

      {:ok,
       %{
         document_id: document_id,
         winning_revision: winning,
         winning_deleted: deleted == 1,
         update_sequence: sequence,
         body: body
       }}
    end
  end

  defp validate_document_term(nil, nil, 1), do: :ok

  defp validate_document_term(body_json, body_term, 0)
       when is_binary(body_json) and is_binary(body_term) do
    case TermBlob.decode(body_term, body_json) do
      {:ok, _} -> :ok
      _ -> {:error, Error.integrity_violation("materialized winner is inconsistent")}
    end
  end

  defp validate_document_term(_body_json, _body_term, 1),
    do: {:error, Error.integrity_violation("materialized winner is inconsistent")}

  defp validate_document_term(_body_json, _body_term, _deleted), do: :ok

  defp load_changes(conn) do
    case Connection.query(
           conn,
           "SELECT sequence, document_id, winning_revision, leaf_set_json, leaf_set_term FROM changes ORDER BY sequence"
         ) do
      {:ok, rows} ->
        collect_normalized_rows(rows, &normalize_change_row/1)

      {:error, reason} ->
        {:error, normalize_error(reason)}
    end
  end

  defp collect_normalized_rows(rows, normalize_fun) when is_function(normalize_fun, 1) do
    rows
    |> Enum.reduce_while({:ok, []}, &append_normalized_row(&1, &2, normalize_fun))
    |> finalize_normalized_rows()
  end

  defp append_normalized_row(row, {:ok, acc}, normalize_fun) do
    case normalize_fun.(row) do
      {:ok, normalized} -> {:cont, {:ok, [normalized | acc]}}
      {:error, _} = error -> {:halt, error}
    end
  end

  defp finalize_normalized_rows({:ok, list}), do: {:ok, Enum.reverse(list)}
  defp finalize_normalized_rows(error), do: error

  defp normalize_change_row([sequence, document_id, winning, leaf_json, leaf_term]) do
    with {:ok, leaves} <- StrictDecoder.decode(leaf_json),
         {:ok, term_leaves} <- TermBlob.decode(leaf_term, leaf_json),
         :ok <- validate_change_terms(leaves, term_leaves) do
      {:ok,
       %{
         sequence: sequence,
         document_id: document_id,
         winning_revision: winning,
         leaf_revisions: leaves
       }}
    else
      {:fallback, _} ->
        {:error, Error.integrity_violation("change JSON and term differ")}

      {:error, error} ->
        {:error, error}
    end
  end

  defp validate_change_terms(leaves, leaves) when is_list(leaves), do: :ok

  defp validate_change_terms(_leaves, _term_leaves),
    do: {:error, Error.integrity_violation("change JSON and term differ")}

  defp load_pending_blobs(conn), do: Attachments.list_pending_blobs(conn)

  defp load_checkpoints(conn) do
    with {:ok, rows} <- RetentionRecords.list_by_namespace(conn, "checkpoints") do
      {:ok, Enum.map(rows, fn %{key: key, value: value} -> %{key: key, value: value} end)}
    end
  end

  defp revision_body(1, _body_json), do: nil
  defp revision_body(_deleted, body_json), do: StrictDecoder.decode_or_nil(body_json)

  defp validate_revision_term(nil, nil, 1, _body), do: :ok

  defp validate_revision_term(body_json, body_term, 1, _body)
       when not is_nil(body_json) or not is_nil(body_term),
       do: {:error, Error.integrity_violation("deleted revision has a body term")}

  defp validate_revision_term(body_json, body_term, _deleted, body) do
    case {StrictDecoder.decode(body_json), TermBlob.decode(body_term, body_json)} do
      {{:ok, ^body}, {:ok, ^body}} ->
        :ok

      _ ->
        {:error, Error.integrity_violation("revision JSON and body term differ")}
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

        case Rules.validate_digest_size_consistency(pairs) do
          :ok -> {:ok, pairs}
          {:error, _} = error -> error
        end

      {:error, reason} ->
        {:error, normalize_error(reason)}
    end
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

      {:error, %Error{} = error} ->
        {:halt,
         {:error,
          Error.integrity_violation(
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
         Error.integrity_violation("attachment blobs directory is unreadable", %{
           reason: inspect(reason)
         })}
    end
  end

  defp inventory_blob_prefix(blobs_path, entry, digests) do
    path = Path.join(blobs_path, entry)

    case File.lstat(path) do
      {:ok, %File.Stat{type: :symlink}} ->
        {:halt, {:error, Error.integrity_violation("attachment blob path contains a symlink")}}

      {:ok, %File.Stat{type: :directory}} ->
        inventory_directory_prefix(path, entry, digests)

      {:ok, %File.Stat{type: :regular}} ->
        {:halt,
         {:error, Error.integrity_violation("attachment blobs root contains a non-directory entry")}}

      {:error, reason} ->
        {:halt,
         {:error,
          Error.integrity_violation("attachment blob path is unreadable", %{
            reason: inspect(reason)
          })}}
    end
  end

  defp inventory_directory_prefix(path, entry, digests) do
    if Regex.match?(~r/^[0-9a-f]{2}$/, entry) do
      continue_inventory(inventory_blob_files(path, entry, digests))
    else
      {:halt, {:error, Error.integrity_violation("attachment blob prefix directory is malformed")}}
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
         Error.integrity_violation("attachment blob prefix is unreadable", %{
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
        {:error, Error.integrity_violation("attachment blob representation is a symlink")}

      {:ok, %File.Stat{type: :regular}} ->
        accept_blob_filename(prefix, file, digests)

      {:ok, _} ->
        {:error, Error.integrity_violation("attachment blob entry has an invalid type")}

      {:error, reason} ->
        {:error,
         Error.integrity_violation("attachment blob entry is unreadable", %{
           reason: inspect(reason)
         })}
    end
  end

  defp accept_blob_filename(prefix, file, digests) do
    case parse_blob_filename(prefix, file) do
      {:ok, digest} ->
        record_physical_digest(digests, digest)

      :error ->
        {:error, Error.integrity_violation("attachment blob filename is malformed")}
    end
  end

  defp record_physical_digest(digests, digest) do
    if MapSet.member?(digests, digest) do
      {:error, Error.integrity_violation("attachment has multiple physical representations")}
    else
      {:ok, MapSet.put(digests, digest)}
    end
  end

  defp parse_blob_filename(prefix, file) do
    case Regex.run(~r/^([0-9a-f]{64})\.blob$/, file) do
      [_, digest] ->
        if String.slice(digest, 0, 2) == prefix, do: {:ok, digest}, else: :error

      _ ->
        :error
    end
  end

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

  defp normalize_error(%Error{} = error), do: error

  defp normalize_error(reason),
    do: Error.internal_error("SQLite operation failed", %{cause: inspect(reason)})
end
