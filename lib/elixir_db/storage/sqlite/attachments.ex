defmodule ElixirDB.Storage.SQLite.Attachments do
  @moduledoc """
  SQLite attachment-metadata SQL for Version 1.

  Owns `revision_attachments` and `pending_blobs` access: manifest row
  insert/load, ticket and blob-metadata resolution, pending protection, live
  digest paging, and expired-pending cleanup. Mutation orchestration stays in
  `Mutations`; revision-row SQL stays in `Revisions`.
  """

  alias ElixirDB.Attachments.Manifest
  alias ElixirDB.Attachments.Ticket
  alias ElixirDB.MapAccess
  alias ElixirDB.Storage.SQLite.Connection

  @pending_ttl_seconds 86_400
  @live_digest_page_size 4096
  @digest_pattern ~r/^[0-9a-f]{64}$/

  @doc """
  Inserts the complete immutable attachment manifest for one revision.
  """
  @spec insert_manifest(Connection.handle(), integer(), binary(), Manifest.t()) ::
          :ok | {:error, ElixirDB.Error.t()}
  def insert_manifest(conn, doc_key, revision_id, attachments)
      when is_integer(doc_key) and is_binary(revision_id) and is_map(attachments) do
    attachments
    |> Enum.sort_by(fn {name, _entry} -> name end)
    |> Enum.reduce_while(:ok, fn {name, entry}, :ok ->
      case insert_row(conn, doc_key, revision_id, name, entry) do
        :ok -> {:cont, :ok}
        {:error, error} -> {:halt, {:error, error}}
      end
    end)
  end

  @doc """
  Loads the complete attachment manifest for one revision.
  """
  @spec load_manifest(Connection.handle(), integer(), binary()) ::
          {:ok, Manifest.t()} | {:error, ElixirDB.Error.t()}
  def load_manifest(conn, doc_key, revision_id)
      when is_integer(doc_key) and is_binary(revision_id) do
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
      {:ok, rows} ->
        manifest =
          Map.new(rows, fn [name, digest, logical_size, content_type] ->
            {name, Manifest.entry(digest, logical_size, content_type)}
          end)

        Manifest.normalize(manifest)

      {:error, reason} ->
        {:error, normalize_error(reason)}
    end
  end

  @doc """
  Resolves an attachment ticket from document + optional revision + name.

  `revision` / `revision_id` of `nil` selects the current winning revision.
  Accepts wire keys (`id`, `revision`, `name`) and internal keys
  (`document_id`, `revision_id`, `attachment_name`).
  """
  @spec resolve_attachment_ticket(map(), map()) :: {:ok, map()} | {:error, ElixirDB.Error.t()}
  def resolve_attachment_ticket(adapter, request) when is_map(request) do
    document_id = MapAccess.get(request, :document_id) || MapAccess.get(request, :id)
    revision_id = MapAccess.get(request, :revision_id) || MapAccess.get(request, :revision)
    name = MapAccess.get(request, :attachment_name) || MapAccess.get(request, :name)

    with {:ok, name} <- Manifest.validate_name(name),
         {:ok, doc} <- find_document(adapter.conn, document_id),
         {:ok, resolved_revision} <- resolve_revision_id(doc, revision_id),
         {:ok, row} <-
           load_attachment_row(adapter.conn, doc.doc_key, resolved_revision, name) do
      Ticket.build(
        adapter.identity.database_uuid,
        bundle_root(adapter.path),
        row.blob_digest,
        row.logical_size,
        row.content_type,
        doc.document_id,
        resolved_revision,
        name
      )
    end
  end

  def resolve_attachment_ticket(_adapter, _request),
    do: {:error, ElixirDB.Error.invalid_request("attachment ticket request must be an object")}

  @doc """
  Returns authoritative logical size when the digest is reachable via unexpired
  pending protection or any retained revision attachment row.
  """
  @spec resolve_blob_metadata(Connection.handle(), map()) ::
          {:ok, map()} | {:error, ElixirDB.Error.t()}
  def resolve_blob_metadata(conn, request) when is_map(request) do
    with {:ok, digest} <- request_digest(request),
         {:ok, logical_size} <- lookup_reachable_size(conn, digest) do
      {:ok, %{digest: digest, logical_size: logical_size, length: logical_size}}
    end
  end

  def resolve_blob_metadata(_conn, _request),
    do: {:error, ElixirDB.Error.invalid_request("blob metadata request must be an object")}

  @doc """
  Inserts or renews a local pending-blob protection row for 24 hours.
  """
  @spec protect_pending_blob(Connection.handle(), map()) ::
          {:ok, map()} | {:error, ElixirDB.Error.t()}
  def protect_pending_blob(conn, request) when is_map(request) do
    with {:ok, digest} <- request_digest(request),
         {:ok, logical_size} <- request_logical_size(request) do
      now = DateTime.utc_now() |> DateTime.truncate(:second)
      expires_at = DateTime.add(now, @pending_ttl_seconds, :second)
      now_iso = DateTime.to_iso8601(now)
      expires_iso = DateTime.to_iso8601(expires_at)

      case Connection.execute(
             conn,
             """
             INSERT INTO pending_blobs(blob_digest, logical_size, expires_at, updated_at)
             VALUES (?, ?, ?, ?)
             ON CONFLICT(blob_digest) DO UPDATE SET
               logical_size = excluded.logical_size,
               expires_at = excluded.expires_at,
               updated_at = excluded.updated_at
             """,
             [digest, logical_size, expires_iso, now_iso]
           ) do
        :ok ->
          {:ok,
           %{
             digest: digest,
             logical_size: logical_size,
             length: logical_size,
             expires_at: expires_iso
           }}

        {:error, reason} ->
          {:error, normalize_error(reason)}
      end
    end
  end

  def protect_pending_blob(_conn, _request),
    do:
      {:error, ElixirDB.Error.invalid_request("pending blob protection request must be an object")}

  @doc """
  Removes pending protection for one digest or a list of digests.
  """
  @spec remove_pending_blob_protection(Connection.handle(), map()) ::
          {:ok, map()} | {:error, ElixirDB.Error.t()}
  def remove_pending_blob_protection(conn, request) when is_map(request) do
    digests =
      case MapAccess.get(request, :digests) do
        list when is_list(list) ->
          list

        _ ->
          case request_digest(request) do
            {:ok, digest} -> [digest]
            {:error, _} -> []
          end
      end

    with :ok <- validate_digest_list(digests),
         :ok <- delete_pending_digests(conn, digests) do
      {:ok, %{removed: length(digests), digests: digests}}
    end
  end

  def remove_pending_blob_protection(_conn, _request),
    do:
      {:error,
       ElixirDB.Error.invalid_request("remove pending blob protection request must be an object")}

  @doc """
  Removes pending protection for digests newly referenced by a revision manifest.

  Safe after the revision_attachments rows for those digests are visible in the
  same transaction.
  """
  @spec remove_pending_for_manifest(Connection.handle(), Manifest.t()) ::
          :ok | {:error, ElixirDB.Error.t()}
  def remove_pending_for_manifest(_conn, attachments) when map_size(attachments) == 0, do: :ok

  def remove_pending_for_manifest(conn, attachments) when is_map(attachments) do
    digests =
      attachments
      |> Map.values()
      |> Enum.map(& &1.digest)
      |> Enum.uniq()

    delete_pending_digests(conn, digests)
  end

  @doc """
  Pages the distinct union of retained revision digests and unexpired pending digests.
  """
  @spec list_live_attachment_digests(Connection.handle(), map()) ::
          {:ok, map()} | {:error, ElixirDB.Error.t()}
  def list_live_attachment_digests(conn, request) when is_map(request) do
    after_digest = MapAccess.get(request, :after_digest)
    limit = page_limit(MapAccess.get(request, :limit))
    now_iso = DateTime.utc_now() |> DateTime.truncate(:second) |> DateTime.to_iso8601()

    with :ok <- validate_after_digest(after_digest),
         {:ok, rows} <- query_live_digests(conn, after_digest, limit + 1, now_iso) do
      digests = Enum.map(rows, &List.first/1)
      page = Enum.take(digests, limit)

      next_after =
        if length(digests) > limit do
          List.last(page)
        else
          nil
        end

      {:ok, %{digests: page, next_after_digest: next_after}}
    end
  end

  def list_live_attachment_digests(_conn, _request),
    do: {:error, ElixirDB.Error.invalid_request("live attachment digest request must be an object")}

  @doc """
  Deletes expired pending_blobs rows. Optional `now` overrides the cutoff clock.
  """
  @spec cleanup_expired_pending_blobs(Connection.handle(), map()) ::
          {:ok, map()} | {:error, ElixirDB.Error.t()}
  def cleanup_expired_pending_blobs(conn, request) when is_map(request) do
    with {:ok, now_iso} <- cleanup_now(request),
         {:ok, [[count]]} <-
           Connection.query(
             conn,
             "SELECT COUNT(*) FROM pending_blobs WHERE expires_at <= ?",
             [now_iso]
           ),
         :ok <-
           Connection.execute(conn, "DELETE FROM pending_blobs WHERE expires_at <= ?", [now_iso]) do
      {:ok, %{removed: count}}
    else
      {:error, reason} -> {:error, normalize_error(reason)}
    end
  end

  def cleanup_expired_pending_blobs(_conn, _request),
    do: {:error, ElixirDB.Error.invalid_request("cleanup request must be an object")}

  @doc """
  Ensures every digest in a manifest is reachable via pending or retained rows.
  """
  @spec ensure_reachable(Connection.handle(), Manifest.t()) ::
          :ok | {:error, ElixirDB.Error.t()}
  def ensure_reachable(_conn, attachments) when map_size(attachments) == 0, do: :ok

  def ensure_reachable(conn, attachments) when is_map(attachments) do
    attachments
    |> Map.values()
    |> Enum.map(& &1.digest)
    |> Enum.uniq()
    |> Enum.reduce_while(:ok, fn digest, :ok ->
      case lookup_reachable_size(conn, digest) do
        {:ok, _size} -> {:cont, :ok}
        {:error, error} -> {:halt, {:error, error}}
      end
    end)
  end

  defp insert_row(conn, doc_key, revision_id, name, entry) do
    case Connection.execute(
           conn,
           """
           INSERT INTO revision_attachments(
             doc_key, revision_id, attachment_name, blob_digest, logical_size, content_type
           ) VALUES (?, ?, ?, ?, ?, ?)
           """,
           [doc_key, revision_id, name, entry.digest, entry.length, entry.content_type]
         ) do
      :ok -> :ok
      {:error, reason} -> {:error, normalize_error(reason)}
    end
  end

  defp find_document(conn, document_id) when is_binary(document_id) and document_id != "" do
    case Connection.query(
           conn,
           "SELECT doc_key, document_id, winning_revision FROM documents WHERE document_id = ?",
           [document_id]
         ) do
      {:ok, [[doc_key, id, winning]]} ->
        {:ok, %{doc_key: doc_key, document_id: id, winning_revision: winning}}

      {:ok, []} ->
        {:error, ElixirDB.Error.document_not_found("document does not exist")}

      {:error, reason} ->
        {:error, normalize_error(reason)}
    end
  end

  defp find_document(_conn, _document_id),
    do: {:error, ElixirDB.Error.invalid_request("document id must be a non-empty UTF-8 string")}

  defp resolve_revision_id(%{winning_revision: nil}, nil),
    do: {:error, ElixirDB.Error.document_not_found("document has no winning revision")}

  defp resolve_revision_id(%{winning_revision: winning}, nil) when is_binary(winning),
    do: {:ok, winning}

  defp resolve_revision_id(_doc, revision_id) when is_binary(revision_id) and revision_id != "",
    do: {:ok, revision_id}

  defp resolve_revision_id(_doc, _revision_id),
    do: {:error, ElixirDB.Error.invalid_request("revision must be a string or null")}

  defp load_attachment_row(conn, doc_key, revision_id, name) do
    case Connection.query(
           conn,
           """
           SELECT blob_digest, logical_size, content_type
           FROM revision_attachments
           WHERE doc_key = ? AND revision_id = ? AND attachment_name = ?
           """,
           [doc_key, revision_id, name]
         ) do
      {:ok, [[digest, logical_size, content_type]]} ->
        {:ok, %{blob_digest: digest, logical_size: logical_size, content_type: content_type}}

      {:ok, []} ->
        case revision_exists?(conn, doc_key, revision_id) do
          true ->
            {:error, ElixirDB.Error.attachment_not_found("attachment not found")}

          false ->
            {:error,
             ElixirDB.Error.revision_not_found("revision not found", %{revision: revision_id})}

          {:error, error} ->
            {:error, error}
        end

      {:error, reason} ->
        {:error, normalize_error(reason)}
    end
  end

  defp revision_exists?(conn, doc_key, revision_id) do
    case Connection.query(
           conn,
           "SELECT 1 FROM revisions WHERE doc_key = ? AND revision_id = ?",
           [doc_key, revision_id]
         ) do
      {:ok, [[1]]} -> true
      {:ok, []} -> false
      {:error, reason} -> {:error, normalize_error(reason)}
    end
  end

  defp lookup_reachable_size(conn, digest) do
    now_iso = DateTime.utc_now() |> DateTime.truncate(:second) |> DateTime.to_iso8601()

    case Connection.query(
           conn,
           """
           SELECT logical_size FROM pending_blobs
           WHERE blob_digest = ? AND expires_at > ?
           """,
           [digest, now_iso]
         ) do
      {:ok, [[size]]} ->
        {:ok, size}

      {:ok, []} ->
        case Connection.query(
               conn,
               """
               SELECT logical_size FROM revision_attachments
               WHERE blob_digest = ?
               LIMIT 1
               """,
               [digest]
             ) do
          {:ok, [[size]]} ->
            {:ok, size}

          {:ok, []} ->
            {:error, ElixirDB.Error.attachment_blob_not_found("attachment blob not found")}

          {:error, reason} ->
            {:error, normalize_error(reason)}
        end

      {:error, reason} ->
        {:error, normalize_error(reason)}
    end
  end

  defp request_digest(request) do
    digest = MapAccess.get(request, :digest) || MapAccess.get(request, :blob)
    Manifest.validate_digest(digest)
  end

  defp request_logical_size(request) do
    size = MapAccess.get(request, :logical_size) || MapAccess.get(request, :length)

    if is_integer(size) and size >= 0,
      do: {:ok, size},
      else: {:error, ElixirDB.Error.invalid_request("logical_size must be a non-negative integer")}
  end

  defp validate_digest_list(digests) when is_list(digests) do
    Enum.reduce_while(digests, :ok, fn digest, :ok ->
      case Manifest.validate_digest(digest) do
        {:ok, _} -> {:cont, :ok}
        {:error, error} -> {:halt, {:error, error}}
      end
    end)
  end

  defp delete_pending_digests(_conn, []), do: :ok

  defp delete_pending_digests(conn, digests) do
    Enum.reduce_while(digests, :ok, fn digest, :ok ->
      case Connection.execute(conn, "DELETE FROM pending_blobs WHERE blob_digest = ?", [digest]) do
        :ok -> {:cont, :ok}
        {:error, reason} -> {:halt, {:error, normalize_error(reason)}}
      end
    end)
  end

  defp page_limit(nil), do: @live_digest_page_size

  defp page_limit(limit) when is_integer(limit) and limit > 0,
    do: min(limit, @live_digest_page_size)

  defp page_limit(_), do: @live_digest_page_size

  defp validate_after_digest(nil), do: :ok

  defp validate_after_digest(digest) when is_binary(digest) do
    if Regex.match?(@digest_pattern, digest),
      do: :ok,
      else: {:error, ElixirDB.Error.invalid_request("after_digest must be lowercase SHA-256 hex")}
  end

  defp validate_after_digest(_),
    do: {:error, ElixirDB.Error.invalid_request("after_digest must be lowercase SHA-256 hex")}

  defp query_live_digests(conn, nil, limit, now_iso) do
    Connection.query(
      conn,
      """
      SELECT digest FROM (
        SELECT blob_digest AS digest FROM revision_attachments
        UNION
        SELECT blob_digest AS digest FROM pending_blobs WHERE expires_at > ?
      )
      ORDER BY digest
      LIMIT ?
      """,
      [now_iso, limit]
    )
  end

  defp query_live_digests(conn, after_digest, limit, now_iso) do
    Connection.query(
      conn,
      """
      SELECT digest FROM (
        SELECT blob_digest AS digest FROM revision_attachments
        UNION
        SELECT blob_digest AS digest FROM pending_blobs WHERE expires_at > ?
      )
      WHERE digest > ?
      ORDER BY digest
      LIMIT ?
      """,
      [now_iso, after_digest, limit]
    )
  end

  defp cleanup_now(request) do
    case MapAccess.get(request, :now) do
      nil ->
        {:ok, DateTime.utc_now() |> DateTime.truncate(:second) |> DateTime.to_iso8601()}

      %DateTime{} = dt ->
        {:ok, DateTime.to_iso8601(DateTime.truncate(dt, :second))}

      value when is_binary(value) ->
        case DateTime.from_iso8601(value) do
          {:ok, dt, _} -> {:ok, DateTime.to_iso8601(DateTime.truncate(dt, :second))}
          _ -> {:error, ElixirDB.Error.invalid_request("now must be an RFC3339 timestamp")}
        end

      _ ->
        {:error, ElixirDB.Error.invalid_request("now must be an RFC3339 timestamp")}
    end
  end

  defp bundle_root(sqlite_path) when is_binary(sqlite_path),
    do: Path.dirname(Path.expand(sqlite_path))

  defp normalize_error(%ElixirDB.Error{} = error), do: error

  defp normalize_error(reason),
    do: ElixirDB.Error.internal_error("SQLite operation failed", %{cause: inspect(reason)})
end
