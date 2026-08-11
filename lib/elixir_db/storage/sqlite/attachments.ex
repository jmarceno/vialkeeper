defmodule ElixirDB.Storage.SQLite.Attachments do
  @moduledoc """
  SQLite attachment-metadata SQL for Version 1.

  Owns `revision_attachments` and `pending_blobs` fact access: manifest row
  insert/load, ticket resolution, pending row upsert/delete, retained/pending
  digest listing, and reachable-size lookup. Shared live-digest union, pending
  TTL, and reachability policy live in `ElixirDB.Storage.Services.Attachments`.
  Mutation orchestration stays in `Mutations`; revision-row SQL stays in
  `Revisions`.
  """

  alias ElixirDB.Attachments.Manifest
  alias ElixirDB.Attachments.MetadataRequest
  alias ElixirDB.Attachments.Ticket
  alias ElixirDB.MapAccess
  alias ElixirDB.Storage.SQLite.Connection

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
         {:ok, resolved_revision} <- MetadataRequest.resolve_revision_id(doc, revision_id),
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
  @spec lookup_reachable_size(Connection.handle(), binary()) ::
          {:ok, non_neg_integer()} | {:error, ElixirDB.Error.t()}
  def lookup_reachable_size(conn, digest) when is_binary(digest) do
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

  @doc "Inserts or renews a pending-blob row from a shared orchestration payload."
  @spec put_pending_blob(Connection.handle(), map()) ::
          {:ok, map()} | {:error, ElixirDB.Error.t()}
  def put_pending_blob(conn, row) when is_map(row) do
    digest = MapAccess.get(row, :digest)
    logical_size = MapAccess.get(row, :logical_size) || MapAccess.get(row, :length)
    expires_at = MapAccess.get(row, :expires_at)
    updated_at = MapAccess.get(row, :updated_at)

    with {:ok, digest} <- Manifest.validate_digest(digest),
         true <- is_integer(logical_size) and logical_size >= 0,
         true <- is_binary(expires_at) and is_binary(updated_at) do
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
             [digest, logical_size, expires_at, updated_at]
           ) do
        :ok ->
          {:ok,
           %{
             digest: digest,
             logical_size: logical_size,
             length: logical_size,
             expires_at: expires_at
           }}

        {:error, reason} ->
          {:error, normalize_error(reason)}
      end
    else
      false ->
        {:error, ElixirDB.Error.invalid_request("pending blob row is incomplete")}

      {:error, _} = error ->
        error
    end
  end

  @doc "Deletes pending protection for digests."
  @spec delete_pending_digests(Connection.handle(), [binary()]) ::
          :ok | {:error, ElixirDB.Error.t()}
  def delete_pending_digests(_conn, []), do: :ok

  def delete_pending_digests(conn, digests) when is_list(digests) do
    with :ok <- MetadataRequest.validate_digest_list(digests) do
      delete_each_pending(conn, digests)
    end
  end

  @doc "Lists digests retained by any revision attachment row."
  @spec list_retained_digests(Connection.handle()) ::
          {:ok, [binary()]} | {:error, ElixirDB.Error.t()}
  def list_retained_digests(conn) do
    case Connection.query(conn, "SELECT DISTINCT blob_digest FROM revision_attachments") do
      {:ok, rows} -> {:ok, Enum.map(rows, &List.first/1)}
      {:error, reason} -> {:error, normalize_error(reason)}
    end
  end

  @doc "Lists pending-blob rows as maps."
  @spec list_pending_blobs(Connection.handle()) :: {:ok, [map()]} | {:error, ElixirDB.Error.t()}
  def list_pending_blobs(conn) do
    case Connection.query(
           conn,
           "SELECT blob_digest, logical_size, expires_at, updated_at FROM pending_blobs"
         ) do
      {:ok, rows} ->
        {:ok,
         Enum.map(rows, fn [digest, logical_size, expires_at, updated_at] ->
           %{
             digest: digest,
             logical_size: logical_size,
             expires_at: expires_at,
             updated_at: updated_at
           }
         end)}

      {:error, reason} ->
        {:error, normalize_error(reason)}
    end
  end

  defp delete_each_pending(conn, digests) do
    Enum.reduce_while(digests, :ok, fn digest, :ok ->
      case Connection.execute(conn, "DELETE FROM pending_blobs WHERE blob_digest = ?", [digest]) do
        :ok -> {:cont, :ok}
        {:error, reason} -> {:halt, {:error, normalize_error(reason)}}
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

  defp bundle_root(sqlite_path) when is_binary(sqlite_path),
    do: Path.dirname(Path.expand(sqlite_path))

  defp normalize_error(%ElixirDB.Error{} = error), do: error

  defp normalize_error(reason),
    do: ElixirDB.Error.internal_error("SQLite operation failed", %{cause: inspect(reason)})
end
