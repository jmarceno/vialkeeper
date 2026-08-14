defmodule ElixirDB.Storage.SQLite.Revisions do
  @moduledoc """
  Revision-row SQL helpers for the Version 1 SQLite adapter.

  Owns find/insert/leaf queries against the `revisions` table, parent checks,
  and leaf-set encoding used by change-feed rows. Attachment manifests are
  loaded and written through `Attachments` in the same SQLite transaction.
  Chain reads live in `Chains`; import writes live in `Import`. Transaction
  boundaries remain in the adapter.
  """

  alias ElixirDB.Attachments.Manifest
  alias ElixirDB.Domain.Revision
  alias ElixirDB.JSON.{Canonical, StrictDecoder}
  alias ElixirDB.Revisions.Compare
  alias ElixirDB.Storage.SQLite.Adapter
  alias ElixirDB.Storage.SQLite.{Attachments, Connection, TermBlob}
  @revision_body_term_cache_limit 256
  @doc false
  def get(adapter, request), do: Adapter.get_revision(adapter, request)

  def import(adapter, request),
    do: Adapter.import_revision_chains(adapter, request)

  def chains(adapter, request),
    do: Adapter.get_revision_chains(adapter, request)

  @doc """
  Loads one revision by document key and revision id, including attachments.
  """
  @spec find(Connection.handle(), integer(), binary() | nil) ::
          {:ok, Revision.t()} | {:error, ElixirDB.Error.t()}
  def find(_conn, _doc_key, nil),
    do: {:error, ElixirDB.Error.document_not_found("document has no winning revision")}

  def find(conn, doc_key, revision_id) do
    case Connection.query(
           conn,
           """
           SELECT r.revision_id, r.generation, r.parent_revision, r.history_id,
                  r.digest, r.deleted, r.body_json, r.body_term, r.insertion_sequence,
                  a.attachment_name, a.blob_digest, a.logical_size, a.content_type
           FROM revisions AS r
           LEFT JOIN revision_attachments AS a
             ON a.doc_key = r.doc_key AND a.revision_id = r.revision_id
           WHERE r.doc_key = ? AND r.revision_id = ?
           ORDER BY a.attachment_name
           """,
           [doc_key, revision_id]
         ) do
      {:ok, [first_row | _] = rows} ->
        [
          id,
          generation,
          parent,
          history_id,
          digest_value,
          deleted,
          body_json,
          body_term,
          sequence | _
        ] =
          first_row

        with {:ok, attachments} <- manifest_from_rows(rows) do
          {:ok,
           from_row(
             [
               id,
               generation,
               parent,
               history_id,
               digest_value,
               deleted,
               body_json,
               body_term,
               sequence
             ],
             attachments
           )}
        end

      {:ok, []} ->
        {:error, ElixirDB.Error.revision_not_found("revision not found", %{revision: revision_id})}

      {:error, reason} ->
        {:error, normalize_error(reason)}
    end
  end

  defp manifest_from_rows(rows) do
    rows
    |> Enum.reject(fn [_, _, _, _, _, _, _, _, _, name | _] -> is_nil(name) end)
    |> Map.new(fn row ->
      [_, _, _, _, _, _, _, _, _, name, digest, logical_size, content_type] = row
      {name, Manifest.entry(digest, logical_size, content_type)}
    end)
    |> Manifest.normalize()
  end

  @doc """
  Loads all leaf revisions for a document key, including attachments.
  """
  @spec load_leaves(Connection.handle(), integer()) ::
          {:ok, [Revision.t()]} | {:error, ElixirDB.Error.t()}
  def load_leaves(conn, doc_key) do
    case Connection.query(
           conn,
           "SELECT r.revision_id, r.generation, r.parent_revision, r.history_id, r.digest, r.deleted, r.body_json, r.body_term, r.insertion_sequence, a.attachment_name, a.blob_digest, a.logical_size, a.content_type FROM revisions AS r LEFT JOIN revision_attachments AS a ON a.doc_key = r.doc_key AND a.revision_id = r.revision_id WHERE r.doc_key = ? AND r.is_leaf = 1 ORDER BY r.revision_id, a.attachment_name",
           [doc_key]
         ) do
      {:ok, rows} ->
        rows |> leaf_revisions() |> reverse_leaves()

      {:error, reason} ->
        {:error, normalize_error(reason)}
    end
  end

  defp leaf_revisions(rows) do
    Enum.reduce_while(Enum.chunk_by(rows, &Enum.take(&1, 9)), {:ok, []}, fn grouped, {:ok, acc} ->
      case manifest_from_rows(grouped) do
        {:ok, attachments} ->
          {:cont, {:ok, [from_row(Enum.take(hd(grouped), 9), attachments) | acc]}}

        {:error, error} ->
          {:halt, {:error, error}}
      end
    end)
  end

  @doc """
  Walks parent links from `revision_id` toward the root, excluding the start.
  """
  @spec load_ancestors(Connection.handle(), integer(), binary()) ::
          {:ok, [Revision.t()]} | {:error, ElixirDB.Error.t()}
  def load_ancestors(conn, doc_key, revision_id)
      when is_integer(doc_key) and is_binary(revision_id) do
    with {:ok, revisions_by_id} <- load_revisions_by_id(conn, doc_key) do
      case Map.fetch(revisions_by_id, revision_id) do
        {:ok, %Revision{parent_revision: parent}} ->
          walk_ancestors_map(revisions_by_id, parent, %{}, [])

        :error ->
          {:error,
           ElixirDB.Error.revision_not_found("revision not found", %{revision: revision_id})}
      end
    end
  end

  defp load_revisions_by_id(conn, doc_key) do
    case Connection.query(
           conn,
           """
           SELECT r.revision_id, r.generation, r.parent_revision, r.history_id,
                  r.digest, r.deleted, r.body_json, r.body_term, r.insertion_sequence,
                  a.attachment_name, a.blob_digest, a.logical_size, a.content_type
           FROM revisions AS r
           LEFT JOIN revision_attachments AS a
             ON a.doc_key = r.doc_key AND a.revision_id = r.revision_id
           WHERE r.doc_key = ?
           ORDER BY r.revision_id, a.attachment_name
           """,
           [doc_key]
         ) do
      {:ok, rows} ->
        rows
        |> Enum.chunk_by(&Enum.take(&1, 9))
        |> Enum.reduce_while({:ok, %{}}, &accumulate_revision_row/2)

      {:error, reason} ->
        {:error, normalize_error(reason)}
    end
  end

  defp accumulate_revision_row(grouped, {:ok, acc}) do
    case manifest_from_rows(grouped) do
      {:ok, attachments} ->
        revision = from_row(Enum.take(hd(grouped), 9), attachments)
        {:cont, {:ok, Map.put(acc, revision.revision_id, revision)}}

      {:error, error} ->
        {:halt, {:error, error}}
    end
  end

  defp walk_ancestors_map(_revisions_by_id, nil, _seen, acc), do: {:ok, Enum.reverse(acc)}

  defp walk_ancestors_map(revisions_by_id, revision_id, seen, acc) do
    if Map.has_key?(seen, revision_id) do
      {:error, ElixirDB.Error.integrity_violation("revision ancestry cycle detected")}
    else
      case Map.fetch(revisions_by_id, revision_id) do
        {:ok, revision} ->
          walk_ancestors_map(
            revisions_by_id,
            revision.parent_revision,
            Map.put(seen, revision_id, true),
            [revision | acc]
          )

        :error ->
          {:error,
           ElixirDB.Error.revision_not_found("revision not found", %{revision: revision_id})}
      end
    end
  end

  defp reverse_leaves({:ok, revisions}), do: {:ok, Enum.reverse(revisions)}
  defp reverse_leaves(error), do: error

  @doc """
  Returns leaf revisions, or `[]` when the leaf query fails.
  """
  @spec leaves(Connection.handle(), integer()) :: [Revision.t()]
  def leaves(conn, doc_key) do
    case load_leaves(conn, doc_key) do
      {:ok, value} -> value
      _ -> []
    end
  end

  @doc """
  Inserts a revision, its attachment manifest, and clears the parent's leaf marker.
  """
  @spec insert(Connection.handle(), integer(), Revision.t()) ::
          :ok | {:error, ElixirDB.Error.t()}
  def insert(conn, doc_key, %Revision{} = revision),
    do: insert(conn, doc_key, revision, nil)

  @spec insert(Connection.handle(), integer(), Revision.t(), binary() | nil) ::
          :ok | {:error, ElixirDB.Error.t()}
  def insert(conn, doc_key, %Revision{} = revision, body_json) do
    body = if revision.deleted, do: nil, else: body_json || Canonical.encode!(revision.body)

    with {:ok, body_term} <- stored_body_term(revision, body),
         :ok <- clear_parent_leaf(conn, doc_key, revision.parent_revision),
         :ok <-
           Connection.execute(
             conn,
             "INSERT INTO revisions(doc_key, revision_id, generation, parent_revision, history_id, digest, deleted, body_json, body_term, insertion_sequence, is_leaf) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 0, 1)",
             [
               doc_key,
               revision.revision_id,
               revision.generation,
               revision.parent_revision,
               revision.history_id,
               revision.digest,
               if(revision.deleted, do: 1, else: 0),
               body,
               TermBlob.bind(body_term)
             ]
           ),
         :ok <-
           Attachments.insert_manifest(
             conn,
             doc_key,
             revision.revision_id,
             revision.attachments || %{}
           ) do
      :ok
    else
      {:error, reason} -> {:error, normalize_error(reason)}
    end
  end

  defp clear_parent_leaf(_conn, _doc_key, nil), do: :ok

  defp clear_parent_leaf(conn, doc_key, parent_revision) do
    Connection.execute(
      conn,
      "UPDATE revisions SET is_leaf = 0 WHERE doc_key = ? AND revision_id = ?",
      [doc_key, parent_revision]
    )
  end

  @doc """
  Inserts a revision, or accepts an identical existing row (import/replay).
  """
  @spec insert_or_accept(Connection.handle(), integer(), Revision.t()) ::
          :ok | {:error, ElixirDB.Error.t()}
  def insert_or_accept(conn, doc_key, %Revision{} = revision) do
    case find(conn, doc_key, revision.revision_id) do
      {:ok, existing} ->
        if same?(existing, revision),
          do: :ok,
          else:
            {:error,
             ElixirDB.Error.integrity_violation("existing revision differs from imported revision")}

      {:error, %ElixirDB.Error{code: :revision_not_found}} ->
        insert(conn, doc_key, revision)

      {:error, error} ->
        {:error, error}
    end
  end

  @doc """
  Ensures a parent revision exists when one is required.
  """
  @spec ensure_parent(Connection.handle(), integer(), binary() | nil) ::
          :ok | {:error, ElixirDB.Error.t()}
  def ensure_parent(_conn, _doc_key, nil), do: :ok

  def ensure_parent(conn, doc_key, parent) do
    case find(conn, doc_key, parent) do
      {:ok, _} ->
        :ok

      {:error, _} ->
        {:error,
         ElixirDB.Error.integrity_violation("revision parent is missing", %{parent_revision: parent})}
    end
  end

  @doc """
  Encodes leaf revisions for a change-feed `leaf_set_json` column.
  """
  @spec encode_leaf_set([Revision.t()]) :: {:ok, binary()} | {:error, term()}
  def encode_leaf_set(leaves), do: Compare.encode_leaf_set(leaves)

  @doc """
  True when two revision structs describe the same stored content.
  """
  @spec same?(Revision.t(), Revision.t()) :: boolean()
  def same?(a, b), do: Compare.same?(a, b)

  @doc false
  def from_row(row, attachments \\ %{})

  def from_row(
        [
          id,
          generation,
          parent,
          history_id,
          digest_value,
          deleted,
          body_json,
          body_term,
          sequence
        ],
        attachments
      )
      when is_map(attachments) do
    %Revision{
      document_id: nil,
      history_id: history_id,
      revision_id: id,
      generation: generation,
      parent_revision: parent,
      digest: digest_value,
      deleted: deleted == 1,
      body: if(body_json, do: decode_body!(body_term, body_json)),
      attachments: attachments,
      insertion_sequence: sequence
    }
  end

  defp decode_json!(json) do
    case StrictDecoder.decode(json) do
      {:ok, value} -> value
      _ -> nil
    end
  end

  defp decode_body!(body_term, body_json) do
    case TermBlob.decode_with_cache(
           body_term,
           body_json,
           :revision_body_term,
           @revision_body_term_cache_limit
         ) do
      {:ok, value} -> value
      {:fallback, _reason} -> decode_json!(body_json)
    end
  end

  defp stored_body_term(%Revision{deleted: true}, _body), do: {:ok, nil}

  defp stored_body_term(%Revision{body: body}, body_json) when is_binary(body_json),
    do: TermBlob.encode(body, body_json)

  defp normalize_error(%ElixirDB.Error{} = error), do: error

  defp normalize_error(reason),
    do: ElixirDB.Error.internal_error("SQLite operation failed", %{cause: inspect(reason)})
end
