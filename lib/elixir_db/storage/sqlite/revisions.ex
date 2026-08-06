defmodule ElixirDB.Storage.SQLite.Revisions do
  @moduledoc """
  Revision-row SQL helpers for the Version 1 SQLite adapter.

  Owns find/insert/leaf queries against the `revisions` table, parent checks,
  and leaf-set encoding used by change-feed rows. Chain reads live in `Chains`;
  import writes live in `Import`. Transaction boundaries remain in the adapter.
  """

  alias ElixirDB.Domain.Revision
  alias ElixirDB.JSON.{Canonical, StrictDecoder}
  alias ElixirDB.Storage.SQLite.Connection

  @doc false
  def get(adapter, request), do: ElixirDB.Storage.SQLite.Adapter.get_revision(adapter, request)

  def import(adapter, request),
    do: ElixirDB.Storage.SQLite.Adapter.import_revision_chains(adapter, request)

  def chains(adapter, request),
    do: ElixirDB.Storage.SQLite.Adapter.get_revision_chains(adapter, request)

  @doc """
  Loads one revision by document key and revision id.
  """
  @spec find(Connection.handle(), integer(), binary() | nil) ::
          {:ok, Revision.t()} | {:error, ElixirDB.Error.t()}
  def find(_conn, _doc_key, nil),
    do: {:error, ElixirDB.Error.document_not_found("document has no winning revision")}

  def find(conn, doc_key, revision_id) do
    case Connection.query(
           conn,
           "SELECT revision_id, generation, parent_revision, digest, deleted, body_json, insertion_sequence FROM revisions WHERE doc_key = ? AND revision_id = ?",
           [doc_key, revision_id]
         ) do
      {:ok, [[id, generation, parent, digest_value, deleted, body_json, sequence]]} ->
        {:ok, from_row(doc_key, id, generation, parent, digest_value, deleted, body_json, sequence)}

      {:ok, []} ->
        {:error, ElixirDB.Error.revision_not_found("revision not found", %{revision: revision_id})}

      {:error, reason} ->
        {:error, normalize_error(reason)}
    end
  end

  @doc """
  Loads all leaf revisions for a document key.
  """
  @spec load_leaves(Connection.handle(), integer()) ::
          {:ok, [Revision.t()]} | {:error, ElixirDB.Error.t()}
  def load_leaves(conn, doc_key) do
    case Connection.query(
           conn,
           "SELECT revision_id, generation, parent_revision, digest, deleted, body_json, insertion_sequence FROM revisions WHERE doc_key = ? AND is_leaf = 1 ORDER BY revision_id",
           [doc_key]
         ) do
      {:ok, rows} ->
        {:ok,
         Enum.map(rows, fn [id, generation, parent, digest_value, deleted, body_json, sequence] ->
           from_row(doc_key, id, generation, parent, digest_value, deleted, body_json, sequence)
         end)}

      {:error, reason} ->
        {:error, normalize_error(reason)}
    end
  end

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
  Inserts a revision and clears the parent's leaf marker.
  """
  @spec insert(Connection.handle(), integer(), Revision.t()) ::
          :ok | {:error, ElixirDB.Error.t()}
  def insert(conn, doc_key, %Revision{} = revision) do
    body = if revision.deleted, do: nil, else: Canonical.encode!(revision.body)

    with :ok <-
           Connection.execute(
             conn,
             "UPDATE revisions SET is_leaf = 0 WHERE doc_key = ? AND revision_id = ?",
             [doc_key, revision.parent_revision]
           ),
         :ok <-
           Connection.execute(
             conn,
             "INSERT INTO revisions(doc_key, revision_id, generation, parent_revision, digest, deleted, body_json, insertion_sequence, is_leaf) VALUES (?, ?, ?, ?, ?, ?, ?, 0, 1)",
             [
               doc_key,
               revision.revision_id,
               revision.generation,
               revision.parent_revision,
               revision.digest,
               if(revision.deleted, do: 1, else: 0),
               body
             ]
           ) do
      :ok
    else
      {:error, reason} -> {:error, normalize_error(reason)}
    end
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
  def encode_leaf_set(leaves),
    do:
      Canonical.encode(
        Enum.map(leaves, fn leaf -> %{"revision" => leaf.revision_id, "deleted" => leaf.deleted} end)
      )

  @doc """
  True when two revision structs describe the same stored content.
  """
  @spec same?(Revision.t(), Revision.t()) :: boolean()
  def same?(a, b),
    do:
      a.revision_id == b.revision_id and a.generation == b.generation and
        a.parent_revision == b.parent_revision and a.deleted == b.deleted and a.body == b.body

  @doc false
  def from_row(_doc_key, id, generation, parent, digest_value, deleted, body_json, sequence) do
    %Revision{
      document_id: nil,
      revision_id: id,
      generation: generation,
      parent_revision: parent,
      digest: digest_value,
      deleted: deleted == 1,
      body: if(body_json, do: decode_json!(body_json)),
      insertion_sequence: sequence
    }
  end

  defp decode_json!(json) do
    case StrictDecoder.decode(json) do
      {:ok, value} -> value
      _ -> nil
    end
  end

  defp normalize_error(%ElixirDB.Error{} = error), do: error

  defp normalize_error(reason),
    do: ElixirDB.Error.internal_error("SQLite operation failed", %{cause: inspect(reason)})
end
