defmodule ElixirDB.Storage.SQLite.FullTextIndexes do
  @moduledoc """
  Full-text logical-index facade for the Version 1 SQLite adapter.

  Physical FTS5 DDL, search, and refresh live in `Indexes`. Catalog-row
  transactions remain in the adapter; this module thin-wraps the FTS path.
  """

  alias ElixirDB.Storage.SQLite.{Adapter, Connection, Indexes}

  @doc false
  def create(adapter, definition), do: Adapter.create_index(adapter, definition)

  def delete(adapter, id), do: Adapter.delete_index(adapter, id)
  def rebuild(adapter, id), do: Adapter.rebuild_index(adapter, id)

  @doc """
  Creates the physical full-text index for a logical definition.
  """
  @spec create_physical(Connection.handle(), binary(), map()) ::
          {:ok, map()} | {:error, ElixirDB.Error.t()}
  def create_physical(conn, index_id, definition), do: Indexes.create(conn, index_id, definition)

  @doc """
  Drops the physical full-text index described by adapter metadata.
  """
  @spec drop(Connection.handle(), map()) :: :ok | {:error, ElixirDB.Error.t()}
  def drop(conn, metadata), do: Indexes.drop(conn, metadata)

  @doc """
  Refreshes FTS rows for one document.
  """
  @spec refresh_document(Connection.handle(), map(), integer(), map() | nil, boolean()) ::
          :ok | {:error, ElixirDB.Error.t()}
  def refresh_document(conn, metadata, doc_key, body, deleted),
    do: Indexes.refresh_document(conn, metadata, doc_key, body, deleted)

  @doc """
  Runs a compiled full-text search against the physical FTS table.
  """
  @spec search(Connection.handle(), map(), binary(), binary()) ::
          {:ok, list()} | {:error, ElixirDB.Error.t()}
  def search(conn, metadata, text, mode), do: Indexes.search(conn, metadata, text, mode)

  @doc """
  Runs full-text physical integrity probes.
  """
  @spec integrity(Connection.handle(), map()) :: {:ok, map()} | {:error, ElixirDB.Error.t()}
  def integrity(conn, metadata), do: Indexes.integrity(conn, metadata)
end
