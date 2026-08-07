defmodule ElixirDB.Storage.SQLite.FullTextIndexes do
  @moduledoc """
  Full-text logical-index facade for the Version 1 SQLite adapter.

  Physical FTS5 DDL, search, and refresh live in `Indexes`. Catalog-row
  transactions remain in the adapter; this module thin-wraps the FTS path.
  """

  use ElixirDB.Storage.SQLite.IndexFacade
  alias ElixirDB.Storage.SQLite.{Connection, Indexes}

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
end
