defmodule ElixirDB.Storage.SQLite.StructuredIndexes do
  @moduledoc """
  Structured logical-index facade for the Version 1 SQLite adapter.

  Physical DDL and probes live in `Indexes`. Catalog-row transactions live in
  `IndexCatalog` (via the adapter); this module thin-wraps the structured
  physical path.
  """

  alias ElixirDB.Storage.SQLite.{Adapter, Connection, Indexes}

  @doc false
  def create(adapter, definition), do: Adapter.create_index(adapter, definition)

  def delete(adapter, id), do: Adapter.delete_index(adapter, id)
  def rebuild(adapter, id), do: Adapter.rebuild_index(adapter, id)

  @doc """
  Creates the physical structured index for a logical definition.
  """
  @spec create_physical(Connection.handle(), binary(), map()) ::
          {:ok, map()} | {:error, ElixirDB.Error.t()}
  def create_physical(conn, index_id, definition), do: Indexes.create(conn, index_id, definition)

  @doc """
  Drops the physical structured index described by adapter metadata.
  """
  @spec drop(Connection.handle(), map()) :: :ok | {:error, ElixirDB.Error.t()}
  def drop(conn, metadata), do: Indexes.drop(conn, metadata)

  @doc """
  Runs structured physical integrity probes.
  """
  @spec integrity(Connection.handle(), map()) :: {:ok, map()} | {:error, ElixirDB.Error.t()}
  def integrity(conn, metadata), do: Indexes.integrity(conn, metadata)
end
