defmodule ElixirDB.Storage.SQLite.FullTextIndexes do
  @moduledoc false
  def create(adapter, definition),
    do: ElixirDB.Storage.SQLite.Adapter.create_index(adapter, definition)

  def delete(adapter, id), do: ElixirDB.Storage.SQLite.Adapter.delete_index(adapter, id)
  def rebuild(adapter, id), do: ElixirDB.Storage.SQLite.Adapter.rebuild_index(adapter, id)
end
