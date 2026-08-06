defmodule ElixirDB.Storage.SQLite.Revisions do
  @moduledoc false
  def get(adapter, request), do: ElixirDB.Storage.SQLite.Adapter.get_revision(adapter, request)

  def import(adapter, request),
    do: ElixirDB.Storage.SQLite.Adapter.import_revision_chains(adapter, request)

  def chains(adapter, request),
    do: ElixirDB.Storage.SQLite.Adapter.get_revision_chains(adapter, request)
end
