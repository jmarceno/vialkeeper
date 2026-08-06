defmodule ElixirDB.Storage.SQLite.Changes do
  @moduledoc false
  def read(adapter, request), do: ElixirDB.Storage.SQLite.Adapter.read_changes(adapter, request)
end
