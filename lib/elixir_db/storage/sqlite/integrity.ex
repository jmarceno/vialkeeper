defmodule ElixirDB.Storage.SQLite.Integrity do
  @moduledoc false
  def check(adapter), do: ElixirDB.Storage.SQLite.Adapter.integrity_check(adapter, %{})
end
