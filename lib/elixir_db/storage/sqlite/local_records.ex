defmodule ElixirDB.Storage.SQLite.LocalRecords do
  @moduledoc false
  def get(adapter, namespace, key),
    do: ElixirDB.Storage.SQLite.Adapter.get_local_record(adapter, namespace, key)

  def put(adapter, request),
    do: ElixirDB.Storage.SQLite.Adapter.put_local_record_cas(adapter, request)
end
