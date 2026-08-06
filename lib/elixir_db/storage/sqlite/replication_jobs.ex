defmodule ElixirDB.Storage.SQLite.ReplicationJobs do
  @moduledoc false
  def list(adapter), do: ElixirDB.Storage.SQLite.Adapter.list_replication_jobs(adapter)

  def put(adapter, request),
    do: ElixirDB.Storage.SQLite.Adapter.put_replication_job(adapter, request)

  def delete(adapter, id), do: ElixirDB.Storage.SQLite.Adapter.delete_replication_job(adapter, id)
end
