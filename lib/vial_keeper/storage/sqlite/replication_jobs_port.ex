defmodule VialKeeper.Storage.SQLite.ReplicationJobsPort do
  @moduledoc "SQLite implementation of the durable replication-jobs port."

  @behaviour VialKeeper.Storage.Ports.ReplicationJobs

  alias VialKeeper.Storage.BackendContext
  alias VialKeeper.Storage.Ports.Errors
  alias VialKeeper.Storage.SQLite.{Context, ReplicationJobs}

  @impl true
  def list(%BackendContext{} = context) do
    with {:ok, adapter} <- Context.unwrap(context) do
      Errors.wrap(ReplicationJobs.list_all(adapter.conn))
    end
  end

  @impl true
  def put(%BackendContext{} = context, request) when is_map(request) do
    with {:ok, adapter} <- Context.unwrap(context) do
      Errors.wrap(ReplicationJobs.upsert(adapter.conn, request))
    end
  end

  @impl true
  def delete(%BackendContext{} = context, job_id) when is_binary(job_id) do
    with {:ok, adapter} <- Context.unwrap(context) do
      Errors.wrap(ReplicationJobs.delete_by_id(adapter.conn, job_id))
    end
  end
end
