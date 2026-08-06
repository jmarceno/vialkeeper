defmodule ElixirDB.Storage.SQLite.ReplicationJobs do
  @moduledoc """
  Replication-job SQL helpers for the Version 1 SQLite adapter.

  Owns list/put/delete against the `replication_jobs` table. Public API entry
  points still route through the adapter behaviour implementation.
  """

  alias ElixirDB.JSON.{Canonical, StrictDecoder}
  alias ElixirDB.Storage.SQLite.Connection

  @doc false
  def list(adapter), do: ElixirDB.Storage.SQLite.Adapter.list_replication_jobs(adapter)

  def put(adapter, request),
    do: ElixirDB.Storage.SQLite.Adapter.put_replication_job(adapter, request)

  def delete(adapter, id), do: ElixirDB.Storage.SQLite.Adapter.delete_replication_job(adapter, id)

  @doc """
  Lists all persisted replication jobs ordered by job id.
  """
  @spec list_all(Connection.handle()) :: {:ok, [map()]} | {:error, ElixirDB.Error.t()}
  def list_all(conn) do
    with {:ok, rows} <-
           Connection.query(
             conn,
             "SELECT job_id, definition_json, enabled, last_diagnostic_json FROM replication_jobs ORDER BY job_id"
           ) do
      {:ok,
       Enum.map(rows, fn [id, definition, enabled, diagnostic] ->
         %{
           job_id: id,
           definition: decode_json!(definition),
           enabled: enabled == 1,
           diagnostic: if(diagnostic, do: decode_json!(diagnostic))
         }
       end)}
    else
      {:error, reason} -> {:error, normalize_error(reason)}
    end
  end

  @doc """
  Upserts one replication job definition.
  """
  @spec upsert(Connection.handle(), map()) :: {:ok, map()} | {:error, ElixirDB.Error.t()}
  def upsert(conn, job) do
    id = job[:job_id] || job["job_id"]
    definition = job[:definition] || job["definition"] || job
    enabled = if(job[:enabled] || job["enabled"], do: 1, else: 0)

    with {:ok, definition_json} <- Canonical.encode(definition),
         :ok <-
           Connection.execute(
             conn,
             "INSERT INTO replication_jobs(job_id, definition_json, enabled, last_diagnostic_json) VALUES (?, ?, ?, NULL) ON CONFLICT(job_id) DO UPDATE SET definition_json=excluded.definition_json, enabled=excluded.enabled",
             [id, definition_json, enabled]
           ) do
      {:ok, %{job_id: id}}
    else
      {:error, reason} -> {:error, normalize_error(reason)}
    end
  end

  @doc """
  Deletes one replication job by id.
  """
  @spec delete_by_id(Connection.handle(), binary()) :: :ok | {:error, ElixirDB.Error.t()}
  def delete_by_id(conn, job_id) do
    case Connection.execute(conn, "DELETE FROM replication_jobs WHERE job_id = ?", [job_id]) do
      :ok -> :ok
      {:error, reason} -> {:error, normalize_error(reason)}
    end
  end

  defp decode_json!(json) do
    case StrictDecoder.decode(json) do
      {:ok, value} -> value
      _ -> nil
    end
  end

  defp normalize_error(%ElixirDB.Error{} = error), do: error

  defp normalize_error(reason),
    do: ElixirDB.Error.internal_error("SQLite operation failed", %{cause: inspect(reason)})
end
