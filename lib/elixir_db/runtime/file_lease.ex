defmodule ElixirDB.Runtime.FileLease do
  @moduledoc false
  use GenServer
  alias ElixirDB.Storage.SQLite.Connection

  def start_link(path), do: GenServer.start_link(__MODULE__, path)

  @impl true
  def init(database_path) do
    lease_path = database_path <> ".lease"

    with {:ok, conn} <- Connection.open(lease_path),
         :ok <- set_busy_timeout(conn),
         :ok <- Connection.execute(conn, "BEGIN EXCLUSIVE") do
      {:ok, %{conn: conn, path: lease_path}}
    else
      {:error, reason} ->
        {:stop,
         ElixirDB.Error.database_in_use("database ownership lease is unavailable", %{
           cause: inspect(reason)
         })}
    end
  end

  @impl true
  def terminate(_reason, %{conn: conn}) do
    _ = Connection.execute(conn, "ROLLBACK")
    _ = Connection.close(conn)
    :ok
  end

  defp set_busy_timeout(conn) do
    case Exqlite.Sqlite3.set_busy_timeout(conn, 0) do
      :ok -> :ok
      {:error, reason} -> {:error, reason}
    end
  end
end
