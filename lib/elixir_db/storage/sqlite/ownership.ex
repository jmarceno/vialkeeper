defmodule ElixirDB.Storage.SQLite.Ownership do
  @moduledoc """
  SQLite single-owner lease using a companion database and BEGIN EXCLUSIVE.

  This is the physical ownership implementation. Runtime code starts ownership
  through `ElixirDB.Runtime.Ownership`, never by constructing lease paths or
  issuing SQLite transaction text.
  """
  use GenServer

  alias ElixirDB.Storage.SQLite.Connection

  @doc "Starts ownership for the SQLite data artifact at `database_path`."
  def start_link(database_path), do: GenServer.start_link(__MODULE__, database_path)

  @impl true
  def init(database_path) do
    lease_path = database_path <> ".lease"

    case Connection.open(lease_path) do
      {:ok, conn} ->
        case acquire(conn) do
          :ok ->
            {:ok, %{conn: conn, path: lease_path}}

          {:error, reason} ->
            _ = Connection.close(conn)
            unavailable(reason)
        end

      {:error, reason} ->
        unavailable(reason)
    end
  end

  @impl true
  def terminate(_reason, %{conn: conn}) do
    _ = Connection.execute(conn, "ROLLBACK")
    _ = Connection.close(conn)
    :ok
  end

  defp set_busy_timeout(conn) do
    Exqlite.Sqlite3.set_busy_timeout(conn, 0)
  end

  defp acquire(conn) do
    with :ok <- set_busy_timeout(conn) do
      Connection.execute(conn, "BEGIN EXCLUSIVE")
    end
  end

  defp unavailable(reason) do
    {:stop,
     ElixirDB.Error.database_in_use("database ownership lease is unavailable", %{
       cause: inspect(reason)
     })}
  end
end
