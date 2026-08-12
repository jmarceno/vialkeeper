defmodule ElixirDB.Storage.SQLite.Connection do
  @moduledoc "Private SQLite connection and statement execution primitives."
  alias ElixirDB.Storage.SQLite.Statements
  alias Exqlite.Sqlite3

  @type handle :: reference()

  @spec open(binary(), keyword()) :: {:ok, handle()} | {:error, term()}
  def open(path, opts \\ []) do
    Sqlite3.open(path, opts)
  end

  @spec close(handle() | nil) :: :ok | {:error, term()}
  def close(nil), do: :ok

  def close(handle) do
    # Exqlite documents cancel/1 as part of connection teardown: it wakes a
    # connection blocked in SQLite's busy handler before statements or the
    # database handle are finalized. This matters for short-lived contenders
    # such as the file-lease process, which must not leave a journal/lock
    # behind for the next owner.
    _ = Sqlite3.cancel(handle)
    Statements.release_all(handle)
    Sqlite3.close(handle)
  end

  @spec execute(handle(), iodata(), list()) :: :ok | {:error, term()}
  def execute(conn, sql, params \\ []) do
    case run(conn, sql, params, false) do
      {:ok, _rows} -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  @spec query(handle(), iodata(), list()) :: {:ok, [list()]} | {:error, term()}
  def query(conn, sql, params \\ []), do: run(conn, sql, params, true)

  @spec pragma(handle(), binary()) :: {:ok, [list()]} | {:error, term()}
  def pragma(conn, statement), do: query(conn, "PRAGMA " <> statement)

  defp run(conn, sql, params, collect_rows) do
    sql = IO.iodata_to_binary(sql)

    with {:ok, statement} <- Statements.checkout(conn, sql),
         :ok <- Sqlite3.bind(statement, params) do
      step(conn, statement, collect_rows, [])
    end
  end

  defp step(conn, statement, collect_rows, rows) do
    case Sqlite3.step(conn, statement) do
      {:row, row} when collect_rows -> step(conn, statement, collect_rows, [row | rows])
      {:row, _row} -> step(conn, statement, collect_rows, rows)
      :done -> {:ok, Enum.reverse(rows)}
      {:error, reason} -> {:error, reason}
      other -> {:error, other}
    end
  end
end
