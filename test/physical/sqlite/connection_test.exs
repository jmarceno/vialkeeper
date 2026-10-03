defmodule VialKeeper.StorageAdapter.ConnectionTest do
  @moduledoc "Covers statement reuse and chunked row fetching in the SQLite connection."
  # Call trace patterns are VM-global, so this module does not run async.
  use ExUnit.Case, async: false

  @moduletag :sqlite_physical

  alias Exqlite.Sqlite3
  alias VialKeeper.Storage.SQLite.{Adapter, Connection, Statements}

  @counting_sql """
  WITH RECURSIVE counter(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM counter WHERE x < ?)
  SELECT x FROM counter
  """

  setup do
    {:ok, adapter} = Adapter.create(":memory:", %{storage_mode: :memory})
    on_exit(fn -> Adapter.close(adapter) end)
    %{conn: adapter.conn}
  end

  test "returns results spanning several fetch chunks in order", %{conn: conn} do
    count = Connection.fetch_chunk_rows() * 2 + 7

    assert {:ok, rows} = Connection.query(conn, @counting_sql, [count])
    assert rows == Enum.map(1..count, &[&1])

    assert {:ok, [[1]]} = Connection.query(conn, @counting_sql, [1])
    assert {:ok, rows} = Connection.query(conn, @counting_sql, [Connection.fetch_chunk_rows()])
    assert length(rows) == Connection.fetch_chunk_rows()
  end

  test "reuses a finished cached statement without a reset call", %{conn: conn} do
    # The statement cache is per process, so the traced process warms its own.
    resets =
      traced_calls({Sqlite3, :reset, 1}, fn ->
        assert {:ok, [[1], [2], [3]]} = Connection.query(conn, @counting_sql, [3])
        assert {:ok, [[1], [2]]} = Connection.query(conn, @counting_sql, [2])
        assert {:ok, [[1], [2], [3], [4]]} = Connection.query(conn, @counting_sql, [4])
      end)

    assert resets == []
  end

  test "resets a statement abandoned mid-run before the connection is used again", %{conn: conn} do
    sql = "SELECT x FROM (SELECT 1 AS x UNION ALL SELECT 2 UNION ALL SELECT 3) ORDER BY x"

    assert {:ok, statement} = Statements.checkout(conn, sql)
    assert :ok = Sqlite3.bind(statement, [])
    assert {:row, [1]} = Sqlite3.step(conn, statement)

    # The run above never checked in; the next run must start from the top.
    assert {:ok, [[1], [2], [3]]} = Connection.query(conn, sql, [])
    assert {:ok, [[1], [2], [3]]} = Connection.query(conn, sql, [])
  end

  test "a failed statement leaves the cached statement reusable", %{conn: conn} do
    assert :ok = Connection.execute(conn, "CREATE TABLE unique_values(v INTEGER UNIQUE)")
    insert = "INSERT INTO unique_values(v) VALUES (?)"

    assert :ok = Connection.execute(conn, insert, [1])
    assert {:error, _reason} = Connection.execute(conn, insert, [1])
    assert :ok = Connection.execute(conn, insert, [2])

    assert {:ok, [[1], [2]]} =
             Connection.query(conn, "SELECT v FROM unique_values ORDER BY v", [])
  end

  defp traced_calls(mfa, fun) do
    {_result, calls} = VialKeeper.CallTrace.run([mfa], fun)
    calls
  end
end
