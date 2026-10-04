defmodule VialKeeper.StorageAdapter.ConnectionTest do
  @moduledoc "Covers statement execution, reuse and parameter binding in the SQLite connection."
  # Call trace patterns are VM-global, so this module does not run async.
  use ExUnit.Case, async: false

  @moduletag :sqlite_physical

  alias VialKeeper.Storage.SQLite.{Adapter, Connection, Native}

  @counting_sql """
  WITH RECURSIVE counter(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM counter WHERE x < ?)
  SELECT x FROM counter
  """

  setup do
    {:ok, adapter} = Adapter.create(":memory:", %{storage_mode: :memory})
    on_exit(fn -> Adapter.close(adapter) end)
    %{conn: adapter.conn}
  end

  test "returns large results in order", %{conn: conn} do
    assert {:ok, rows} = Connection.query(conn, @counting_sql, [1_007])
    assert rows == Enum.map(1..1_007, &[&1])

    assert {:ok, [[1]]} = Connection.query(conn, @counting_sql, [1])
  end

  test "runs each statement in one driver call, reusing the cached statement", %{conn: conn} do
    calls =
      traced_calls({Native, :query, 3}, fn ->
        assert {:ok, [[1], [2], [3]]} = Connection.query(conn, @counting_sql, [3])
        assert {:ok, [[1], [2]]} = Connection.query(conn, @counting_sql, [2])
        assert {:ok, [[1], [2], [3], [4]]} = Connection.query(conn, @counting_sql, [4])
      end)

    assert [_, _, _] = calls
  end

  test "a statement that fails mid-run starts from the top on its next run", %{conn: conn} do
    # json() raises on the second row only, after the first row was stepped.
    sql = """
    SELECT x, json(CASE WHEN x = ? THEN 'not json' ELSE '1' END)
    FROM (SELECT 1 AS x UNION ALL SELECT 2 UNION ALL SELECT 3) ORDER BY x
    """

    assert {:error, reason} = Connection.query(conn, sql, [2])
    assert reason =~ "JSON"
    assert {:ok, [[1, "1"], [2, "1"], [3, "1"]]} = Connection.query(conn, sql, [0])
  end

  test "converts non-native parameters before binding", %{conn: conn} do
    sql = "SELECT ?, ?, ?, ?, typeof(?)"

    assert {:ok, [["2026-10-04", "2026-10-04T12:00:00", "active", "ab", "blob"]]} =
             Connection.query(conn, sql, [
               ~D[2026-10-04],
               ~U[2026-10-04 12:00:00Z],
               :active,
               [?a, "b"],
               {:blob, [<<1>>, <<2>>]}
             ])
  end

  test "reports a parameter count mismatch as an error", %{conn: conn} do
    assert {:error, reason} = Connection.query(conn, "SELECT ?, ?", [1])
    assert reason =~ "expected 2 arguments, got 1"
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
