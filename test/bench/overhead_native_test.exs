Code.require_file("../../bench/overhead/native.exs", __DIR__)

defmodule VialKeeper.Bench.OverheadNativeTest do
  use ExUnit.Case, async: true

  alias Exqlite.Sqlite3
  alias VialKeeper.Benchmarks.Overhead.Native

  # Compiles sqlite3.c once (cached under _build), which takes about a minute.
  @moduletag :slow
  @moduletag timeout: 600_000

  setup_all do
    %{build: Native.build!()}
  end

  setup %{build: build} do
    port = Native.start(build["path"])
    on_exit(fn -> Native.stop(port) end)
    %{port: port}
  end

  test "runs the same SQLite engine as ExQLite", %{port: port} do
    {:ok, conn} = Sqlite3.open(":memory:")
    {:ok, image} = Sqlite3.serialize(conn, "main")
    :ok = Native.open_image(port, image)

    {:ok, statement} = Sqlite3.prepare(conn, "PRAGMA compile_options")
    exqlite_options = collect(conn, statement) |> Enum.map(&List.first/1)
    native_options = Native.scalar(port, "PRAGMA compile_options")
    without_compiler = &Enum.reject(&1, fn option -> String.starts_with?(option, "COMPILER=") end)

    assert without_compiler.(native_options) == without_compiler.(exqlite_options)
    Sqlite3.close(conn)
  end

  test "restores an image and replays every parameter type", %{port: port} do
    {:ok, conn} = Sqlite3.open(":memory:")
    :ok = Sqlite3.execute(conn, "CREATE TABLE t(i INTEGER, r REAL, s TEXT, b BLOB, n)")
    :ok = Sqlite3.execute(conn, "INSERT INTO t VALUES (1, 1.5, 'seed', x'00', NULL)")
    {:ok, image} = Sqlite3.serialize(conn, "main")
    Sqlite3.close(conn)

    :ok = Native.open_image(port, image)
    :ok = Native.prepare(port, 0, "INSERT INTO t VALUES (?, ?, ?, ?, ?)")
    :ok = Native.prepare(port, 1, "SELECT * FROM t WHERE i >= ?")

    insert = {:stmt, false, 0, [-9_007_199_254_740_993, 2.25, "text", {:blob, <<1, 2, 3>>}, nil]}

    result =
      [{:exec, "BEGIN IMMEDIATE"}, insert, {:exec, "COMMIT"}, {:stmt, true, 1, [-1.0e20]}]
      |> Native.encode_run()
      |> then(&Native.run(port, &1))

    assert %{rows: 2, statements: 4} = result
    assert result.ns > 0
    assert result.vm_steps > 0

    assert Native.scalar(
             port,
             "SELECT i || '|' || r || '|' || s || '|' || hex(b) FROM t ORDER BY i"
           ) ==
             ["-9007199254740993|2.25|text|010203", "1|1.5|seed|00"]
  end

  defp collect(conn, statement) do
    case Sqlite3.step(conn, statement) do
      {:row, row} -> [row | collect(conn, statement)]
      :done -> []
    end
  end
end
