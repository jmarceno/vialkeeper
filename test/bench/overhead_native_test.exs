Code.require_file("../../bench/overhead/native.exs", __DIR__)

defmodule VialKeeper.Bench.OverheadNativeTest do
  use ExUnit.Case, async: true

  alias VialKeeper.Benchmarks.Overhead.Native
  alias VialKeeper.Storage.SQLite.Native, as: Driver

  # Compiles sqlite3.c once (cached under _build), which takes about a minute.
  @moduletag :slow
  @moduletag timeout: 600_000

  # SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE
  @open_flags 0x6

  setup_all do
    %{build: Native.build!()}
  end

  setup %{build: build} do
    port = Native.start(build["path"])
    on_exit(fn -> Native.stop(port) end)
    %{port: port}
  end

  test "runs the same SQLite engine as the driver", %{port: port} do
    {:ok, conn} = Driver.open(":memory:", @open_flags)
    {:ok, image} = Driver.serialize(conn)
    :ok = Native.open_image(port, image)

    {:ok, rows} = Driver.query(conn, "PRAGMA compile_options", [])
    driver_options = Enum.map(rows, &List.first/1)
    native_options = Native.scalar(port, "PRAGMA compile_options")
    without_compiler = &Enum.reject(&1, fn option -> String.starts_with?(option, "COMPILER=") end)

    assert without_compiler.(native_options) == without_compiler.(driver_options)

    {:ok, [[version, source_id]]} =
      Driver.query(conn, "SELECT sqlite_version(), sqlite_source_id()", [])

    assert Native.scalar(port, "SELECT sqlite_version()") == [version]
    assert Native.scalar(port, "SELECT sqlite_source_id()") == [source_id]
    :ok = Driver.close(conn)
  end

  test "restores an image and replays every parameter type", %{port: port} do
    {:ok, conn} = Driver.open(":memory:", @open_flags)
    :ok = Driver.execute(conn, "CREATE TABLE t(i INTEGER, r REAL, s TEXT, b BLOB, n)")
    :ok = Driver.execute(conn, "INSERT INTO t VALUES (1, 1.5, 'seed', x'00', NULL)")
    {:ok, image} = Driver.serialize(conn)
    :ok = Driver.close(conn)

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
end
