defmodule VialKeeper.Storage.Turso.ConnectionTest do
  @moduledoc """
  Covers statement execution, parameter binding, schema atomicity and memory
  mode on the Turso driver, which reimplements the SQLite NIF's binding and
  row encoding.
  """
  use ExUnit.Case, async: true

  @moduletag :turso_physical

  alias VialKeeper.JSON.Canonical
  alias VialKeeper.Storage.SQLite.{Connection, Schema}
  alias VialKeeper.Storage.Turso.{Adapter, Driver}

  @counting_sql """
  WITH RECURSIVE counter(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM counter WHERE x < ?)
  SELECT x FROM counter
  """

  setup do
    {:ok, bundle} = VialKeeper.TempDatabase.create(prefix: "turso-connection")
    {:ok, adapter} = Adapter.create(Adapter.artifact_path(bundle), %{})

    on_exit(fn ->
      _ = Adapter.close(adapter)
      VialKeeper.TempDatabase.cleanup(bundle)
    end)

    %{conn: adapter.conn, bundle: bundle}
  end

  test "returns large results in order", %{conn: conn} do
    assert {:ok, rows} = Connection.query(conn, @counting_sql, [1_007])
    assert rows == Enum.map(1..1_007, &[&1])

    assert {:ok, [[1]]} = Connection.query(conn, @counting_sql, [1])
  end

  test "a statement that fails mid-run starts from the top on its next run", %{conn: conn} do
    sql = """
    SELECT x, json(CASE WHEN x = ? THEN 'not json' ELSE '1' END)
    FROM (SELECT 1 AS x UNION ALL SELECT 2 UNION ALL SELECT 3) ORDER BY x
    """

    assert {:error, reason} = Connection.query(conn, sql, [2])
    assert is_binary(reason)
    assert {:ok, [[1, "1"], [2, "1"], [3, "1"]]} = Connection.query(conn, sql, [0])
  end

  test "binds and returns every value type like the SQLite driver", %{conn: conn} do
    sql = "SELECT ?, ?, ?, ?, typeof(?), ?, ?, typeof(?)"

    assert {:ok, [["2026-10-04", "2026-10-04T12:00:00", "active", "ab", "blob", 1.5, nil, "null"]]} =
             Connection.query(conn, sql, [
               ~D[2026-10-04],
               ~U[2026-10-04 12:00:00Z],
               :active,
               [?a, "b"],
               {:blob, [<<1>>, <<2>>]},
               1.5,
               nil,
               nil
             ])

    assert {:ok, [[<<1, 2>>]]} = Connection.query(conn, "SELECT ?", [{:blob, <<1, 2>>}])
  end

  test "a failed statement leaves the cached statement reusable", %{conn: conn} do
    assert :ok = Connection.execute(conn, "CREATE TABLE unique_values(v INTEGER UNIQUE)")
    insert = "INSERT INTO unique_values(v) VALUES (?)"

    assert :ok = Connection.execute(conn, insert, [1])
    assert {:error, reason} = Connection.execute(conn, insert, [1])
    refute reason == :write_conflict
    assert :ok = Connection.execute(conn, insert, [2])

    assert {:ok, [[1], [2]]} =
             Connection.query(conn, "SELECT v FROM unique_values ORDER BY v", [])
  end

  test "failed initialization rolls back the schema and metadata together", %{bundle: bundle} do
    path = Path.join(bundle, "atomic.db")
    {:ok, conn} = Connection.open(path, driver: Driver)
    on_exit(fn -> _ = Connection.close(conn) end)

    :ok = Schema.configure(conn)
    config_json = Canonical.encode!(VialKeeper.Config.defaults())

    assert {:error, %VialKeeper.Error{code: :internal_error}} =
             Schema.create(conn, VialKeeper.UUID.v4(), config_json,
               database_kind: :ordinary,
               initial_derived_view: %{}
             )

    assert {:ok, []} =
             Connection.query(
               conn,
               "SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'db_meta'"
             )
  end

  test "in-memory databases are isolated and single-writer" do
    assert {:ok, first} = Adapter.create(":memory:", %{storage_mode: :memory})
    assert {:ok, second} = Adapter.create(":memory:", %{storage_mode: :memory})

    on_exit(fn ->
      Adapter.close(first)
      Adapter.close(second)
    end)

    assert {:ok, %{revision: revision}} =
             Adapter.apply_local_mutation(first, %{
               operation: :put,
               document_id: "memory-only",
               body: %{"value" => 1}
             })

    assert {:ok, %{revision: ^revision}} =
             Adapter.get_document(first, %{document_id: "memory-only"})

    assert {:error, %VialKeeper.Error{code: :document_not_found}} =
             Adapter.get_document(second, %{document_id: "memory-only"})

    assert {:error, :unsupported_readers} = Adapter.open_reader(first)
    assert %{max_writers: 1, sequence_persistence: :none} = Adapter.writer_capabilities(first)
  end
end
