defmodule VialKeeper.Storage.Turso.EngineTest do
  @moduledoc """
  Turso engine behaviour the storage backend relies on: MVCC visibility,
  schema constraints inside `BEGIN CONCURRENT`, row-level conflicts, durability
  across a killed writer process, and that no storage statement uses
  `RETURNING`.
  """
  use ExUnit.Case, async: false

  @moduletag :turso_physical

  alias VialKeeper.Storage.Services
  alias VialKeeper.Storage.SQLite.Connection
  alias VialKeeper.Storage.Turso.{Adapter, Driver}

  setup do
    {:ok, bundle} = VialKeeper.TempDatabase.create(prefix: "turso-engine")
    path = Adapter.artifact_path(bundle)
    {:ok, adapter} = Adapter.create(path, %{})

    on_exit(fn ->
      _ = Adapter.close(adapter)
      VialKeeper.TempDatabase.cleanup(bundle)
    end)

    %{adapter: adapter, path: path, bundle: bundle}
  end

  test "a committed row is visible to a new read transaction on another connection", %{
    adapter: adapter
  } do
    {:ok, writer} = Adapter.open_writer(adapter)
    {:ok, reader} = Adapter.open_reader(adapter)
    on_exit(fn -> Enum.each([writer, reader], &Adapter.close/1) end)

    assert :ok = Connection.exec(writer.conn, "BEGIN CONCURRENT")
    assert :ok = put_local(writer.conn, "visible", 1)
    assert :ok = Connection.exec(writer.conn, "COMMIT")

    assert :ok = Connection.exec(reader.conn, "BEGIN")
    assert {:ok, [["{}"]]} = local_value(reader.conn, "visible")
    assert :ok = Connection.exec(reader.conn, "COMMIT")
  end

  test "schema constraints hold inside BEGIN CONCURRENT", %{adapter: adapter} do
    put_document!(adapter, "doc", %{"n" => 1})
    conn = adapter.conn

    in_concurrent(conn, fn ->
      # STRICT typing
      assert {:error, _} =
               Connection.execute(
                 conn,
                 "INSERT INTO local_records(namespace, record_key, record_version, value_json) VALUES ('engine', 'strict', 'not a number', '{}')"
               )

      # CHECK
      assert {:error, _} = Connection.execute(conn, "UPDATE db_meta SET compaction_epoch = -1")

      # UNIQUE (document_id)
      assert {:error, _} =
               Connection.execute(
                 conn,
                 "INSERT INTO documents(document_id, winning_revision, winning_deleted, update_sequence) VALUES ('doc', '1-r', 1, 99)"
               )

      # ON DELETE RESTRICT: the document still has revisions and changes.
      assert {:error, _} =
               Connection.execute(conn, "DELETE FROM documents WHERE document_id = 'doc'")
    end)

    # ON DELETE CASCADE: attachment rows go with their revision.
    {:ok, [[doc_key, revision_id]]} =
      Connection.query(conn, "SELECT doc_key, revision_id FROM revisions LIMIT 1")

    in_concurrent(conn, fn ->
      assert :ok =
               Connection.execute(
                 conn,
                 "INSERT INTO revision_attachments(doc_key, revision_id, attachment_name, blob_digest, logical_size, content_type) VALUES (?, ?, 'a.txt', ?, 1, 'text/plain')",
                 [doc_key, revision_id, String.duplicate("a", 64)]
               )
    end)

    in_concurrent(conn, fn ->
      assert :ok = Connection.execute(conn, "DELETE FROM changes WHERE doc_key = ?", [doc_key])

      assert :ok =
               Connection.execute(conn, "DELETE FROM revisions WHERE doc_key = ?", [doc_key])
    end)

    assert {:ok, [[0]]} = Connection.query(conn, "SELECT count(*) FROM revision_attachments")

    # ON CONFLICT upsert
    in_concurrent(conn, fn ->
      assert :ok = put_local(conn, "upsert", 1)
      assert :ok = put_local(conn, "upsert", 2)
    end)

    assert {:ok, [[2]]} =
             Connection.query(
               conn,
               "SELECT record_version FROM local_records WHERE record_key = 'upsert'"
             )
  end

  test "two connections writing one row conflict; disjoint inserts do not", %{adapter: adapter} do
    put_document!(adapter, "shared", %{"n" => 1})
    {:ok, a} = Adapter.open_writer(adapter)
    {:ok, b} = Adapter.open_writer(adapter)
    on_exit(fn -> Enum.each([a, b], &Adapter.close/1) end)

    assert :ok = Connection.exec(a.conn, "BEGIN CONCURRENT")
    assert :ok = Connection.exec(b.conn, "BEGIN CONCURRENT")

    update =
      "UPDATE documents SET update_sequence = update_sequence + 1 WHERE document_id = 'shared'"

    assert :ok = Connection.execute(a.conn, update)

    conflict =
      with :ok <- Connection.execute(b.conn, update), do: Connection.exec(b.conn, "COMMIT")

    assert {:error, :write_conflict} = conflict
    assert Connection.take_conflict(b.conn)
    _ = Connection.exec(b.conn, "ROLLBACK")
    assert :ok = Connection.exec(a.conn, "COMMIT")

    assert :ok = Connection.exec(a.conn, "BEGIN CONCURRENT")
    assert :ok = Connection.exec(b.conn, "BEGIN CONCURRENT")
    assert :ok = insert_document_rows(a.conn, "left", 100)
    assert :ok = insert_document_rows(b.conn, "right", 101)
    assert :ok = Connection.exec(a.conn, "COMMIT")
    assert :ok = Connection.exec(b.conn, "COMMIT")

    assert {:ok, [[2]]} =
             Connection.query(
               adapter.conn,
               "SELECT count(*) FROM changes WHERE document_id IN ('left', 'right')"
             )
  end

  # Turso refuses DDL inside BEGIN CONCURRENT, which is why serial write
  # transactions (index creation among them) use BEGIN IMMEDIATE.
  test "DDL runs in serial write transactions, never inside BEGIN CONCURRENT", %{
    adapter: adapter
  } do
    assert :ok = Connection.exec(adapter.conn, "BEGIN CONCURRENT")

    assert {:error, message} =
             Connection.execute(adapter.conn, "CREATE TABLE ddl_probe (id INTEGER)")

    assert message =~ "BEGIN CONCURRENT"
    _ = Connection.exec(adapter.conn, "ROLLBACK")

    assert {:ok, %{"index_id" => _}} =
             Adapter.create_index(adapter, %{
               "name" => "by-n",
               "type" => "structured",
               "fields" => [%{"path" => "/n", "type" => "number"}]
             })
  end

  test "no storage statement sent to the Turso driver uses RETURNING", %{adapter: adapter} do
    Application.put_env(:vial_keeper, :sql_tap, {Driver, self()})
    on_exit(fn -> Application.delete_env(:vial_keeper, :sql_tap) end)

    put_document!(adapter, "tapped", %{"n" => 1})

    assert {:ok, _} =
             Services.apply_bulk_mutation(Adapter.to_context(adapter), %{
               operations: [
                 %{operation: :put, document_id: "bulk-a", body: %{"n" => 2}},
                 %{operation: :put, document_id: "bulk-b", body: %{"n" => 3}}
               ]
             })

    Application.delete_env(:vial_keeper, :sql_tap)
    statements = drain_tap([])
    assert Enum.any?(statements, &String.starts_with?(&1, "INSERT INTO documents"))
    refute Enum.any?(statements, &(&1 =~ ~r/\bRETURNING\b/i))
  end

  @tag :slow
  @tag timeout: 180_000
  test "rows committed before a killed writer's last commit survive reopen", %{
    adapter: adapter,
    path: path
  } do
    :ok = Adapter.close(adapter)
    holder = start_writer!(path)
    Process.sleep(500)
    System.cmd("kill", ["-9", Integer.to_string(holder.pid)], stderr_to_stdout: true)
    VialKeeper.Eventual.eventually(fn -> not alive?(holder.pid) end, timeout: 10_000)

    {:ok, reopened} = Adapter.open(path)
    on_exit(fn -> Adapter.close(reopened) end)

    {:ok, rows} =
      Connection.query(
        reopened.conn,
        "SELECT CAST(substr(record_key, 5) AS INTEGER) FROM local_records WHERE namespace = 'crash' ORDER BY 1"
      )

    committed = Enum.map(rows, &hd/1)
    assert length(committed) >= holder.acknowledged
    # One writer commits in order: whatever survived is a prefix.
    assert committed == Enum.to_list(1..length(committed))
  end

  defp in_concurrent(conn, fun) do
    assert :ok = Connection.exec(conn, "BEGIN CONCURRENT")
    fun.()

    case Connection.exec(conn, "COMMIT") do
      :ok -> :ok
      {:error, _} -> _ = Connection.exec(conn, "ROLLBACK")
    end
  end

  defp put_document!(adapter, id, body) do
    {:ok, _} =
      Services.apply_local_mutation(Adapter.to_context(adapter), %{
        operation: :put,
        document_id: id,
        body: body
      })
  end

  defp put_local(conn, key, version) do
    Connection.execute(
      conn,
      "INSERT INTO local_records(namespace, record_key, record_version, value_json) VALUES ('engine', ?, ?, '{}') ON CONFLICT(namespace, record_key) DO UPDATE SET record_version = excluded.record_version",
      [key, version]
    )
  end

  defp local_value(conn, key),
    do: Connection.query(conn, "SELECT value_json FROM local_records WHERE record_key = ?", [key])

  defp insert_document_rows(conn, id, sequence) do
    with :ok <-
           Connection.execute(
             conn,
             "INSERT INTO documents(document_id, winning_revision, winning_deleted, update_sequence) VALUES (?, '1-a', 1, ?)",
             [id, sequence]
           ),
         {:ok, doc_key} <- Connection.last_insert_rowid(conn),
         :ok <-
           Connection.execute(
             conn,
             "INSERT INTO revisions(doc_key, revision_id, generation, parent_revision, history_id, digest, deleted, body_json, body_term, insertion_sequence, is_leaf) VALUES (?, '1-a', 1, NULL, 'h', 'a', 1, NULL, NULL, 0, 1)",
             [doc_key]
           ) do
      Connection.execute(
        conn,
        "INSERT INTO changes(sequence, doc_key, document_id, winning_revision, winning_deleted, leaf_set_json, leaf_set_term, origin) VALUES (?, ?, ?, '1-a', 1, '[]', X'00', 'local')",
        [sequence, doc_key, id]
      )
    end
  end

  defp drain_tap(acc) do
    receive do
      {:sql_tap, sql} -> drain_tap([sql | acc])
    after
      0 -> Enum.reverse(acc)
    end
  end

  # A child OS process commits numbered rows one at a time through one Turso
  # writer connection and reports each acknowledged commit, until killed.
  defp start_writer!(path) do
    ready = path <> ".acked"
    _ = File.rm(ready)

    script = """
    alias VialKeeper.Storage.SQLite.Connection
    alias VialKeeper.Storage.Turso.Driver
    {:ok, conn} = Connection.open(#{inspect(path)}, mode: [:readwrite], driver: Driver)
    :ok = Driver.configure(conn, :disk)

    Enum.each(Stream.iterate(1, &(&1 + 1)), fn n ->
      :ok = Connection.exec(conn, "BEGIN CONCURRENT")
      :ok =
        Connection.execute(
          conn,
          "INSERT INTO local_records(namespace, record_key, record_version, value_json) VALUES ('crash', ?, 1, '{}')",
          ["row-\#{n}"]
        )
      :ok = Connection.exec(conn, "COMMIT")
      File.write!(#{inspect(ready)}, "\#{System.pid()} \#{n}")
    end)
    """

    mix = System.find_executable("mix") || flunk("mix is required for the crash test")

    _port =
      Port.open({:spawn_executable, mix}, [
        :binary,
        :exit_status,
        :stderr_to_stdout,
        args: ["run", "--no-start", "-e", script],
        cd: File.cwd!(),
        env: [{~c"MIX_ENV", ~c"test"}]
      ])

    VialKeeper.Eventual.eventually(fn -> File.exists?(ready) end,
      timeout: 120_000,
      message: "Turso writer did not start"
    )

    Process.sleep(300)
    [pid, acknowledged] = ready |> File.read!() |> String.split() |> Enum.map(&String.to_integer/1)
    File.rm!(ready)
    %{pid: pid, acknowledged: acknowledged}
  end

  defp alive?(pid) do
    match?({_, 0}, System.cmd("kill", ["-0", Integer.to_string(pid)], stderr_to_stdout: true))
  end
end
