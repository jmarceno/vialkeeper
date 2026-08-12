defmodule ElixirDB.EndToEnd.HotJournalRecoveryTest do
  @moduledoc """
  Crash recovery with a hot rollback journal (DELETE mode).

  Limitation (documented): fully mid-mutation SIGKILL of an in-BEAM Adapter owner
  during `apply_local_mutation` is not exercised here — that path shares the test
  VM and is hard to interrupt atomically without racing the commit. Instead this
  test SIGKILLs a child OS process that opened the same file via
  `ElixirDB.Storage.SQLite.Adapter.open/1` and held an in-flight SQLite write,
  leaving a real `-journal` beside an ElixirDB-shaped database. Catalog reopen
  must recover, preserve the preimage committed document, then prove portability.
  """
  use ExUnit.Case, async: false

  @moduletag :sqlite_physical
  @moduletag :integration

  alias ElixirDB.Runtime.DatabaseCatalog

  @tag :slow
  test "SIGKILL of Adapter-holding child leaves -journal; reopen recovers; then portable" do
    root = ElixirDB.Config.database_root()
    File.mkdir_p!(root)

    rel = "e2e-hot-journal-#{System.unique_integer([:positive])}.elixirdb"
    abs = Path.join(root, rel)
    sqlite = ElixirDB.TempDatabase.sqlite_path(abs)
    journal = sqlite <> "-journal"
    copy_rel = String.replace_suffix(rel, ".elixirdb", "-copy.elixirdb")
    copy_abs = Path.join(root, copy_rel)

    for path <- [abs, copy_abs] do
      ElixirDB.TempDatabase.cleanup(path)
    end

    assert {:ok, identity} = DatabaseCatalog.create(rel)
    uuid = identity.database_uuid

    on_exit(fn ->
      _ = DatabaseCatalog.close(uuid)
      _ = DatabaseCatalog.unregister(uuid)
      ElixirDB.TempDatabase.cleanup(abs)
      ElixirDB.TempDatabase.cleanup(copy_abs)
    end)

    assert {:ok, %{revision: revision}} =
             ElixirDB.Documents.put(uuid, %{id: "durable", body: %{"committed" => true}})

    assert :ok = DatabaseCatalog.close(uuid)

    # Project default is rollback-journal DELETE — no WAL sidecars when closed.
    refute File.exists?(sqlite <> "-wal")
    refute File.exists?(sqlite <> "-shm")
    refute File.exists?(journal)

    holder = hold_hot_journal_via_adapter!(sqlite)
    on_exit(fn -> reap_holder(holder) end)

    assert File.exists?(journal)
    assert File.stat!(journal).size > 0

    # Brutal OS kill — no SQLite cleanup / rollback from the writer process.
    if holder_alive?(holder.pid) do
      _ = System.cmd("kill", ["-9", Integer.to_string(holder.pid)], stderr_to_stdout: true)
    end

    wait_until(fn -> not holder_alive?(holder.pid) end, "writer process did not die")
    assert File.exists?(journal), "hot -journal must survive SIGKILL of the writer"

    # Reopen recovers through SQLite hot-journal playback.
    assert {:ok, _} = DatabaseCatalog.open(uuid)

    assert {:ok, %{revision: ^revision, body: %{"committed" => true}}} =
             ElixirDB.Documents.get(uuid, %{id: "durable"})

    # In-flight probe mutation from the killed child must not leave a half-applied
    # ElixirDB document (probe uses a side table / raw SQL, not a document id).
    assert {:error, %ElixirDB.Error{code: :document_not_found}} =
             ElixirDB.Documents.get(uuid, %{id: "in-flight"})

    assert {:ok, %{ok: true}} =
             DatabaseCatalog.command(uuid, {:command, :integrity_check, %{}})

    # Touched write clears any empty leftover journal file after recovery.
    assert {:ok, %{revision: post}} =
             ElixirDB.Documents.put(uuid, %{
               id: "after-recovery",
               body: %{"recovered" => true}
             })

    assert :ok = DatabaseCatalog.close(uuid)
    refute File.exists?(journal)
    refute File.exists?(sqlite <> "-wal")

    # Offline-portable: ordinary OS copy of the closed bundle directory.
    File.cp_r!(abs, copy_abs)
    assert :ok = DatabaseCatalog.unregister(uuid)
    assert {:ok, restored} = DatabaseCatalog.register(copy_rel)
    assert restored.database_uuid == uuid

    assert {:ok, %{revision: ^revision, body: %{"committed" => true}}} =
             ElixirDB.Documents.get(uuid, %{id: "durable"})

    assert {:ok, %{revision: ^post, body: %{"recovered" => true}}} =
             ElixirDB.Documents.get(uuid, %{id: "after-recovery"})

    assert {:ok, %{ok: true}} =
             DatabaseCatalog.command(uuid, {:command, :integrity_check, %{}})
  end

  defp hold_hot_journal_via_adapter!(sqlite_path) do
    ready = sqlite_path <> ".ready"
    pid_file = sqlite_path <> ".holder_pid"
    _ = File.rm(ready)
    _ = File.rm(pid_file)

    # Child OS process opens via Adapter.open (same entry ElixirDB uses), begins a
    # write, signals readiness while -journal exists, then sleeps until SIGKILL.
    script = """
    ready = #{inspect(ready)}
    pid_file = #{inspect(pid_file)}
    path = #{inspect(sqlite_path)}
    File.write!(pid_file, System.pid())
    {:ok, _} = Application.ensure_all_started(:elixir_db)
    {:ok, adapter} = ElixirDB.Storage.SQLite.Adapter.open(path)
    :ok = ElixirDB.Storage.SQLite.Connection.execute(adapter.conn, "BEGIN IMMEDIATE")
    :ok =
      ElixirDB.Storage.SQLite.Connection.execute(
        adapter.conn,
        "CREATE TABLE IF NOT EXISTS __hot_journal_probe(x INTEGER)"
      )
    :ok =
      ElixirDB.Storage.SQLite.Connection.execute(
        adapter.conn,
        "INSERT INTO __hot_journal_probe VALUES (1)"
      )
    File.write!(ready, "ready")
    Process.sleep(3_600_000)
    """

    mix = System.find_executable("mix") || flunk("mix is required for hot-journal e2e")

    port =
      Port.open({:spawn_executable, mix}, [
        :binary,
        :exit_status,
        :stderr_to_stdout,
        args: ["run", "--no-start", "-e", script],
        cd: File.cwd!(),
        env: [{~c"MIX_ENV", ~c"test"}]
      ])

    wait_until(
      fn -> File.exists?(ready) and File.exists?(pid_file) end,
      "hot journal holder not ready"
    )

    pid = String.trim(File.read!(pid_file)) |> String.to_integer()
    assert File.exists?(sqlite_path <> "-journal")

    %{port: port, pid: pid, ready: ready, pid_file: pid_file}
  end

  defp reap_holder(%{pid: pid, ready: ready, pid_file: pid_file} = holder) do
    if holder_alive?(pid) do
      _ = System.cmd("kill", ["-9", Integer.to_string(pid)], stderr_to_stdout: true)
    end

    _ = File.rm(ready)
    _ = File.rm(pid_file)

    if match?(%{port: port} when is_port(port), holder) do
      try do
        Port.close(holder.port)
      rescue
        _ -> :ok
      catch
        :exit, _ -> :ok
      end
    end

    :ok
  rescue
    _ -> :ok
  end

  defp holder_alive?(pid) when is_integer(pid) do
    case System.cmd("kill", ["-0", Integer.to_string(pid)], stderr_to_stdout: true) do
      {_, 0} -> true
      _ -> false
    end
  end

  defp wait_until(fun, message) do
    ElixirDB.Eventual.eventually(fun, timeout: 15_000, message: message)
  end
end
