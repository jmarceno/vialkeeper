defmodule VialKeeper.Storage.SQLite.Driver.Rusqlite do
  @moduledoc """
  SQLite engine driver: the `native/vial_sqlite` NIF (Rust over `rusqlite`
  with bundled SQLite) plus SQLite's WAL pragmas and `BEGIN IMMEDIATE`
  writes. One writer runs at a time; extra writer connections wait on the
  write lock.
  """
  use VialKeeper.Storage.SQLite.Driver, native: VialKeeper.Storage.SQLite.Native

  alias VialKeeper.Storage.SQLite.{Capabilities, Connection}

  @impl true
  def engine, do: "sqlite"

  @impl true
  def artifact_name, do: "database.sqlite3"

  @impl true
  def sidecar_suffixes, do: ["-wal", "-shm"]

  @impl true
  def begin_concurrent, do: "BEGIN IMMEDIATE"

  # `BEGIN IMMEDIATE` waits for the write lock, so SQLite never conflicts.
  @impl true
  def serial_conflict_retries, do: 0

  # 16 384 pages × 4 KiB = 64 MiB, matching the page cache. The SQLite default
  # (1 000 pages, 4 MiB) fsyncs the WAL inside ordinary commits on spinning
  # disk and produces the bulk-write p95 tail. Close still TRUNCATEs. App-crash
  # durability is unchanged under NORMAL; a larger checkpoint interval only
  # widens the already-documented power/OS-loss window.
  @wal_autocheckpoint_pages 16_384

  @impl true
  def configure(conn, storage_mode) do
    with :ok <- Connection.execute(conn, journal_mode_sql(storage_mode)),
         :ok <- Connection.execute(conn, "PRAGMA synchronous = NORMAL"),
         :ok <- wal_autocheckpoint(conn, storage_mode),
         :ok <- Connection.execute(conn, "PRAGMA foreign_keys = ON"),
         :ok <- Connection.execute(conn, "PRAGMA locking_mode = NORMAL"),
         :ok <- Connection.execute(conn, "PRAGMA trusted_schema = OFF"),
         :ok <- Connection.execute(conn, cache_size_sql()) do
      Connection.execute(conn, temp_store_sql())
    end
  end

  @impl true
  def configure_reader(conn) do
    with :ok <- Connection.execute(conn, "PRAGMA query_only = ON"),
         :ok <- Connection.execute(conn, "PRAGMA foreign_keys = ON"),
         :ok <- Connection.execute(conn, "PRAGMA locking_mode = NORMAL"),
         :ok <- Connection.execute(conn, "PRAGMA trusted_schema = OFF"),
         :ok <- Connection.execute(conn, cache_size_sql()) do
      Connection.execute(conn, temp_store_sql())
    end
  end

  @impl true
  def valid_pragmas?(conn, storage_mode) do
    with {:ok, [[journal_mode]]} <- Connection.pragma(conn, "journal_mode"),
         {:ok, [[synchronous]]} <- Connection.pragma(conn, "synchronous"),
         {:ok, [[locking_mode]]} <- Connection.pragma(conn, "locking_mode"),
         {:ok, [[trusted_schema]]} <- Connection.pragma(conn, "trusted_schema") do
      String.downcase(to_string(journal_mode)) == expected_journal_mode(storage_mode) and
        synchronous in [1, "1"] and String.downcase(to_string(locking_mode)) == "normal" and
        trusted_schema in [0, "0"]
    else
      _ -> false
    end
  end

  # The immutable URI reads the file as it is and creates no sidecars.
  @impl true
  def open_closed_artifact(path) do
    Connection.open("file:" <> URI.encode(path) <> "?immutable=1", mode: [:readonly])
  end

  @impl true
  def release_closed_artifact(_path), do: :ok

  @doc """
  `max_writers` is the test-only `:sqlite_max_writers` setting (default 1);
  extra SQLite writers still serialize through `BEGIN IMMEDIATE`.
  """
  @impl true
  def writer_capabilities(storage_mode) do
    %{
      max_writers: Application.get_env(:vial_keeper, :sqlite_max_writers, 1),
      sequence_persistence: if(storage_mode == :disk, do: :separate_connection, else: :none)
    }
  end

  @impl true
  def validate_capabilities!, do: Capabilities.validate!()

  @impl true
  def capabilities_report, do: Capabilities.report()

  defp journal_mode_sql(:disk), do: "PRAGMA journal_mode = WAL"
  defp journal_mode_sql(:memory), do: "PRAGMA journal_mode = MEMORY"

  defp expected_journal_mode(:disk), do: "wal"
  defp expected_journal_mode(:memory), do: "memory"

  defp wal_autocheckpoint(conn, :disk),
    do: Connection.execute(conn, "PRAGMA wal_autocheckpoint = #{@wal_autocheckpoint_pages}")

  defp wal_autocheckpoint(_conn, :memory), do: :ok

  # 64 MiB page cache. Negative values are KiB, independent of page size.
  defp cache_size_sql, do: "PRAGMA cache_size = -65536"

  defp temp_store_sql, do: "PRAGMA temp_store = MEMORY"
end
