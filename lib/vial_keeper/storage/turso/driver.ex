defmodule VialKeeper.Storage.Turso.Driver do
  @moduledoc """
  Turso engine driver: the `native/vial_turso` NIF plus Turso's MVCC journal
  and `BEGIN CONCURRENT` writes.

  Writer connections commit concurrently and conflict per row: a write that
  touches a row another open transaction changed gets `:write_conflict` and
  its transaction is already rolled back. The shared SQL modules and
  transaction code drive it through `VialKeeper.Storage.SQLite.Driver`.
  """
  use VialKeeper.Storage.SQLite.Driver, native: VialKeeper.Storage.Turso.Native

  alias VialKeeper.Storage.SQLite.Connection
  alias VialKeeper.Storage.Turso.Capabilities

  @max_writers 16

  @impl true
  def engine, do: "turso"

  @impl true
  def artifact_name, do: "turso.db"

  # `-log` is the MVCC logical log; `-wal` the page WAL a checkpoint writes
  # through. A TRUNCATE checkpoint leaves both empty.
  @impl true
  def sidecar_suffixes, do: ["-wal", "-log"]

  @impl true
  def begin_concurrent, do: "BEGIN CONCURRENT"

  # Serial commands use `BEGIN IMMEDIATE` (Turso allows DDL only outside
  # `BEGIN CONCURRENT`) and run alone behind the writer pool's barrier; a
  # conflict can only come from the sequence ledger's reservation write.
  @impl true
  def serial_conflict_retries, do: 8

  @impl true
  def configure(conn, _storage_mode) do
    with :ok <- Connection.execute(conn, "PRAGMA journal_mode = 'mvcc'"),
         :ok <- Connection.execute(conn, "PRAGMA synchronous = NORMAL"),
         :ok <- Connection.execute(conn, "PRAGMA foreign_keys = ON") do
      require_mvcc(conn)
    end
  end

  @impl true
  def configure_reader(conn) do
    with :ok <- Connection.execute(conn, "PRAGMA query_only = 1"),
         :ok <- Connection.execute(conn, "PRAGMA foreign_keys = ON") do
      require_mvcc(conn)
    end
  end

  @impl true
  def valid_pragmas?(conn, _storage_mode) do
    match?({:ok, [["mvcc"]]}, Connection.pragma(conn, "journal_mode")) and
      match?({:ok, [[1]]}, Connection.pragma(conn, "synchronous")) and
      match?({:ok, [[1]]}, Connection.pragma(conn, "foreign_keys"))
  end

  # Turso has no immutable mode: the closed file opens normally with
  # query_only set, and the empty sidecars the open created are removed after
  # the close.
  @impl true
  def open_closed_artifact(path) do
    with {:ok, conn} <- Connection.open(path, mode: [:readwrite], driver: __MODULE__) do
      case configure_reader(conn) do
        :ok ->
          {:ok, conn}

        {:error, reason} ->
          _ = Connection.close(conn)
          release_closed_artifact(path)
          {:error, reason}
      end
    end
  end

  @impl true
  def release_closed_artifact(path) do
    Enum.each(sidecar_suffixes(), fn suffix ->
      case File.stat(path <> suffix) do
        {:ok, %{size: 0}} -> _ = File.rm(path <> suffix)
        _ -> :ok
      end
    end)
  end

  @impl true
  def writer_capabilities(storage_mode) do
    %{
      max_writers: if(storage_mode == :disk, do: @max_writers, else: 1),
      sequence_persistence: if(storage_mode == :disk, do: :separate_connection, else: :none)
    }
  end

  @impl true
  def validate_capabilities!, do: Capabilities.validate!()

  @impl true
  def capabilities_report, do: Capabilities.report()

  defp require_mvcc(conn) do
    case Connection.pragma(conn, "journal_mode") do
      {:ok, [["mvcc"]]} ->
        :ok

      other ->
        {:error,
         VialKeeper.Error.unsupported_format("Turso connection is not in MVCC mode", %{
           cause: inspect(other)
         })}
    end
  end
end
