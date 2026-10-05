defmodule VialKeeper.Storage.Turso.Capabilities do
  @moduledoc """
  Turso runtime capability probe for the physical backend.

  Full-text search is Tantivy, so the probe needs no FTS: it checks that the
  engine answers `sqlite_version()` and that a disk file accepts the MVCC
  journal and a `BEGIN CONCURRENT` / `COMMIT` round trip.
  """

  alias VialKeeper.Storage.SQLite.Connection
  alias VialKeeper.Storage.Turso.Driver

  @report_key {__MODULE__, :report}

  @doc "Fails fast when the Turso build lacks MVCC concurrent writes."
  @spec validate!() :: binary()
  def validate! do
    case probe() do
      {:ok, version} -> version
      reason -> raise "Turso runtime does not satisfy Version 1 capabilities: #{inspect(reason)}"
    end
  end

  @doc "Returns opaque Turso capability metadata for diagnostics."
  @spec report() :: map()
  def report do
    case :persistent_term.get(@report_key, :missing) do
      :missing ->
        report = load_report()
        :persistent_term.put(@report_key, report)
        report

      report ->
        report
    end
  end

  defp load_report do
    case probe() do
      {:ok, version} ->
        %{engine: "turso", driver: "turso", sqlite: version, mvcc: true, max_writers: 16}

      _ ->
        %{engine: "turso", available: false}
    end
  end

  defp probe do
    dir =
      Path.join(System.tmp_dir!(), "vialkeeper-turso-probe-#{System.unique_integer([:positive])}")

    File.mkdir_p!(dir)

    try do
      with {:ok, conn} <- Connection.open(Path.join(dir, "probe.db"), driver: Driver) do
        try do
          run_probe(conn)
        after
          Connection.close(conn)
        end
      end
    after
      File.rm_rf(dir)
    end
  end

  defp run_probe(conn) do
    with {:ok, [[version]]} <- Connection.query(conn, "SELECT sqlite_version()"),
         :ok <- Connection.execute(conn, "PRAGMA journal_mode = 'mvcc'"),
         {:ok, [["mvcc"]]} <- Connection.pragma(conn, "journal_mode"),
         :ok <- Connection.execute(conn, "CREATE TABLE probe (id INTEGER PRIMARY KEY)"),
         :ok <- Connection.exec(conn, "BEGIN CONCURRENT"),
         :ok <- Connection.execute(conn, "INSERT INTO probe (id) VALUES (1)"),
         :ok <- Connection.exec(conn, "COMMIT"),
         {:ok, [[1]]} <- Connection.query(conn, "SELECT count(*) FROM probe") do
      {:ok, version}
    end
  end
end
