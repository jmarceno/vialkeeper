defmodule ElixirDB.Storage.SQLite.Capabilities do
  @moduledoc """
  SQLite runtime capability probes for the physical backend.

  Product startup asks the selected backend to validate required capabilities.
  These probes stay below the storage boundary.
  """

  alias ElixirDB.Storage.SQLite.Connection

  @report_key {__MODULE__, :report}

  @doc "Fails fast when the SQLite build lacks Version 1 capabilities."
  @spec validate!() :: binary()
  def validate! do
    with {:ok, conn} <- Connection.open(":memory:"),
         {:ok, [[version]]} <- Connection.query(conn, "SELECT sqlite_version()"),
         true <- version_at_least?(version, "3.45.0"),
         {:ok, _} <- Connection.query(conn, "CREATE VIRTUAL TABLE fts_probe USING fts5(body)") do
      Application.put_env(
        :elixir_db,
        :fts5_contentless_delete,
        fts5_contentless_delete_supported?(conn)
      )

      _ = Connection.close(conn)
      version
    else
      reason ->
        Application.put_env(:elixir_db, :fts5_contentless_delete, false)

        raise "SQLite runtime does not satisfy Version 1 capabilities: #{inspect(reason)}"
    end
  end

  @doc "Returns opaque SQLite capability metadata for diagnostics."
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
    with {:ok, conn} <- Connection.open(":memory:"),
         {:ok, [[sqlite_version]]} <- Connection.query(conn, "SELECT sqlite_version()"),
         {:ok, compile_options} <- Connection.query(conn, "PRAGMA compile_options") do
      contentless_delete = fts5_contentless_delete_supported?(conn)
      _ = Connection.close(conn)

      %{
        engine: "sqlite",
        exqlite: Application.spec(:exqlite, :vsn) |> to_string(),
        sqlite: sqlite_version,
        sqlite_compile_options: Enum.map(compile_options, &List.first/1),
        fts5_contentless_delete: contentless_delete
      }
    else
      _ ->
        %{engine: "sqlite", available: false}
    end
  end

  @doc """
  Returns true when the pinned SQLite build supports contentless-delete FTS5.
  """
  @spec fts5_contentless_delete_supported?() :: boolean()
  def fts5_contentless_delete_supported? do
    case Application.get_env(:elixir_db, :fts5_contentless_delete) do
      true ->
        true

      false ->
        false

      nil ->
        case Connection.open(":memory:") do
          {:ok, conn} ->
            result = fts5_contentless_delete_supported?(conn)
            Application.put_env(:elixir_db, :fts5_contentless_delete, result)
            _ = Connection.close(conn)
            result

          _ ->
            false
        end
    end
  end

  defp fts5_contentless_delete_supported?(conn) do
    table = "fts_contentless_delete_probe"

    with :ok <-
           Connection.execute(
             conn,
             "CREATE VIRTUAL TABLE #{table} USING fts5(content, content='', contentless_delete=1)"
           ),
         :ok <-
           Connection.execute(conn, "INSERT INTO #{table}(rowid, content) VALUES (1, 'alpha')"),
         :ok <- Connection.execute(conn, "DELETE FROM #{table} WHERE rowid = 1"),
         :ok <-
           Connection.execute(conn, "INSERT INTO #{table}(rowid, content) VALUES (1, 'beta')"),
         :ok <- Connection.execute(conn, "BEGIN"),
         :ok <-
           Connection.execute(conn, "INSERT INTO #{table}(rowid, content) VALUES (2, 'gamma')"),
         :ok <- Connection.execute(conn, "ROLLBACK"),
         {:ok, [[1]]} <- Connection.query(conn, "SELECT rowid FROM #{table}"),
         :ok <- Connection.execute(conn, "BEGIN"),
         :ok <-
           Connection.execute(conn, "DELETE FROM #{table} WHERE rowid = 1"),
         :ok <-
           Connection.execute(conn, "INSERT INTO #{table}(rowid, content) VALUES (3, 'delta')"),
         :ok <-
           Connection.execute(conn, "INSERT INTO #{table}(rowid, content) VALUES (4, 'epsilon')"),
         :ok <- Connection.execute(conn, "COMMIT"),
         {:ok, [[3], [4]]} <-
           Connection.query(conn, "SELECT rowid FROM #{table} ORDER BY rowid") do
      _ = Connection.execute(conn, "DROP TABLE #{table}")
      true
    else
      _ ->
        _ = Connection.execute(conn, "DROP TABLE IF EXISTS #{table}")
        false
    end
  end

  defp version_at_least?(actual, minimum) do
    parse = fn value -> value |> String.split(".") |> Enum.map(&String.to_integer/1) end
    parse.(actual) >= parse.(minimum)
  end
end
