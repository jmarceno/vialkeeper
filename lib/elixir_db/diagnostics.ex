defmodule ElixirDB.Diagnostics do
  @moduledoc "Runtime and SQLite compatibility metadata for releases and support."

  alias ElixirDB.Storage.SQLite.Connection

  def runtime do
    with {:ok, conn} <- Connection.open(":memory:"),
         {:ok, [[sqlite_version]]} <- Connection.query(conn, "SELECT sqlite_version()"),
         {:ok, compile_options} <- Connection.query(conn, "PRAGMA compile_options") do
      contentless_delete = fts5_contentless_delete_supported?(conn)
      _ = Connection.close(conn)

      %{
        elixir: System.version(),
        otp: :erlang.system_info(:otp_release) |> List.to_string(),
        exqlite: Application.spec(:exqlite, :vsn) |> to_string(),
        sqlite: sqlite_version,
        sqlite_compile_options: Enum.map(compile_options, &List.first/1),
        fts5_contentless_delete: contentless_delete,
        protocol_major: ElixirDB.protocol_major(),
        revision_algorithm_version: ElixirDB.revision_algorithm_version(),
        canonicalization_version: ElixirDB.canonicalization_version(),
        # Plan §3.1: release record MUST include the git commit. Prefer an explicit
        # ELIXIRDB_GIT_REF env override, then a build-time file written by the release
        # step (priv/git_ref), then "unknown" — never shell out to git at runtime.
        git_commit: git_commit()
      }
    else
      _ ->
        %{
          elixir: System.version(),
          otp: :erlang.system_info(:otp_release) |> List.to_string(),
          git_commit: git_commit()
        }
    end
  end

  @doc """
  Resolves the release git commit without shelling out to git at runtime.

  Resolution order: `ELIXIRDB_GIT_REF` env (only when non-empty), `priv/git_ref`
  build-time file, `"unknown"`.
  """
  @spec git_commit() :: binary()
  def git_commit do
    case System.get_env("ELIXIRDB_GIT_REF") do
      value when is_binary(value) and value != "" -> value
      _ -> read_build_time_git_ref() || "unknown"
    end
  end

  defp read_build_time_git_ref do
    # :code.priv_dir/1 returns {:error, :bad_name} when the app's priv dir cannot be
    # resolved (unusual deployment layouts); treat that as "no build-time ref" rather than
    # crashing runtime/0.
    path =
      case :code.priv_dir(:elixir_db) do
        {:error, :bad_name} -> nil
        priv -> Path.join(priv, "git_ref")
      end

    with dest when is_binary(dest) <- path,
         {:ok, content} <- File.read(dest) do
      ref = String.trim(content)
      if ref == "", do: nil, else: ref
    else
      _ -> nil
    end
  end

  def validate_sqlite! do
    with {:ok, conn} <- Connection.open(":memory:"),
         {:ok, [[version]]} <- Connection.query(conn, "SELECT sqlite_version()"),
         true <- version_at_least?(version, "3.45.0"),
         {:ok, _} <- Connection.query(conn, "CREATE VIRTUAL TABLE fts_probe USING fts5(body)") do
      # Prefer contentless-delete when available; fall back to ordinary contentless (Plan §9.6).
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

  @doc """
  Returns true when the pinned SQLite build supports contentless-delete FTS5 with
  create, insert/update/delete, rollback, and rebuild-in-one-transaction behavior.
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

    # Contentless-delete supports INSERT + DELETE by rowid (not UPDATE of stored content).
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
