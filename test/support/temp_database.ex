defmodule ElixirDB.TempDatabase do
  @moduledoc """
  Creates and cleans up temporary SQLite database paths for tests.

  Owns unique file paths under the system temp directory so adapter and
  runtime tests do not collide when run in parallel or sequentially.
  """

  @doc """
  Returns a unique absolute SQLite database path that does not yet exist.
  """
  @spec path(keyword()) :: binary()
  def path(opts \\ []) do
    prefix = Keyword.get(opts, :prefix, "elixirdb")
    dir = Keyword.get(opts, :dir, System.tmp_dir!())
    name = "#{prefix}-#{System.unique_integer([:positive])}.db"
    Path.join(dir, name)
  end

  @doc """
  Builds a unique temporary database path and ensures its parent directory exists.
  """
  @spec create(keyword()) :: {:ok, binary()}
  def create(opts \\ []) do
    db_path = path(opts)
    File.mkdir_p!(Path.dirname(db_path))
    {:ok, db_path}
  end

  @doc """
  Removes a database file and SQLite companion files when present.

  SQLite may leave rollback journals behind after an interrupted process. Tests
  must remove those sidecars explicitly so a later test cannot inherit stale
  recovery state from another scenario.
  """
  @spec cleanup(binary()) :: :ok
  def cleanup(db_path) when is_binary(db_path) do
    for suffix <- ["", ".lease", ".lease-journal", "-journal", "-wal", "-shm"] do
      _ = File.rm(db_path <> suffix)
    end

    :ok
  end
end
