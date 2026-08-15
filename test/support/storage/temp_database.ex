defmodule VialKeeper.TempDatabase do
  @moduledoc """
  Creates and cleans up temporary database bundle paths for tests.

  Owns unique `.vialkeeper` bundle directories under the system temp directory so
  adapter and runtime tests do not collide when run in parallel or sequentially.
  """

  alias VialKeeper.DatabaseBundle

  @sqlite_filename "database.sqlite3"

  @doc """
  Returns a unique absolute database bundle directory path that does not yet exist.
  """
  @spec path(keyword()) :: binary()
  def path(opts \\ []) do
    prefix = Keyword.get(opts, :prefix, "vialkeeper")
    dir = Keyword.get(opts, :dir, System.tmp_dir!())
    name = "#{prefix}-#{System.unique_integer([:positive])}.vialkeeper"
    Path.join(dir, name)
  end

  @doc """
  Returns the canonical SQLite metadata path inside a bundle directory.
  """
  @spec sqlite_path(binary()) :: binary()
  def sqlite_path(bundle_path) when is_binary(bundle_path),
    do: Path.join(bundle_path, @sqlite_filename)

  @doc """
  Builds a unique temporary bundle directory and creates the bundle layout.
  """
  @spec create(keyword()) :: {:ok, binary()}
  def create(opts \\ []) do
    bundle_path = path(opts)
    File.mkdir_p!(Path.dirname(bundle_path))

    case DatabaseBundle.create(bundle_path) do
      {:ok, _bundle} -> {:ok, bundle_path}
      {:error, reason} -> raise "cannot create temp database bundle: #{inspect(reason)}"
    end
  end

  @doc """
  Removes a database bundle directory and SQLite companion files when present.

  SQLite may leave `-wal`, `-shm`, or `-journal` files behind after an
  interrupted process. Tests must remove those sidecars explicitly so a later
  test cannot inherit stale recovery state from another scenario.
  """
  @spec cleanup(binary()) :: :ok
  def cleanup(bundle_path) when is_binary(bundle_path) do
    sqlite_path = sqlite_path(bundle_path)

    for suffix <- ["", ".lease", ".lease-journal", "-journal", "-wal", "-shm"] do
      _ = File.rm(sqlite_path <> suffix)
    end

    _ = File.rm_rf(bundle_path)
    :ok
  end
end
