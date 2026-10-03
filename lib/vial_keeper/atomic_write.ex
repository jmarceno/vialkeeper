defmodule VialKeeper.AtomicWrite do
  @moduledoc """
  Atomic, durable file replacement shared by the registration manifest and the
  host configuration file.

  Implements write-to-temporary-file, fsync, atomic rename, and directory sync,
  so that a failed write leaves the previous file intact. The same durability
  discipline required by `LIFE-007` is reused for `CONFIG-001`.
  """

  alias VialKeeper.DurableFS

  @doc """
  Writes `contents` to `path` atomically.

  Creates the parent directory if missing. Returns `:ok` on success or
  `{:error, reason}` on any failure; a failed write removes the temporary file
  and leaves any previous file at `path` untouched.
  """
  @spec write(Path.t(), iodata()) :: :ok | {:error, File.posix()}
  def write(path, contents) do
    root = Path.dirname(path)
    temp = path <> ".tmp." <> Integer.to_string(System.unique_integer([:positive]))

    with :ok <- File.mkdir_p(root),
         :ok <- File.write(temp, contents),
         :ok <- sync(temp),
         :ok <- File.rename(temp, path),
         :ok <- DurableFS.sync_directory(root) do
      :ok
    else
      {:error, reason} ->
        # Remove only this call's temp file; a concurrent writer to the same
        # path owns its own uniquely-named temp and must not lose it mid-write.
        _ = File.rm(temp)
        {:error, reason}
    end
  end

  defp sync(file) do
    case File.open(file, [:read, :write], fn io -> :file.sync(io) end) do
      {:ok, :ok} -> :ok
      {:ok, {:error, reason}} -> {:error, reason}
      {:error, reason} -> {:error, reason}
    end
  end
end
