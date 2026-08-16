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

    with :ok <- File.mkdir_p(root),
         temp = path <> ".tmp." <> Integer.to_string(System.unique_integer([:positive])),
         :ok <- File.write(temp, contents),
         :ok <- sync(temp),
         :ok <- File.rename(temp, path),
         :ok <- DurableFS.sync_directory(root) do
      :ok
    else
      {:error, reason} ->
        # The temp path is local to this clause only on the success path; best
        # effort cleanup of any leftover temp is handled below.
        cleanup_temps(path)
        {:error, reason}
    end
  end

  defp cleanup_temps(path) do
    root = Path.dirname(path)

    case File.ls(root) do
      {:ok, entries} ->
        prefix = Path.basename(path) <> ".tmp."
        Enum.each(entries, &cleanup_temp_entry(root, prefix, &1))

      {:error, _} ->
        :ok
    end
  end

  defp cleanup_temp_entry(root, prefix, entry) do
    if String.starts_with?(entry, prefix), do: File.rm(Path.join(root, entry))
  end

  defp sync(file) do
    case File.open(file, [:read, :write], fn io -> :file.sync(io) end) do
      {:ok, :ok} -> :ok
      {:error, reason} -> {:error, reason}
    end
  end
end
