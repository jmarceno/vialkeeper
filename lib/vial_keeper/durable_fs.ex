defmodule VialKeeper.DurableFS do
  @moduledoc """
  Shared filesystem durability helpers used by atomic writers and blob install.
  """

  @doc """
  Flushes file contents with `fdatasync`.

  Content and the metadata required to read it become durable. Callers that
  create a new directory entry still need `sync_directory/1` after rename or
  link. `fdatasync` is enough for the inode; `fsync` also flushed unused
  timestamps and dominated attachment ingest on spinning disk.
  """
  @spec sync_file(Path.t()) :: :ok | {:error, File.posix()}
  def sync_file(path) when is_binary(path) do
    case File.open(path, [:read, :write, :raw, :binary], fn io -> :file.datasync(io) end) do
      {:ok, :ok} -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  @doc "Flushes an already-open file descriptor with `fdatasync`."
  @spec sync_fd(term()) :: :ok | {:error, term()}
  def sync_fd(fd), do: :file.datasync(fd)

  @doc """
  Syncs a directory for durability after rename. Platforms that reject
  directory sync (`:eperm`, `:eisdir`, `:enotsup`) are treated as success.
  """
  @spec sync_directory(Path.t()) :: :ok | {:error, File.posix()}
  def sync_directory(directory) do
    case File.open(directory, [:read], fn io -> :file.sync(io) end) do
      {:ok, :ok} -> :ok
      {:error, reason} when reason in [:eperm, :eisdir, :enotsup] -> :ok
      {:error, reason} -> {:error, reason}
    end
  end
end
