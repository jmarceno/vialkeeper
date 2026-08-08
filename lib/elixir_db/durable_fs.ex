defmodule ElixirDB.DurableFS do
  @moduledoc """
  Shared filesystem durability helpers used by atomic writers and blob install.
  """

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
