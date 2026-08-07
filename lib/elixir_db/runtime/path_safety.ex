defmodule ElixirDB.Runtime.PathSafety do
  @moduledoc "Filesystem path checks shared by registration and runtime routing."

  @doc "Returns false when an existing path component is a symlink or cannot be inspected."
  @spec no_symlink_components?(Path.t()) :: boolean()
  def no_symlink_components?(path) do
    result =
      path
      |> Path.split()
      |> Enum.reduce_while("", fn component, current ->
        next = Path.join(current, component)

        case File.lstat(next) do
          {:ok, %File.Stat{type: :symlink}} -> {:halt, false}
          {:ok, _} -> {:cont, next}
          {:error, :enoent} -> {:halt, true}
          {:error, _} -> {:halt, false}
        end
      end)

    result != false
  end
end
