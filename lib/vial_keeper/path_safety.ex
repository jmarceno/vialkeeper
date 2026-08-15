defmodule VialKeeper.PathSafety do
  @moduledoc "Filesystem path checks shared by registration and runtime routing."

  @doc "Returns false when an existing path component is a symlink or cannot be inspected."
  @spec no_symlink_components?(Path.t()) :: boolean()
  def no_symlink_components?(path) do
    path
    |> Path.expand()
    |> walk_components()
    |> Kernel.!=(false)
  end

  @doc """
  Returns true when `path` resolves to a location inside `root`.

  Rejects paths that escape the root via `..`, absolute paths outside the root,
  or cases where `Path.relative_to/2` cannot express containment.
  """
  @spec within_root?(Path.t(), Path.t()) :: boolean()
  def within_root?(path, root) do
    expanded = Path.expand(path)
    root = Path.expand(root)
    relative = Path.relative_to(expanded, root)

    relative != expanded and not String.starts_with?(relative, "../")
  end

  defp walk_components(path) do
    case Path.split(path) do
      ["/" | components] -> Enum.reduce_while(components, "/", &check_component/2)
      components -> Enum.reduce_while(components, "", &check_component/2)
    end
  end

  defp check_component(component, current) do
    next = Path.join(current, component)

    case File.lstat(next) do
      {:ok, %File.Stat{type: :symlink}} -> {:halt, false}
      {:ok, _} -> {:cont, next}
      {:error, :enoent} -> {:halt, true}
      {:error, _} -> {:halt, false}
    end
  end
end
