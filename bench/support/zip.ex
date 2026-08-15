defmodule VialKeeper.Bench.Zip do
  @moduledoc "Zip extraction that refuses path-escaping entries."

  alias VialKeeper.Bench.Root

  @spec extract!(Root.t(), Path.t(), Path.t()) :: :ok | {:error, binary()}
  def extract!(%Root{} = context, archive, dest) do
    with :ok <- assert_inside(context, archive),
         :ok <- assert_inside(context, dest),
         :ok <- File.mkdir_p(dest) do
      extract_archive(archive, dest)
    end
  end

  defp extract_archive(archive, dest) do
    char_archive = String.to_charlist(archive)

    case :zip.zip_open(char_archive, [:cooked]) do
      {:ok, handle} ->
        try do
          with {:ok, entries} <- :zip.zip_list_dir(handle),
               {:ok, names} <- safe_names(entries, dest) do
            unzip(char_archive, dest, names)
          end
        after
          _ = :zip.zip_close(handle)
        end

      {:error, reason} ->
        {:error, "cannot open zip #{archive}: #{inspect(reason)}"}
    end
  end

  defp unzip(archive, dest, names) do
    case :zip.unzip(archive, cwd: String.to_charlist(dest), file_list: names) do
      {:ok, _files} -> :ok
      {:error, reason} -> {:error, "zip extract failed: #{inspect(reason)}"}
    end
  end

  defp safe_names(entries, dest) do
    Enum.reduce_while(entries, {:ok, []}, fn entry, {:ok, acc} ->
      accumulate_zip_entry(entry, dest, acc)
    end)
    |> case do
      {:ok, names} -> {:ok, Enum.reverse(names)}
      error -> error
    end
  end

  defp accumulate_zip_entry(entry, dest, acc) do
    case entry_name(entry) do
      nil -> {:cont, {:ok, acc}}
      name -> accept_zip_name(name, dest, acc)
    end
  end

  defp accept_zip_name(name, dest, acc) do
    relative = List.to_string(name)

    if safe_relative?(relative, dest) do
      {:cont, {:ok, [name | acc]}}
    else
      {:halt, {:error, "zip entry escapes destination: #{relative}"}}
    end
  end

  defp entry_name({:zip_file, name, _info, _comment, _offset, _comp}), do: name
  defp entry_name(_), do: nil

  defp safe_relative?(relative, dest) do
    parts = Path.split(relative)
    expanded = Path.expand(relative, dest)

    (Root.descendant?(expanded, dest) or expanded == Path.expand(dest)) and
      not Enum.member?(parts, "..") and not Enum.member?(parts, "/")
  end

  defp assert_inside(context, path) do
    expanded = Path.expand(path)

    if Root.descendant?(expanded, context.root) or expanded == context.root do
      :ok
    else
      {:error, "zip path #{expanded} is outside #{context.root}"}
    end
  end
end
