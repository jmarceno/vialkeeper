defmodule VialKeeper.Bench.Marker do
  @moduledoc "READY markers for prepared dataset fixtures."

  alias VialKeeper.Bench.{Checksums, Root}
  alias VialKeeper.Runtime.AtomicWrite

  @filename "READY.json"

  @spec filename() :: binary()
  def filename, do: @filename

  @spec write(Root.t(), Path.t(), map()) :: :ok | {:error, binary()}
  def write(%Root{} = context, dataset_dir, payload) when is_map(payload) do
    path = Path.join(dataset_dir, @filename)

    with :ok <- assert_inside(context, dataset_dir),
         :ok <- assert_inside(context, path) do
      body = JSON.encode!(Map.put(payload, "schema_version", 1)) <> "\n"

      case AtomicWrite.write(path, body) do
        :ok -> :ok
        {:error, reason} -> {:error, "failed to write READY marker: #{inspect(reason)}"}
      end
    end
  end

  @spec read(Path.t()) :: {:ok, map()} | {:error, term()}
  def read(dataset_dir) when is_binary(dataset_dir) do
    path = Path.join(dataset_dir, @filename)

    case File.read(path) do
      {:ok, body} ->
        case JSON.decode(body) do
          {:ok, payload} when is_map(payload) -> {:ok, payload}
          {:ok, _} -> {:error, :invalid_marker}
          {:error, _} -> {:error, :invalid_marker}
        end

      {:error, :enoent} ->
        {:error, :missing}

      {:error, reason} ->
        {:error, reason}
    end
  end

  @spec present?(Path.t()) :: boolean()
  def present?(dataset_dir), do: match?({:ok, _}, read(dataset_dir))

  @spec manifest_hash(Path.t()) :: {:ok, binary()} | {:error, term()}
  def manifest_hash(dataset_dir) do
    Checksums.sha256_file(Path.join(dataset_dir, "manifest.json"))
  end

  defp assert_inside(context, path) do
    expanded = Path.expand(path)

    if Root.descendant?(expanded, context.root) or expanded == context.root do
      :ok
    else
      {:error, "marker path #{expanded} is outside #{context.root}"}
    end
  end
end
