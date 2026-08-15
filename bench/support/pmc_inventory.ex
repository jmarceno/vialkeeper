defmodule VialKeeper.Bench.PmcInventory do
  @moduledoc """
  First-use PMC inventory snapshot freeze and metadata selection.

  The inventory CSV identity is stored under the external cache so a later
  regenerate with the same snapshot and algorithm yields the same 100K set.
  """

  alias VialKeeper.Bench.{Downloader, Pmc, Registry, Root}

  @list_url "https://pmc-oa-opendata.s3.amazonaws.com/?list-type=2&prefix=inventory-reports/pmc-oa-opendata/metadata/&delimiter=/"
  @https "https://pmc-oa-opendata.s3.amazonaws.com"
  @overscan 300_000
  @fetch_concurrency 8

  @spec generate(Root.t(), map(), keyword()) :: {:ok, map()} | {:error, binary()}
  def generate(%Root{} = context, spec, opts) do
    download = Keyword.get(opts, :download, &Downloader.download/3)
    count = Registry.selection_count("pmc", :standard)

    with {:ok, cache} <- Root.cache_path(context, spec["name"], spec["version"]),
         :ok <- File.mkdir_p(cache),
         {:ok, snapshot, files} <- freeze_snapshot(context, cache, opts),
         :ok <- download_inventory_files(context, cache, files, download, opts),
         keys <- collect_keys(cache, files),
         selected <- Pmc.select_inventory_keys(keys, spec, min(@overscan, max(count * 3, count))),
         {:ok, rows} <- fetch_metadata(context, cache, selected, download, opts),
         :ok <- enough_rows(rows, count) do
      spec = Map.put(spec, "inventory_snapshot", snapshot)
      {:ok, Pmc.generate_manifest(rows, spec, count)}
    end
  end

  defp enough_rows(rows, count) do
    if length(rows) < count do
      {:error, "PMC inventory yielded #{length(rows)} selectable articles, need #{count}"}
    else
      :ok
    end
  end

  defp freeze_snapshot(context, cache, opts) do
    marker = Path.join(cache, "inventory-snapshot.json")

    cond do
      File.regular?(marker) ->
        read_snapshot(marker)

      is_binary(opts[:inventory_snapshot_prefix]) ->
        load_snapshot(context, cache, opts[:inventory_snapshot_prefix], marker, opts)

      true ->
        with {:ok, prefix} <- latest_snapshot(opts) do
          load_snapshot(context, cache, prefix, marker, opts)
        end
    end
  end

  defp latest_snapshot(opts) do
    get = Keyword.get(opts, :get_body, &Downloader.get_body/1)

    case get.(@list_url) do
      {:ok, body} ->
        prefixes =
          Regex.scan(~r/<Prefix>([^<]+)<\/Prefix>/, body)
          |> Enum.map(fn [_, prefix] -> prefix end)
          |> Enum.filter(&Regex.match?(~r/\d{4}-\d{2}-\d{2}T\d{2}-\d{2}Z\/$/, &1))
          |> Enum.sort()

        case List.last(prefixes) do
          nil -> {:error, "no PMC inventory snapshots were listed"}
          prefix -> {:ok, prefix}
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp load_snapshot(context, _cache, prefix, marker, opts) do
    get = Keyword.get(opts, :get_body, &Downloader.get_body/1)
    url = @https <> "/" <> String.trim_trailing(prefix, "/") <> "/manifest.json"

    with {:ok, body} <- get.(url),
         {:ok, payload} <- decode_manifest(body),
         files when is_list(files) and files != [] <- payload["files"] || [],
         keys = Enum.map(files, & &1["key"]),
         :ok <- write_snapshot(context, marker, prefix, keys) do
      {:ok, prefix, keys}
    else
      {:error, reason} -> {:error, reason}
      _ -> {:error, "PMC inventory manifest at #{prefix} is missing file keys"}
    end
  end

  defp decode_manifest(body) do
    case JSON.decode(body) do
      {:ok, payload} when is_map(payload) -> {:ok, payload}
      _ -> {:error, "PMC inventory manifest is not a JSON object"}
    end
  end

  defp write_snapshot(context, path, prefix, keys) do
    if Root.descendant?(path, context.root) do
      payload = JSON.encode!(%{"snapshot" => prefix, "files" => keys}) <> "\n"

      case File.write(path, payload) do
        :ok -> :ok
        {:error, reason} -> {:error, "failed to freeze PMC inventory snapshot: #{inspect(reason)}"}
      end
    else
      {:error, "inventory snapshot path escapes the benchmark root"}
    end
  end

  defp read_snapshot(path) do
    case File.read(path) do
      {:ok, body} ->
        case JSON.decode(body) do
          {:ok, %{"snapshot" => snapshot, "files" => files}}
          when is_binary(snapshot) and is_list(files) ->
            {:ok, snapshot, files}

          _ ->
            {:error, "cached PMC inventory snapshot is invalid"}
        end

      {:error, reason} ->
        {:error, "cannot read cached PMC inventory snapshot: #{inspect(reason)}"}
    end
  end

  defp download_inventory_files(context, cache, files, download, opts) do
    Enum.reduce_while(files, :ok, fn key, :ok ->
      fetch_inventory_file(context, cache, key, download, opts)
    end)
  end

  defp fetch_inventory_file(context, cache, key, download, opts) do
    dest = Path.join(cache, Path.basename(key))

    if File.regular?(dest) do
      {:cont, :ok}
    else
      continue_or_halt(download.(context, %{url: @https <> "/" <> key, dest: dest}, opts))
    end
  end

  defp continue_or_halt(:ok), do: {:cont, :ok}
  defp continue_or_halt({:error, reason}), do: {:halt, {:error, reason}}

  defp collect_keys(cache, files) do
    files
    |> Enum.map(&Path.join(cache, Path.basename(&1)))
    |> Enum.filter(&File.regular?/1)
    |> Stream.flat_map(&Pmc.inventory_keys/1)
  end

  defp fetch_metadata(context, cache, keys, download, opts) do
    meta_dir = Path.join(cache, "metadata")
    :ok = File.mkdir_p(meta_dir)

    rows =
      keys
      |> Task.async_stream(
        fn key -> fetch_one(context, meta_dir, key, download, opts) end,
        max_concurrency: @fetch_concurrency,
        timeout: :infinity,
        ordered: true
      )
      |> Enum.reduce_while([], fn
        {:ok, {:ok, row}}, acc -> {:cont, [row | acc]}
        {:ok, :skip}, acc -> {:cont, acc}
        {:ok, {:error, reason}}, _acc -> {:halt, {:error, reason}}
        {:exit, reason}, _acc -> {:halt, {:error, "PMC metadata fetch crashed: #{inspect(reason)}"}}
      end)

    case rows do
      {:error, reason} -> {:error, reason}
      list -> {:ok, Enum.reverse(list)}
    end
  end

  defp fetch_one(context, meta_dir, key, download, opts) do
    dest = Path.join(meta_dir, Path.basename(key["key"]))

    result =
      if File.regular?(dest) do
        :ok
      else
        download.(context, %{url: Pmc.metadata_https(key["key"]), dest: dest}, opts)
      end

    case result do
      :ok -> decode_metadata_file(dest)
      {:error, reason} -> {:error, reason}
    end
  end

  defp decode_metadata_file(path) do
    case File.read(path) do
      {:ok, body} ->
        case JSON.decode(body) do
          {:ok, row} when is_map(row) -> {:ok, row}
          _ -> :skip
        end

      {:error, _} ->
        :skip
    end
  end
end
