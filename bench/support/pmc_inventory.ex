defmodule VialKeeper.Bench.PmcInventory do
  @moduledoc """
  First-use PMC inventory snapshot freeze and metadata selection.

  The inventory CSV identity is stored under the external cache so a later
  regenerate with the same snapshot and algorithm yields the same 100K set.
  """

  alias VialKeeper.AtomicWrite
  alias VialKeeper.Bench.{Downloader, Pmc, Registry, Root}

  @list_url "https://pmc-oa-opendata.s3.amazonaws.com/?list-type=2&prefix=inventory-reports/pmc-oa-opendata/metadata/&delimiter=/"
  @https "https://pmc-oa-opendata.s3.amazonaws.com"
  @overscan 300_000
  @fetch_concurrency 16
  @metadata_batch_size 1_024

  @spec generate(Root.t(), map(), keyword()) :: {:ok, map()} | {:error, binary()}
  def generate(%Root{} = context, spec, opts) do
    download = Keyword.get(opts, :download, &Downloader.download/3)
    count = Registry.selection_count("pmc", :standard)

    with {:ok, cache} <- Root.cache_path(context, spec["name"], spec["version"]),
         :ok <- File.mkdir_p(cache),
         {:ok, snapshot, files} <- freeze_snapshot(context, cache, opts),
         :ok <- download_inventory_files(context, cache, files, download, opts),
         :ok <- Downloader.ensure_started(),
         keys <- collect_keys(cache, files),
         {:ok, selected} <- selected_keys(context, cache, keys, spec, count),
         {:ok, rows} <- fetch_metadata(context, cache, selected, spec, count, download, opts),
         :ok <- enough_rows(rows, count) do
      spec = Map.put(spec, "inventory_snapshot", snapshot)
      {:ok, Pmc.generate_manifest(rows, spec, count)}
    end
  end

  defp selected_keys(context, cache, keys, spec, count) do
    path = Path.join(cache, "selected-keys.json")

    case read_selected_keys(path) do
      {:ok, selected} ->
        {:ok, selected}

      :missing_or_invalid ->
        selected = Pmc.select_inventory_keys(keys, spec, min(@overscan, max(count * 3, count)))
        cache_selected_keys(context, path, selected)
    end
  end

  defp cache_selected_keys(context, path, selected) do
    if Root.descendant?(path, context.root) do
      write_selected_keys(path, selected)
    else
      {:error, "selected PMC key cache escapes benchmark root"}
    end
  end

  defp write_selected_keys(path, selected) do
    case AtomicWrite.write(path, JSON.encode!(selected) <> "\n") do
      :ok -> {:ok, selected}
      {:error, reason} -> {:error, "failed to cache selected PMC keys: #{inspect(reason)}"}
    end
  end

  defp read_selected_keys(path) do
    with {:ok, body} <- File.read(path),
         {:ok, selected} when is_list(selected) <- JSON.decode(body) do
      validate_selected_keys(selected)
    else
      _ -> :missing_or_invalid
    end
  end

  defp validate_selected_keys(selected) do
    if Enum.all?(selected, &valid_selected_key?/1),
      do: {:ok, selected},
      else: :missing_or_invalid
  end

  defp valid_selected_key?(%{"key" => key, "pmcid" => pmcid, "version" => version})
       when is_binary(key) and is_binary(pmcid) and is_integer(version),
       do: true

  defp valid_selected_key?(_), do: false

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

        case Enum.reverse(prefixes) do
          [] -> {:error, "no PMC inventory snapshots were listed"}
          [prefix | _] -> {:ok, prefix}
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

  defp fetch_metadata(context, cache, keys, spec, count, download, opts) do
    meta_dir = Path.join(cache, "metadata")
    :ok = File.mkdir_p(meta_dir)
    prefixes = spec["approved_license_prefixes"] || ["CC "]

    rows =
      keys
      |> Enum.chunk_every(@metadata_batch_size)
      |> Enum.reduce_while({[], 0}, fn batch, acc ->
        reduce_metadata_batch(context, meta_dir, batch, download, opts, prefixes, count, acc)
      end)

    case rows do
      {:error, reason} -> {:error, reason}
      {batches, _eligible} -> {:ok, batches |> Enum.reverse() |> List.flatten()}
    end
  end

  defp reduce_metadata_batch(
         context,
         meta_dir,
         batch,
         download,
         opts,
         prefixes,
         count,
         {batches, eligible}
       ) do
    case fetch_metadata_batch(context, meta_dir, batch, download, opts) do
      {:ok, batch_rows} ->
        batch_eligible = Enum.count(batch_rows, &Pmc.selectable?(&1, prefixes))
        continue_metadata({[batch_rows | batches], eligible + batch_eligible}, count)

      {:error, reason} ->
        {:halt, {:error, reason}}
    end
  end

  defp continue_metadata({_batches, eligible} = acc, count) when eligible >= count,
    do: {:halt, acc}

  defp continue_metadata(acc, _count), do: {:cont, acc}

  defp fetch_metadata_batch(context, meta_dir, keys, download, opts) do
    result =
      keys
      |> Task.async_stream(
        fn key -> fetch_one(context, meta_dir, key, download, opts) end,
        max_concurrency: @fetch_concurrency,
        timeout: :infinity,
        ordered: false
      )
      |> Enum.reduce_while([], fn
        {:ok, {:ok, row}}, acc ->
          {:cont, [row | acc]}

        {:ok, :skip}, acc ->
          {:cont, acc}

        {:ok, {:error, reason}}, _acc ->
          {:halt, {:error, reason}}

        {:exit, reason}, _acc ->
          {:halt, {:error, "PMC metadata fetch crashed: #{inspect(reason)}"}}
      end)

    case result do
      {:error, reason} -> {:error, reason}
      rows -> {:ok, rows}
    end
  end

  defp fetch_one(context, meta_dir, key, download, opts) do
    fetch_one(context, meta_dir, key, download, opts, 0)
  end

  defp fetch_one(context, meta_dir, key, download, opts, attempt) do
    dest = Path.join(meta_dir, Path.basename(key["key"]))

    result =
      if File.regular?(dest) do
        :ok
      else
        download.(context, %{url: Pmc.metadata_https(key["key"]), dest: dest}, opts)
      end

    case result do
      :ok ->
        decode_metadata_file(dest)

      {:error, reason} ->
        retry_or_skip_metadata(context, meta_dir, key, download, opts, attempt, reason)
    end
  end

  defp retry_or_skip_metadata(context, meta_dir, key, download, opts, attempt, reason) do
    cond do
      is_binary(reason) and String.contains?(reason, "HTTP 404") ->
        :skip

      attempt < 4 ->
        Process.sleep(250 * (attempt + 1))
        fetch_one(context, meta_dir, key, download, opts, attempt + 1)

      true ->
        {:error, reason}
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
