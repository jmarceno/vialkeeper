defmodule VialKeeper.Bench.Prepare do
  @moduledoc """
  Atomic dataset preparation under a verified external benchmark root.

  Downloads land in `staging/`, READY is written only after verification, and
  the staging directory is renamed into `datasets/`.
  """

  alias VialKeeper.Bench.{
    Beir,
    Checksums,
    DiskSpace,
    Downloader,
    Marker,
    OpenImages,
    Pmc,
    PmcInventory,
    Progress,
    Registry,
    Root,
    SimpleWiki,
    Statistics,
    Zip
  }

  alias VialKeeper.AtomicWrite

  @spec prepare(binary(), keyword()) :: {:ok, map()} | {:error, binary()}
  def prepare(name, opts \\ []) when is_binary(name) do
    with {:ok, spec} <- Registry.fetch(name),
         {:ok, profile} <- Registry.profile(opts),
         :ok <- Registry.ensure_profile(name, profile),
         opts <- Keyword.put(opts, :profile, profile),
         {:ok, context} <- Root.load(opts),
         :ok <- preflight(context, spec, profile, opts) do
      prepare_dataset(context, spec, profile, opts)
    end
  end

  @spec status(keyword()) :: {:ok, map()} | {:error, binary()}
  def status(opts \\ []) do
    with {:ok, context} <- Root.load(opts),
         {:ok, available} <- available(context, opts) do
      datasets =
        Enum.map(Registry.names(), fn name ->
          spec = Registry.fetch!(name)
          dataset_status(context, spec)
        end)

      {:ok,
       %{
         "root" => context.root,
         "canonical_root" => context.root,
         "root_id" => context.root_id,
         "approved_parent" => context.approved_parent,
         "free_bytes" => available,
         "datasets" => datasets
       }}
    end
  end

  @spec clean(binary(), keyword()) :: :ok | {:error, binary()}
  def clean(name, opts \\ []) when is_binary(name) do
    with {:ok, spec} <- Registry.fetch(name),
         {:ok, context} <- Root.load(opts) do
      Root.remove_dataset!(context, spec["name"], spec["version"])
    end
  end

  defp prepare_dataset(context, spec, profile, opts) do
    with {:ok, dest} <- Root.dataset_path(context, spec["name"], spec["version"]) do
      expected_profile = Atom.to_string(profile)

      case Marker.read(dest) do
        {:ok, %{"profile" => ready_profile}}
        when ready_profile == expected_profile ->
          ready_prepared_dataset(context, spec, dest)

        {:ok, _marker} ->
          replace_dataset(context, spec, profile, opts)

        {:error, :missing} ->
          do_prepare_dataset(context, spec, profile, opts)

        {:error, _reason} ->
          do_prepare_dataset(context, spec, profile, opts)
      end
    end
  end

  defp ready_prepared_dataset(context, spec, dest) do
    with :ok <- ensure_prepared_derivatives(context, spec, dest) do
      {:ok, %{"dataset" => spec["name"], "path" => dest, "state" => "ready"}}
    end
  end

  defp ensure_prepared_derivatives(context, %{"name" => "simplewiki"}, dest),
    do: SimpleWiki.ensure_query_workload(context, dest)

  defp ensure_prepared_derivatives(_context, _spec, _dest), do: :ok

  defp replace_dataset(context, spec, profile, opts) do
    with :ok <- Root.remove_dataset!(context, spec["name"], spec["version"]) do
      do_prepare_dataset(context, spec, profile, opts)
    end
  end

  defp do_prepare_dataset(context, %{"name" => "trec-covid"} = spec, _profile, opts) do
    prepare_trec(context, spec, opts)
  end

  defp do_prepare_dataset(context, %{"name" => "pmc"} = spec, profile, opts) do
    prepare_object_dataset(context, spec, profile, opts)
  end

  defp do_prepare_dataset(context, %{"name" => "simplewiki"} = spec, profile, opts) do
    prepare_simplewiki(context, spec, profile, opts)
  end

  defp do_prepare_dataset(context, %{"name" => "open-images"} = spec, profile, opts) do
    prepare_object_dataset(context, spec, profile, opts)
  end

  defp prepare_trec(context, spec, opts) do
    unique = unique_id()

    with {:ok, staging} <- Root.staging_path(context, spec["name"], unique),
         {:ok, dest} <- Root.dataset_path(context, spec["name"], spec["version"]),
         {:ok, cache} <- Root.cache_path(context, spec["name"], spec["version"]),
         :ok <- File.mkdir_p(staging),
         :ok <- File.mkdir_p(cache),
         archive = Path.join(cache, spec["archive_name"]),
         :ok <- ensure_trec_archive(context, spec, archive, staging, opts),
         :ok <- Zip.extract!(context, archive, staging),
         :ok <- Beir.validate_fixture(staging, spec),
         :ok <- write_external_manifest(context, staging, trec_manifest(spec, archive)),
         :ok <-
           Marker.write(context, staging, %{
             "dataset" => spec["name"],
             "version" => spec["version"],
             "profile" => "standard"
           }),
         :ok <- Downloader.promote_dir(context, staging, dest) do
      {:ok, %{"dataset" => spec["name"], "path" => dest, "state" => "ready"}}
    end
  end

  defp prepare_simplewiki(context, spec, profile, opts) do
    unique = unique_id()

    with {:ok, staging} <- Root.staging_path(context, spec["name"], unique),
         {:ok, dest} <- Root.dataset_path(context, spec["name"], spec["version"]),
         {:ok, cache} <- Root.cache_path(context, spec["name"], spec["version"]),
         :ok <- File.mkdir_p(Path.join(staging, "objects")),
         :ok <- File.mkdir_p(cache),
         archive = Path.join(cache, spec["archive_name"]),
         :ok <- ensure_simplewiki_archive(context, spec, archive, staging, opts),
         {:ok, manifest} <-
           SimpleWiki.generate_fixture(
             archive,
             spec,
             profile,
             staging,
             article_count: Registry.selection_count("simplewiki", profile)
           ),
         :ok <- write_external_manifest(context, staging, manifest),
         :ok <-
           Marker.write(context, staging, %{
             "dataset" => spec["name"],
             "version" => spec["version"],
             "profile" => Atom.to_string(profile)
           }),
         :ok <- Downloader.promote_dir(context, staging, dest) do
      {:ok, %{"dataset" => spec["name"], "path" => dest, "state" => "ready"}}
    end
  end

  defp ensure_simplewiki_archive(context, spec, archive, staging, opts) do
    download = Keyword.get(opts, :download, &Downloader.download/3)

    if File.regular?(archive) do
      :ok
    else
      staging_archive = Path.join(staging, spec["archive_name"])

      with :ok <-
             download.(
               context,
               %{url: spec["source_url"], dest: staging_archive},
               opts
             ) do
        copy_inside(context, staging_archive, archive)
      end
    end
  end

  defp ensure_trec_archive(context, spec, archive, staging, opts) do
    download = Keyword.get(opts, :download, &Downloader.download/3)

    if File.regular?(archive) do
      Checksums.verify_file(archive, md5: spec["md5"], expected_size: spec["expected_size_bytes"])
    else
      staging_archive = Path.join(staging, spec["archive_name"])

      with :ok <-
             download.(
               context,
               %{
                 url: spec["source_url"],
                 dest: staging_archive,
                 md5: spec["md5"],
                 expected_size: spec["expected_size_bytes"]
               },
               opts
             ),
           :ok <- File.mkdir_p(Path.dirname(archive)) do
        copy_inside(context, staging_archive, archive)
      end
    end
  end

  defp prepare_object_dataset(context, spec, profile, opts) do
    unique = unique_id()

    Progress.with_run(prepare_progress_opts(opts), fn progress ->
      opts = Keyword.put(opts, :progress, progress)

      with {:ok, staging} <- Root.staging_path(context, spec["name"], unique),
           {:ok, dest} <- Root.dataset_path(context, spec["name"], spec["version"]),
           :ok <- File.mkdir_p(Path.join(staging, "objects")),
           {:ok, manifest} <- resolve_manifest(context, spec, profile, opts),
           :ok <- write_external_manifest(context, staging, manifest),
           :ok <- download_objects(context, spec, staging, manifest, opts),
           {:ok, manifest} <- finalize_object_manifest(spec, profile, staging, manifest),
           :ok <- write_external_manifest(context, staging, manifest),
           :ok <-
             Marker.write(context, staging, %{
               "dataset" => spec["name"],
               "version" => spec["version"],
               "profile" => Atom.to_string(profile)
             }),
           :ok <- Downloader.promote_dir(context, staging, dest) do
        Progress.complete(progress)
        {:ok, %{"dataset" => spec["name"], "path" => dest, "state" => "ready"}}
      end
    end)
  end

  defp resolve_manifest(context, %{"name" => "pmc"} = spec, :smoke, opts) do
    _ = context
    {:ok, Pmc.smoke_manifest(spec, opts)}
  end

  defp resolve_manifest(context, %{"name" => "pmc"} = spec, :standard, opts) do
    cond do
      is_map(opts[:manifest]) ->
        {:ok, opts[:manifest]}

      is_binary(opts[:metadata_jsonl]) ->
        Pmc.generate_from_jsonl(
          opts[:metadata_jsonl],
          spec,
          Registry.selection_count("pmc", :standard)
        )

      true ->
        PmcInventory.generate(context, spec, opts)
    end
  end

  defp resolve_manifest(context, %{"name" => "open-images"} = spec, :smoke, opts) do
    _ = context
    {:ok, OpenImages.smoke_manifest(spec, opts)}
  end

  defp resolve_manifest(context, %{"name" => "open-images"} = spec, profile, opts)
       when profile in [:standard, :k1, :k10] do
    cond do
      is_map(opts[:manifest]) ->
        {:ok, opts[:manifest]}

      is_binary(opts[:info_csv]) ->
        OpenImages.generate_from_info_csv(
          opts[:info_csv],
          spec,
          Registry.selection_count("open-images", profile),
          Atom.to_string(profile)
        )

      true ->
        generate_open_images_manifest(context, spec, profile, opts)
    end
  end

  defp generate_open_images_manifest(context, spec, profile, opts) do
    download = Keyword.get(opts, :download, &Downloader.download/3)
    count = open_images_pool_count(profile)

    with {:ok, cache} <- Root.cache_path(context, spec["name"], spec["version"]),
         :ok <- File.mkdir_p(cache),
         csv = Path.join(cache, "train-images-with-rotation.csv"),
         :ok <-
           maybe_download(download, context, spec["image_info_url"], csv, opts),
         {:ok, manifest} <-
           OpenImages.generate_from_info_csv(csv, spec, count, Atom.to_string(profile)) do
      {:ok, OpenImages.use_cvdf_bytes(manifest)}
    end
  end

  defp open_images_pool_count(profile) do
    wanted = Registry.selection_count("open-images", profile)
    wanted * 12
  end

  defp finalize_object_manifest(%{"name" => "open-images"}, profile, staging, manifest) do
    OpenImages.keep_downloaded(
      manifest,
      staging,
      Registry.selection_count("open-images", profile)
    )
  end

  defp finalize_object_manifest(_spec, _profile, _staging, manifest), do: {:ok, manifest}

  defp maybe_download(download, context, url, dest, opts) do
    if File.regular?(dest) do
      :ok
    else
      download.(context, %{url: url, dest: dest}, opts)
    end
  end

  defp download_objects(context, spec, staging, manifest, opts) do
    download = Keyword.get(opts, :download, &Downloader.download/3)
    concurrency = download_concurrency(opts)
    opts = object_download_opts(spec, opts)

    objects =
      spec["name"]
      |> objects_of(manifest)
      |> Enum.map(&Map.put(&1, :dest, Path.join([staging, "objects", &1.dest_name])))

    with :ok <- prepare_object_destinations(context, objects) do
      download_object_batches(context, spec, staging, objects, download, concurrency, opts)
    end
  end

  defp object_download_opts(%{"name" => "open-images"}, opts) do
    wanted = Registry.selection_count("open-images", Keyword.get(opts, :profile, :standard))

    opts
    |> Keyword.put_new(:max_retries, 3)
    |> Keyword.put_new(:receive_timeout, 30_000)
    |> Keyword.put(:object_limit, wanted)
  end

  defp object_download_opts(_spec, opts), do: opts

  defp prepare_progress_opts(opts) do
    [
      label: "prepare",
      stall_timeout_ms: Keyword.get(opts, :stall_timeout_ms, Progress.default_stall_timeout_ms()),
      printer: Keyword.get(opts, :progress_printer)
    ]
  end

  defp download_concurrency(opts) do
    value = Keyword.get(opts, :max_concurrency, 4)

    cond do
      not is_integer(value) -> 4
      value < 1 -> 1
      value > 16 -> 16
      true -> value
    end
  end

  defp prepare_object_destinations(context, objects) do
    Enum.reduce_while(objects, :ok, fn object, :ok ->
      prepare_one_destination(context, object)
    end)
  end

  defp prepare_one_destination(context, object) do
    relative = Path.relative_to(object.dest, context.root)

    case Root.resolve(context, Path.split(relative)) do
      {:ok, _} -> mkdir_object_dir(object.dest)
      {:error, reason} -> {:halt, {:error, reason}}
    end
  end

  defp mkdir_object_dir(dest) do
    case File.mkdir_p(Path.dirname(dest)) do
      :ok -> {:cont, :ok}
      {:error, reason} -> {:halt, {:error, "failed to create #{dest}: #{inspect(reason)}"}}
    end
  end

  defp download_object_batches(context, spec, staging, objects, download, concurrency, opts) do
    wanted = opts[:object_limit] || length(objects)
    Progress.phase(opts[:progress], "download_objects", wanted)

    with :ok <- Downloader.ensure_started() do
      objects
      |> Enum.chunk_every(64)
      |> Enum.reduce_while(:ok, fn chunk, :ok ->
        result =
          download_object_chunk(context, spec, staging, chunk, download, concurrency, opts)

        stop_when_enough(result, staging, opts[:object_limit])
      end)
    end
  end

  defp download_object_chunk(context, spec, staging, chunk, download, concurrency, opts) do
    case space_preflight(context, spec, staging, opts) do
      :ok -> run_object_chunk(context, spec, chunk, download, concurrency, opts)
      {:error, reason} -> {:halt, {:error, reason}}
    end
  end

  defp run_object_chunk(context, spec, chunk, download, concurrency, opts) do
    skip_missing? = spec["name"] == "open-images"

    chunk
    |> Task.async_stream(
      fn object -> download.(context, object, opts) end,
      max_concurrency: concurrency,
      timeout: :infinity,
      ordered: false
    )
    |> Enum.reduce_while(:ok, &join_download_task(&1, &2, skip_missing?, opts[:progress]))
    |> continue_download()
  end

  defp join_download_task({:ok, :ok}, :ok, _skip_missing?, progress) do
    Progress.tick(progress)
    {:cont, :ok}
  end

  defp join_download_task({:ok, {:error, reason}}, :ok, true, _progress) do
    if skippable_object_error?(reason), do: {:cont, :ok}, else: {:halt, {:error, reason}}
  end

  defp join_download_task({:ok, {:error, reason}}, :ok, _skip_missing?, _progress),
    do: {:halt, {:error, reason}}

  defp join_download_task({:exit, reason}, :ok, _skip_missing?, _progress),
    do: {:halt, {:error, "download task crashed: #{inspect(reason)}"}}

  defp skippable_object_error?(reason) do
    text = if is_binary(reason), do: reason, else: inspect(reason)

    String.contains?(text, "HTTP 404") or String.contains?(text, "HTTP 410") or
      String.contains?(text, "timeout")
  end

  defp continue_download(:ok), do: {:cont, :ok}
  defp continue_download({:error, reason}), do: {:halt, {:error, reason}}

  defp stop_when_enough({:cont, :ok}, staging, wanted)
       when is_integer(wanted) and wanted > 0 do
    if downloaded_object_count(staging) >= wanted, do: {:halt, :ok}, else: {:cont, :ok}
  end

  defp stop_when_enough(result, _staging, _wanted), do: result

  defp downloaded_object_count(staging) do
    case File.ls(Path.join(staging, "objects")) do
      {:ok, entries} -> Enum.count(entries, &String.ends_with?(&1, ".jpg"))
      _ -> 0
    end
  end

  defp objects_of("pmc", manifest), do: Pmc.objects_for(manifest)
  defp objects_of("open-images", manifest), do: OpenImages.objects_for(manifest)

  defp space_preflight(context, spec, staging, opts) do
    required = remaining_requirement(spec, staging, opts)
    fun = Keyword.get(opts, :available_bytes_fun, &DiskSpace.available_bytes/1)

    case fun.(context.root) do
      {:ok, available} when available >= required ->
        :ok

      {:ok, available} ->
        {:error,
         "insufficient free space: need #{required} bytes, #{available} bytes available at #{context.root}"}

      {:error, reason} ->
        {:error, "could not query free space: #{inspect(reason)}"}
    end
  end

  defp remaining_requirement(spec, staging, opts) do
    used = Statistics.directory_bytes(staging)
    {source, working} = estimates(spec, Keyword.get(opts, :profile, :standard))
    DiskSpace.required_bytes(max(source - used, 0), working)
  end

  defp preflight(context, spec, profile, opts) do
    {source, working} = estimates(spec, profile)
    required = DiskSpace.required_bytes(source, working)
    fun = Keyword.get(opts, :available_bytes_fun, &DiskSpace.available_bytes/1)

    case fun.(context.root) do
      {:ok, available} when available >= required ->
        :ok

      {:ok, available} ->
        {:error,
         "insufficient free space: need #{required} bytes, #{available} bytes available at #{context.root}"}

      {:error, reason} ->
        {:error, "could not query free space: #{inspect(reason)}"}
    end
  end

  defp estimates(_spec, :smoke), do: {32 * 1024 * 1024, 128 * 1024 * 1024}

  defp estimates(%{"name" => "trec-covid"} = spec, _profile) do
    {spec["estimated_source_bytes"] || 0, spec["estimated_working_bytes"] || 0}
  end

  defp estimates(%{"name" => name} = spec, profile) do
    scale_estimates(spec, Registry.selection_count(name, profile), 100_000)
  end

  defp scale_estimates(spec, count, standard_count) do
    source = spec["estimated_source_bytes"] || 0
    working = spec["estimated_working_bytes"] || 0
    {div(source * count, standard_count), div(working * count, standard_count)}
  end

  defp available(context, opts) do
    fun = Keyword.get(opts, :available_bytes_fun, &DiskSpace.available_bytes/1)
    fun.(context.root)
  end

  defp dataset_status(context, spec) do
    case Root.dataset_path(context, spec["name"], spec["version"]) do
      {:ok, path} ->
        state = fixture_state(path)
        local_bytes = Statistics.directory_bytes(path)

        %{
          "name" => spec["name"],
          "version" => spec["version"],
          "title" => spec["title"],
          "expected_source_bytes" => spec["estimated_source_bytes"],
          "source_bytes_estimated?" => spec["source_bytes_estimated?"],
          "estimated_working_bytes" => spec["estimated_working_bytes"],
          "local_bytes" => local_bytes,
          "state" => state,
          "path" => path
        }

      {:error, reason} ->
        %{
          "name" => spec["name"],
          "state" => "error",
          "error" => reason
        }
    end
  end

  defp fixture_state(path) do
    cond do
      Marker.present?(path) ->
        if corrupt_ready?(path), do: "corrupt", else: "ready"

      File.dir?(path) ->
        "partial"

      true ->
        "missing"
    end
  end

  defp corrupt_ready?(path) do
    not File.regular?(Path.join(path, "manifest.json"))
  end

  defp trec_manifest(spec, archive) do
    {:ok, md5} = Checksums.md5_file(archive)

    %{
      "dataset" => spec["name"],
      "version" => spec["version"],
      "source" => spec["source_url"],
      "expected_size_bytes" => spec["expected_size_bytes"],
      "md5" => md5
    }
  end

  defp write_external_manifest(context, dir, manifest) do
    path = Path.join(dir, "manifest.json")

    with :ok <-
           if(Root.descendant?(path, context.root),
             do: :ok,
             else: {:error, "manifest path escapes benchmark root"}
           ) do
      case AtomicWrite.write(path, JSON.encode!(manifest) <> "\n") do
        :ok -> :ok
        {:error, reason} -> {:error, "failed to write manifest: #{inspect(reason)}"}
      end
    end
  end

  defp copy_inside(context, from, to) do
    with :ok <- assert_inside(context, from),
         :ok <- assert_inside(context, to) do
      case File.cp(from, to) do
        :ok -> :ok
        {:error, reason} -> {:error, "copy failed: #{inspect(reason)}"}
      end
    end
  end

  defp assert_inside(context, path) do
    if Root.descendant?(Path.expand(path), context.root) do
      :ok
    else
      {:error, "path #{path} is outside #{context.root}"}
    end
  end

  defp unique_id do
    Integer.to_string(System.unique_integer([:positive]))
  end
end
