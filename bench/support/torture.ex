defmodule VialKeeper.Bench.Torture do
  @moduledoc "Open Images attachment torture benchmark."

  alias VialKeeper.Attachments

  alias VialKeeper.Bench.{
    IO,
    Marker,
    OpenImages,
    Progress,
    Registry,
    Reports,
    Root,
    Runtime,
    Statistics
  }

  alias VialKeeper.Documents
  alias VialKeeper.Runtime.DatabaseCatalog

  @write_conc [1, 4, 8, 16]
  @read_conc [1, 4, 16, 64]
  @retryable_attempts 40

  @spec run(keyword()) :: {:ok, map()} | {:error, binary()}
  def run(opts \\ []) do
    with {:ok, context} <- Root.load(opts),
         {:ok, spec} <- Registry.fetch("open-images"),
         {:ok, profile} <- Registry.profile(opts),
         :ok <- Registry.ensure_profile("open-images", profile),
         {:ok, dataset} <- require_ready(context, spec),
         {:ok, manifest} <- read_manifest(dataset),
         opts <- Keyword.put(opts, :profile, profile),
         {:ok, images} <- select_images(manifest, opts) do
      Runtime.with_isolated(context, fn ->
        measure(context, spec, dataset, manifest, images, opts)
      end)
    end
  end

  @doc """
  Restricts a prepared Open Images manifest to the requested torture subset.

  `--limit N` wins. Otherwise `--profile smoke|1k|10k` takes that many images
  from the fixture. `standard` uses the whole prepared manifest.
  """
  @spec select_images(map(), keyword()) :: {:ok, [map()]} | {:error, binary()}
  def select_images(manifest, opts) when is_map(manifest) and is_list(opts) do
    images = manifest["images"] || []
    wanted = requested_image_count(opts)

    cond do
      wanted == :all ->
        {:ok, images}

      not is_list(images) ->
        {:error, "Open Images manifest images are invalid"}

      length(images) < wanted ->
        {:error, "open-images fixture has #{length(images)} images; need #{wanted}"}

      true ->
        {:ok, Enum.take(images, wanted)}
    end
  end

  @doc """
  Retries a retryable VialKeeper error so concurrent attachment writes wait for
  coordinator slots instead of aborting the suite.
  """
  @spec retry_retryable((-> result)) :: result
        when result: {:ok, term()} | {:error, VialKeeper.Error.t()}
  def retry_retryable(fun) when is_function(fun, 0), do: retry_retryable(fun, 0)

  defp requested_image_count(opts) do
    cond do
      is_integer(opts[:limit]) and opts[:limit] > 0 ->
        opts[:limit]

      opts[:profile] in [:k1, :k10, :smoke] ->
        Registry.selection_count("open-images", opts[:profile])

      true ->
        :all
    end
  end

  defp require_ready(context, spec) do
    with {:ok, path} <- Root.dataset_path(context, spec["name"], spec["version"]) do
      if Marker.present?(path),
        do: {:ok, path},
        else: {:error, "dataset open-images is not ready; run mix bench.data prepare open-images"}
    end
  end

  defp read_manifest(dataset) do
    case File.read(Path.join(dataset, "manifest.json")) do
      {:ok, body} ->
        case JSON.decode(body) do
          {:ok, manifest} when is_map(manifest) -> {:ok, manifest}
          _ -> {:error, "Open Images manifest is invalid"}
        end

      {:error, reason} ->
        {:error, "cannot read Open Images manifest: #{inspect(reason)}"}
    end
  end

  defp measure(context, spec, dataset, manifest, images, opts) do
    run_id = Integer.to_string(System.unique_integer([:positive]))
    profile = Keyword.get(opts, :profile, :standard)

    with {:ok, work} <- Root.work_run_path(context, "open-images", run_id),
         {:ok, uuid, relative} <- Runtime.create_work_database(context, "open-images", run_id) do
      Progress.with_run(progress_opts(opts, context, uuid, relative), fn progress ->
        opts = Keyword.put(opts, :progress, progress)

        try do
          configure_attachment_limits!(uuid)
          ingest = attachment_ingest(uuid, dataset, images, progress)
          writes = concurrent_write(uuid, dataset, images, progress)
          reads = concurrent_read(uuid, ingest["items"], opts, progress)
          dedup = dedup(uuid, dataset, ingest["items"], progress)
          gc = delete_and_gc(uuid, ingest["items"], progress)
          mixed = mixed_torture(uuid, dataset, ingest["items"], opts, progress)
          Progress.complete(progress)
          {:ok, hash} = Marker.manifest_hash(dataset)

          report =
            Reports.envelope(
              context,
              %{
                "benchmark" => "torture",
                "dataset" => spec["name"],
                "dataset_version" => spec["version"],
                "profile" => Atom.to_string(profile),
                "selection_count" => length(images),
                "fixture_image_count" => length(manifest["images"] || []),
                "fixture_manifest_hash" => hash,
                "dataset_path" => dataset,
                "work_path" => work
              },
              %{
                "attachment_ingest" => ingest["stats"],
                "concurrent_write" => writes,
                "concurrent_read" => reads,
                "dedup" => dedup,
                "delete_and_gc" => gc,
                "mixed_torture" => mixed
              }
            )

          Reports.write(context, "open-images-torture.json", report, opts)
        after
          Runtime.close_work_database(context, uuid, relative)
        end
      end)
    end
  end

  defp attachment_ingest(uuid, dataset, images, progress) do
    started = System.monotonic_time(:microsecond)
    before = Statistics.snapshot_bytes(uuid, :bundle, :before_ingest)
    Progress.phase(progress, "attachment_ingest", length(images))

    items =
      Enum.map(images, fn image ->
        item = ingest_image(uuid, dataset, image)
        Progress.tick(progress)
        item
      end)

    elapsed = System.monotonic_time(:microsecond) - started
    after_bytes = Statistics.snapshot_bytes(uuid, :bundle, :after_ingest)
    count = length(items)
    logical = Enum.reduce(items, 0, &(&1.bytes + &2))

    %{
      "items" => items,
      "stats" => %{
        "images" => count,
        "elapsed_us" => elapsed,
        "images_per_sec" => Statistics.per_sec(count, elapsed),
        "mib_per_sec" => Statistics.mib_per_sec(logical, elapsed),
        "logical_source_bytes" => logical,
        "physical_cas_bytes" => Statistics.cas_bytes(uuid),
        "storage_amplification" => after_bytes - before,
        "database_bytes" => after_bytes
      }
    }
  end

  defp ingest_image(uuid, dataset, image) do
    path = Path.join([dataset, "objects", image["image_id"] <> ".jpg"])

    {:ok, %{blob: digest, deduplicated?: dedup?}} = upload_stream(uuid, path)

    body = OpenImages.document_body(image)

    {:ok, put} =
      Documents.put(uuid, %{
        "id" => image["image_id"],
        "body" => body,
        "attachments" => %{
          "image.jpg" => %{"blob" => digest, "content_type" => "image/jpeg"}
        }
      })

    %{
      id: image["image_id"],
      digest: digest,
      bytes: Statistics.file_size(path),
      path: path,
      revision: put.revision,
      deduplicated?: dedup?
    }
  end

  defp concurrent_write(uuid, dataset, images, progress) do
    Enum.map(@write_conc, fn concurrency ->
      Progress.phase(progress, "concurrent_write:#{concurrency}", length(images))

      elapsed =
        Statistics.timed_us(fn -> write_many(uuid, dataset, images, concurrency, progress) end)

      Map.merge(Statistics.summarize([elapsed], length(images)), %{"concurrency" => concurrency})
    end)
  end

  defp write_many(uuid, dataset, images, concurrency, progress) do
    images
    |> Enum.with_index()
    |> Task.async_stream(
      fn {image, index} ->
        _ = write_one(uuid, dataset, image, concurrency, index)
        Progress.tick(progress)
      end,
      max_concurrency: concurrency,
      timeout: :infinity,
      ordered: false
    )
    |> Stream.run()
  end

  defp write_one(uuid, dataset, image, concurrency, index) do
    path = Path.join([dataset, "objects", image["image_id"] <> ".jpg"])
    {:ok, %{blob: digest}} = upload_stream(uuid, path)

    {:ok, _} =
      Documents.put(uuid, %{
        "id" => "cw-#{concurrency}-#{index}",
        "body" => %{"image_id" => image["image_id"]},
        "attachments" => %{
          "image.jpg" => %{"blob" => digest, "content_type" => "image/jpeg"}
        }
      })
  end

  defp concurrent_read(_uuid, [], _opts, _progress), do: %{"skipped" => true}

  defp concurrent_read(uuid, items, opts, progress) do
    iterations = Keyword.get(opts, :iterations, 3)

    Enum.map(@read_conc, fn concurrency ->
      read_concurrency_row(uuid, items, concurrency, iterations, progress)
    end)
  end

  defp read_concurrency_row(uuid, items, concurrency, iterations, progress) do
    count = length(items)
    Progress.phase(progress, "concurrent_read:#{concurrency}", count * iterations)

    samples =
      Enum.map(1..iterations, fn _ ->
        {elapsed, bytes} = :timer.tc(fn -> read_many(uuid, items, concurrency, progress) end)
        {elapsed, bytes}
      end)

    elapsed = Enum.map(samples, &elem(&1, 0))
    bytes = Enum.reduce(samples, 0, &(elem(&1, 1) + &2))

    Map.merge(Statistics.summarize(elapsed, count), %{
      "concurrency" => concurrency,
      "successful_bytes" => bytes,
      "mib_per_sec" => Statistics.mib_per_sec(bytes, Enum.sum(elapsed))
    })
  end

  defp read_many(uuid, items, concurrency, progress) do
    items
    |> Task.async_stream(
      fn item ->
        bytes =
          case Attachments.open_stream(uuid, %{
                 "id" => item.id,
                 "revision" => nil,
                 "name" => "image.jpg"
               }) do
            {:ok, stream} -> IO.consume_stream(stream.body)
            {:error, _} -> 0
          end

        Progress.tick(progress)
        bytes
      end,
      max_concurrency: concurrency,
      timeout: :infinity,
      ordered: false
    )
    |> Enum.reduce(0, fn {:ok, bytes}, acc -> acc + bytes end)
  end

  defp dedup(_uuid, _dataset, [], _progress), do: %{"skipped" => true}

  defp dedup(uuid, _dataset, items, progress) do
    subset = Enum.take(items, max(1, div(length(items), 2)))
    before = Statistics.snapshot_bytes(uuid, :cas, :before_dedup)
    started = System.monotonic_time(:microsecond)
    Progress.phase(progress, "dedup", length(subset))

    Enum.each(Enum.with_index(subset), fn {item, index} ->
      {:ok, %{blob: digest, deduplicated?: dedup?}} = upload_stream(uuid, item.path)

      {:ok, _} =
        Documents.put(uuid, %{
          "id" => "dedup-#{index}",
          "body" => %{"source" => item.id, "deduplicated" => dedup?},
          "attachments" => %{
            "copy.jpg" => %{"blob" => digest, "content_type" => "image/jpeg"}
          }
        })

      Progress.tick(progress)
    end)

    elapsed = System.monotonic_time(:microsecond) - started
    after_bytes = Statistics.snapshot_bytes(uuid, :cas, :after_dedup)

    %{
      "elapsed_us" => elapsed,
      "logical_bytes_added" => Enum.reduce(subset, 0, &(&1.bytes + &2)),
      "physical_bytes_added" => after_bytes - before,
      "physical_storage_increased" => after_bytes > before
    }
  end

  defp delete_and_gc(_uuid, [], _progress), do: %{"skipped" => true}

  defp delete_and_gc(uuid, items, progress) do
    drop = Enum.take(items, max(1, div(length(items), 3)))
    before = Statistics.snapshot_bytes(uuid, :cas, :before_gc)
    Progress.phase(progress, "delete", length(drop))

    {delete_us, _} =
      :timer.tc(fn ->
        Enum.each(drop, fn item ->
          _ = Documents.delete(uuid, %{"id" => item.id})
          Progress.tick(progress)
        end)
      end)

    Progress.phase(progress, "gc", 1)
    {gc_us, gc} = :timer.tc(fn -> Attachments.gc(uuid) end)
    Progress.tick(progress)
    after_bytes = Statistics.snapshot_bytes(uuid, :cas, :after_gc)

    stats =
      case gc do
        {:ok, map} -> map
        _ -> %{}
      end

    %{
      "delete_elapsed_us" => delete_us,
      "gc_elapsed_us" => gc_us,
      "blobs_reclaimed" => Map.get(stats, :blobs_deleted, Map.get(stats, :deleted, 0)),
      "bytes_reclaimed" => Map.get(stats, :bytes_deleted, 0),
      "storage_before_bytes" => before,
      "storage_after_bytes" => after_bytes
    }
  end

  defp mixed_torture(uuid, dataset, items, _opts, progress) do
    Progress.phase(progress, "mixed_torture", 64)

    {elapsed, _} =
      :timer.tc(fn ->
        1..64
        |> Task.async_stream(
          fn n ->
            _ = mixed_op(uuid, dataset, items, n)
            Progress.tick(progress)
          end,
          max_concurrency: 8,
          timeout: :infinity,
          ordered: false
        )
        |> Stream.run()
      end)

    %{"elapsed_us" => elapsed, "operations" => 64, "concurrency" => 8}
  end

  defp mixed_op(uuid, _dataset, items, n) do
    item = Enum.at(items, rem(n, max(length(items), 1))) || List.first(items)

    if is_map(item) do
      mixed_item_op(uuid, item, rem(n, 5), n)
    else
      :ok
    end
  end

  defp mixed_item_op(uuid, item, 0, _n), do: upload_stream(uuid, item.path)

  defp mixed_item_op(uuid, item, 1, _n) do
    case Attachments.open_stream(uuid, %{
           "id" => item.id,
           "revision" => nil,
           "name" => "image.jpg"
         }) do
      {:ok, stream} -> IO.consume_stream(stream.body)
      {:error, _} -> 0
    end
  end

  defp mixed_item_op(uuid, item, 2, _n), do: Documents.get(uuid, %{"id" => item.id})

  defp mixed_item_op(uuid, _item, 3, n) do
    Documents.put(uuid, %{"id" => "mixed-#{n}", "body" => %{"n" => n}})
  end

  defp mixed_item_op(uuid, _item, _op, n) do
    Documents.delete(uuid, %{"id" => "mixed-#{n - 1}"})
  end

  defp progress_opts(opts, context, uuid, relative) do
    [
      label: "torture",
      stall_timeout_ms: Keyword.get(opts, :stall_timeout_ms, Progress.default_stall_timeout_ms()),
      cleanup: fn -> Runtime.close_work_database(context, uuid, relative) end
    ]
  end

  defp configure_attachment_limits!(uuid) do
    case DatabaseCatalog.command(
           uuid,
           {:command, :update_config,
            %{
              "attachments" => %{
                "max_concurrent_attachment_writes" => Enum.max(@write_conc),
                "max_concurrent_attachment_reads" => Enum.max(@read_conc)
              }
            }}
         ) do
      {:ok, _} -> :ok
      {:error, error} -> Mix.raise("cannot configure attachment limits: #{inspect(error)}")
    end
  end

  defp upload_stream(uuid, path) do
    retry_retryable(fn -> Attachments.upload_stream(uuid, IO.file_chunks(path)) end)
  end

  defp retry_retryable(fun, attempt) do
    case fun.() do
      {:ok, _} = ok ->
        ok

      {:error, %VialKeeper.Error{retryable: true}} when attempt + 1 < @retryable_attempts ->
        Process.sleep(min(15 * (attempt + 1), 200))
        retry_retryable(fun, attempt + 1)

      other ->
        other
    end
  end
end
