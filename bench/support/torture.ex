defmodule VialKeeper.Bench.Torture do
  @moduledoc "Open Images attachment torture benchmark."

  alias VialKeeper.Attachments
  alias VialKeeper.Bench.{IO, Marker, OpenImages, Registry, Reports, Root, Runtime, Statistics}
  alias VialKeeper.Documents

  @write_conc [1, 4, 8, 16]
  @read_conc [1, 4, 16, 64]

  @spec run(keyword()) :: {:ok, map()} | {:error, binary()}
  def run(opts \\ []) do
    with {:ok, context} <- Root.load(opts),
         {:ok, spec} <- Registry.fetch("open-images"),
         {:ok, dataset} <- require_ready(context, spec),
         {:ok, manifest} <- read_manifest(dataset) do
      Runtime.with_isolated(context, fn ->
        measure(context, spec, dataset, manifest, opts)
      end)
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

  defp measure(context, spec, dataset, manifest, opts) do
    run_id = Integer.to_string(System.unique_integer([:positive]))

    with {:ok, work} <- Root.work_run_path(context, "open-images", run_id),
         {:ok, uuid, relative} <- Runtime.create_work_database(context, "open-images", run_id) do
      try do
        ingest = attachment_ingest(uuid, dataset, manifest)
        writes = concurrent_write(uuid, dataset, manifest, opts)
        reads = concurrent_read(uuid, ingest["items"], opts)
        dedup = dedup(uuid, dataset, ingest["items"])
        gc = delete_and_gc(uuid, ingest["items"])
        mixed = mixed_torture(uuid, dataset, ingest["items"], opts)
        {:ok, hash} = Marker.manifest_hash(dataset)

        report =
          Reports.envelope(
            context,
            %{
              "benchmark" => "torture",
              "dataset" => spec["name"],
              "dataset_version" => spec["version"],
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
    end
  end

  defp attachment_ingest(uuid, dataset, manifest) do
    images = manifest["images"] || []
    started = System.monotonic_time(:microsecond)
    before = Statistics.snapshot_bytes(uuid, :bundle, :before_ingest)

    items =
      Enum.map(images, fn image ->
        ingest_image(uuid, dataset, image)
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

    {:ok, %{blob: digest, deduplicated?: dedup?}} =
      Attachments.upload_stream(uuid, IO.file_chunks(path))

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

  defp concurrent_write(uuid, dataset, manifest, _opts) do
    images = manifest["images"] || []

    Enum.map(@write_conc, fn concurrency ->
      elapsed = Statistics.timed_us(fn -> write_many(uuid, dataset, images, concurrency) end)
      Map.merge(Statistics.summarize([elapsed], length(images)), %{"concurrency" => concurrency})
    end)
  end

  defp write_many(uuid, dataset, images, concurrency) do
    images
    |> Enum.with_index()
    |> Task.async_stream(
      fn {image, index} -> write_one(uuid, dataset, image, concurrency, index) end,
      max_concurrency: concurrency,
      timeout: :infinity,
      ordered: false
    )
    |> Stream.run()
  end

  defp write_one(uuid, dataset, image, concurrency, index) do
    path = Path.join([dataset, "objects", image["image_id"] <> ".jpg"])
    {:ok, %{blob: digest}} = Attachments.upload_stream(uuid, IO.file_chunks(path))

    Documents.put(uuid, %{
      "id" => "cw-#{concurrency}-#{index}",
      "body" => %{"image_id" => image["image_id"]},
      "attachments" => %{
        "image.jpg" => %{"blob" => digest, "content_type" => "image/jpeg"}
      }
    })
  end

  defp concurrent_read(_uuid, [], _opts), do: %{"skipped" => true}

  defp concurrent_read(uuid, items, opts) do
    iterations = Keyword.get(opts, :iterations, 3)

    Enum.map(@read_conc, fn concurrency ->
      read_concurrency_row(uuid, items, concurrency, iterations)
    end)
  end

  defp read_concurrency_row(uuid, items, concurrency, iterations) do
    samples =
      Enum.map(1..iterations, fn _ ->
        {elapsed, bytes} = :timer.tc(fn -> read_many(uuid, items, concurrency) end)
        {elapsed, bytes}
      end)

    elapsed = Enum.map(samples, &elem(&1, 0))
    bytes = Enum.reduce(samples, 0, &(elem(&1, 1) + &2))

    Map.merge(Statistics.summarize(elapsed, length(items)), %{
      "concurrency" => concurrency,
      "successful_bytes" => bytes,
      "mib_per_sec" => Statistics.mib_per_sec(bytes, Enum.sum(elapsed))
    })
  end

  defp read_many(uuid, items, concurrency) do
    items
    |> Task.async_stream(
      fn item ->
        case Attachments.open_stream(uuid, %{
               "id" => item.id,
               "revision" => nil,
               "name" => "image.jpg"
             }) do
          {:ok, stream} -> IO.consume_stream(stream.body)
          {:error, _} -> 0
        end
      end,
      max_concurrency: concurrency,
      timeout: :infinity,
      ordered: false
    )
    |> Enum.reduce(0, fn {:ok, bytes}, acc -> acc + bytes end)
  end

  defp dedup(_uuid, _dataset, []), do: %{"skipped" => true}

  defp dedup(uuid, _dataset, items) do
    subset = Enum.take(items, max(1, div(length(items), 2)))
    before = Statistics.snapshot_bytes(uuid, :cas, :before_dedup)
    started = System.monotonic_time(:microsecond)

    Enum.each(Enum.with_index(subset), fn {item, index} ->
      {:ok, %{blob: digest, deduplicated?: dedup?}} =
        Attachments.upload_stream(uuid, IO.file_chunks(item.path))

      {:ok, _} =
        Documents.put(uuid, %{
          "id" => "dedup-#{index}",
          "body" => %{"source" => item.id, "deduplicated" => dedup?},
          "attachments" => %{
            "copy.jpg" => %{"blob" => digest, "content_type" => "image/jpeg"}
          }
        })
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

  defp delete_and_gc(_uuid, []), do: %{"skipped" => true}

  defp delete_and_gc(uuid, items) do
    drop = Enum.take(items, max(1, div(length(items), 3)))
    before = Statistics.snapshot_bytes(uuid, :cas, :before_gc)

    {delete_us, _} =
      :timer.tc(fn ->
        Enum.each(drop, fn item ->
          Documents.delete(uuid, %{"id" => item.id})
        end)
      end)

    {gc_us, gc} = :timer.tc(fn -> Attachments.gc(uuid) end)
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

  defp mixed_torture(uuid, dataset, items, _opts) do
    {elapsed, _} =
      :timer.tc(fn ->
        1..64
        |> Task.async_stream(
          fn n ->
            mixed_op(uuid, dataset, items, n)
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

  defp mixed_item_op(uuid, item, 0, _n),
    do: Attachments.upload_stream(uuid, IO.file_chunks(item.path))

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
end
