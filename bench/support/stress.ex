defmodule VialKeeper.Bench.Stress do
  @moduledoc """
  Phase-separated Simple English Wikipedia stress benchmark.

  Corpus preparation uses bounded public bulk operations. Interactive puts,
  physical attachment installation, attachment reference mutations, full-text
  build/search, reads, and the mixed workload are measured independently.
  Progress is checkpointed atomically after every bounded batch.
  """

  alias VialKeeper.Attachments

  alias VialKeeper.Bench.{
    IO,
    Marker,
    Prepare,
    Registry,
    Reports,
    Root,
    Runtime,
    SimpleWiki,
    Statistics
  }

  alias VialKeeper.Documents
  alias VialKeeper.Query

  @index_name "simplewiki-text"
  @search_concurrencies [1, 4, 16]
  @mixed_concurrencies [4, 16, 32]
  @query_algorithm "simplewiki-query-v2"
  @bulk_batch_size 500
  @attachment_batch_size 500
  @single_document_count 1_000
  @scaling_checkpoints [100, 1_000, 10_000, 25_000, 50_000, 100_000]
  @search_batch_event [:vial_keeper, :search, :rebuild, :batch]

  @type progress_callback :: (non_neg_integer(), non_neg_integer(), non_neg_integer() -> :ok)

  @spec run(keyword()) :: {:ok, binary()} | {:error, binary()}
  def run(opts \\ []) do
    with {:ok, context} <- Root.load_or_configure(opts),
         {:ok, spec} <- Registry.fetch("simplewiki"),
         {:ok, _prepared} <- Prepare.prepare("simplewiki", opts),
         {:ok, dataset} <- require_ready(context, spec),
         {:ok, manifest} <- read_manifest(dataset),
         {:ok, query_workload} <- SimpleWiki.load_query_workload(dataset) do
      Runtime.with_isolated(context, fn ->
        measure(context, spec, dataset, manifest, query_workload, opts)
      end)
    end
  end

  defp require_ready(context, spec) do
    with {:ok, path} <- Root.dataset_path(context, spec["name"], spec["version"]) do
      if Marker.present?(path),
        do: {:ok, path},
        else: {:error, "dataset simplewiki is not ready; run mix bench.data prepare simplewiki"}
    end
  end

  defp read_manifest(dataset) do
    path = Path.join(dataset, "manifest.json")

    case File.read(path) do
      {:ok, body} ->
        case JSON.decode(body) do
          {:ok, manifest} when is_map(manifest) -> {:ok, manifest}
          _ -> {:error, "SimpleWiki manifest is invalid"}
        end

      {:error, reason} ->
        {:error, "cannot read SimpleWiki manifest: #{inspect(reason)}"}
    end
  end

  defp measure(context, spec, dataset, manifest, query_workload, opts) do
    run_id = Integer.to_string(System.unique_integer([:positive]))

    with {:ok, work} <- Root.work_run_path(context, "simplewiki", run_id),
         {:ok, uuid, relative} <- Runtime.create_work_database(context, "simplewiki", run_id),
         {:ok, hash} <- Marker.manifest_hash(dataset) do
      base =
        Reports.envelope(
          context,
          %{
            "benchmark" => "stress",
            "dataset" => spec["name"],
            "dataset_version" => spec["version"],
            "fixture_manifest_hash" => hash,
            "dataset_path" => dataset,
            "work_path" => work,
            "query_algorithm" => @query_algorithm,
            "started_at" => timestamp()
          },
          %{}
        )

      try do
        write_state!(context, base, %{}, [], "running", "single_document_ingest", nil, opts)

        single_progress =
          progress_callback(
            context,
            base,
            %{},
            [],
            "single_document_ingest",
            opts
          )

        single =
          single_document_ingest(context, run_id, dataset, manifest, single_progress, opts)

        results = %{"single_document_ingest" => single}
        completed = ["single_document_ingest"]
        write_completed!(context, base, results, completed, opts)

        bulk_progress =
          progress_callback(context, base, results, completed, "bulk_document_ingest", opts)

        bulk = bulk_document_ingest(uuid, dataset, manifest, bulk_progress, opts)
        results = Map.put(results, "bulk_document_ingest", bulk.stats)
        completed = completed ++ ["bulk_document_ingest"]
        write_completed!(context, base, results, completed, opts)

        attachment_progress =
          progress_callback(
            context,
            base,
            results,
            completed,
            "attachment_physical_ingest",
            opts
          )

        physical =
          attachment_physical_ingest(uuid, bulk.attachments, attachment_progress, opts)

        results = Map.put(results, "attachment_physical_ingest", physical.stats)
        completed = completed ++ ["attachment_physical_ingest"]
        write_completed!(context, base, results, completed, opts)

        reference_progress =
          progress_callback(
            context,
            base,
            results,
            completed,
            "attachment_reference_mutation",
            opts
          )

        reference =
          attachment_reference_mutation(
            uuid,
            physical.references,
            bulk.revisions,
            reference_progress,
            opts
          )

        results = Map.put(results, "attachment_reference_mutation", reference)
        completed = completed ++ ["attachment_reference_mutation"]
        write_completed!(context, base, results, completed, opts)

        fts_progress =
          progress_callback(context, base, results, completed, "fts_build", opts)

        fts = fts_build(uuid, length(manifest["articles"] || []), fts_progress)
        results = Map.put(results, "fts_build", fts)
        completed = completed ++ ["fts_build"]
        write_completed!(context, base, results, completed, opts)

        queries = query_workload["queries"]

        search =
          measured_phase(context, base, results, completed, "fts_search", opts, fn ->
            fts_search(uuid, queries, opts)
          end)

        results = Map.put(results, "fts_search", search)
        completed = completed ++ ["fts_search"]
        write_completed!(context, base, results, completed, opts)

        reads =
          measured_phase(context, base, results, completed, "attachment_read", opts, fn ->
            attachment_read(uuid, physical.attachment_index, opts)
          end)

        results = Map.put(results, "attachment_read", reads)
        completed = completed ++ ["attachment_read"]
        write_completed!(context, base, results, completed, opts)

        mixed =
          measured_phase(context, base, results, completed, "mixed", opts, fn ->
            mixed_workload(uuid, queries, bulk.document_ids, physical.attachment_index, opts)
          end)

        results = Map.put(results, "mixed", mixed)
        completed = completed ++ ["mixed"]

        write_state!(context, base, results, completed, "complete", nil, nil, opts)
      after
        Runtime.close_work_database(context, uuid, relative)
      end
    end
  end

  defp measured_phase(context, base, results, completed, phase, opts, fun) do
    write_state!(context, base, results, completed, "running", phase, nil, opts)
    started_at = timestamp()
    started = System.monotonic_time(:microsecond)
    result = fun.()

    %{
      "started_at" => started_at,
      "ended_at" => timestamp(),
      "elapsed_us" => System.monotonic_time(:microsecond) - started,
      "result" => result,
      "peak_rss_bytes" => peak_rss_bytes()
    }
  end

  defp single_document_ingest(context, run_id, dataset, manifest, progress, opts) do
    articles = Enum.take(manifest["articles"] || [], @single_document_count)
    total = length(articles)

    case Runtime.create_work_database(context, "simplewiki-single", run_id) do
      {:ok, uuid, relative} ->
        try do
          started_at = timestamp()

          {samples, text_bytes, elapsed} =
            articles
            |> Enum.chunk_every(100)
            |> Enum.reduce({[], 0, 0}, fn batch, {samples, text_bytes, elapsed} ->
              {batch_samples, batch_bytes} =
                Enum.map_reduce(batch, 0, fn article, bytes ->
                  text = File.read!(article_text_path(dataset, article))

                  request = %{
                    "id" => "single-" <> article_id(article),
                    "body" => SimpleWiki.document_body(article, text)
                  }

                  sample = timed_result!(fn -> Documents.put(uuid, request) end)
                  {sample, bytes + byte_size(text)}
                end)

              next_samples = Enum.reverse(batch_samples, samples)
              next_elapsed = elapsed + Enum.sum(batch_samples)
              processed = length(next_samples)
              progress.(processed, total, next_elapsed)
              {next_samples, text_bytes + batch_bytes, next_elapsed}
            end)

          samples = Enum.reverse(samples)

          Statistics.summarize(samples, 1)
          |> Map.merge(%{
            "started_at" => started_at,
            "ended_at" => timestamp(),
            "documents" => total,
            "text_bytes" => text_bytes,
            "elapsed_us" => elapsed,
            "docs_per_sec" => Statistics.per_sec(total, elapsed),
            "text_mib_per_sec" => Statistics.mib_per_sec(text_bytes, elapsed),
            "peak_rss_bytes" => peak_rss_bytes(),
            "diagnostic_ceiling_seconds" => opts[:diagnostic_ceiling_seconds]
          })
        after
          Runtime.close_work_database(context, uuid, relative)
        end

      {:error, message} ->
        Mix.raise(message)
    end
  end

  defp bulk_document_ingest(uuid, dataset, manifest, progress, _opts) do
    articles = manifest["articles"] || []
    checkpoints = Enum.filter(@scaling_checkpoints, &(&1 <= length(articles)))
    started_at = timestamp()

    initial = %{
      processed: 0,
      text_bytes: 0,
      elapsed: 0,
      transactions: 0,
      batch_samples: [],
      attachments: [],
      revisions: %{},
      document_ids: [],
      scaling: []
    }

    acc = bulk_seed_loop(uuid, dataset, articles, checkpoints, progress, initial)

    stats = %{
      "started_at" => started_at,
      "ended_at" => timestamp(),
      "documents" => acc.processed,
      "batch_size" => @bulk_batch_size,
      "transaction_count" => acc.transactions,
      "total_text_bytes" => acc.text_bytes,
      "elapsed_us" => acc.elapsed,
      "docs_per_sec" => Statistics.per_sec(acc.processed, acc.elapsed),
      "mib_per_sec" => Statistics.mib_per_sec(acc.text_bytes, acc.elapsed),
      "batch_latency" => Statistics.summarize(Enum.reverse(acc.batch_samples), 1),
      "scaling_ladder" => scaling_rows(Enum.reverse(acc.scaling)),
      "peak_rss_bytes" => peak_rss_bytes()
    }

    %{
      stats: stats,
      attachments: Enum.reverse(acc.attachments),
      revisions: acc.revisions,
      document_ids: Enum.reverse(acc.document_ids)
    }
  end

  defp bulk_seed_loop(_uuid, _dataset, [], _checkpoints, _progress, acc), do: acc

  defp bulk_seed_loop(uuid, dataset, articles, checkpoints, progress, acc) do
    batch_size = next_bulk_batch_size(acc.processed, length(articles), checkpoints)
    {batch, rest} = Enum.split(articles, batch_size)

    prepared = Enum.map(batch, &prepare_seed_article(dataset, &1))
    operations = Enum.map(prepared, & &1.operation)
    {elapsed, {:ok, results}} = :timer.tc(fn -> Documents.bulk_write(uuid, operations) end)

    {attachments, revisions} = collect_seed_attachment_state(prepared, results, acc)
    processed = acc.processed + length(batch)
    total_elapsed = acc.elapsed + elapsed

    scaling =
      if processed in checkpoints or rest == [] do
        [%{documents: processed, elapsed_us: total_elapsed} | acc.scaling]
      else
        acc.scaling
      end

    next = %{
      acc
      | processed: processed,
        text_bytes: acc.text_bytes + Enum.sum(Enum.map(prepared, & &1.text_bytes)),
        elapsed: total_elapsed,
        transactions: acc.transactions + 1,
        batch_samples: [elapsed | acc.batch_samples],
        attachments: attachments,
        revisions: revisions,
        document_ids: Enum.reduce(prepared, acc.document_ids, &[&1.id | &2]),
        scaling: scaling
    }

    progress.(processed, processed + length(rest), total_elapsed)
    bulk_seed_loop(uuid, dataset, rest, checkpoints, progress, next)
  end

  defp prepare_seed_article(dataset, article) do
    text_path = article_text_path(dataset, article)
    text = File.read!(text_path)
    id = article_id(article)

    attachments =
      Enum.map(article["attachments"] || [], fn attachment ->
        name = attachment["name"]

        %{
          id: id,
          name: name,
          path: Path.join(Path.dirname(text_path), name),
          text_path: text_path,
          article: article,
          content_type: SimpleWiki.content_type(name),
          category: attachment_size_category(attachment["expected_size"]),
          logical_bytes:
            attachment["expected_size"] ||
              Statistics.file_size(Path.join(Path.dirname(text_path), name))
        }
      end)

    %{
      id: id,
      text_bytes: byte_size(text),
      attachments: attachments,
      operation: %{"type" => "put", "id" => id, "body" => SimpleWiki.document_body(article, text)}
    }
  end

  defp collect_seed_attachment_state(prepared, results, acc) do
    Enum.zip(prepared, results)
    |> Enum.reduce({acc.attachments, acc.revisions}, fn {item, result}, {attachments, revisions} ->
      if item.attachments == [] do
        {attachments, revisions}
      else
        revision = VialKeeper.MapAccess.get(result, :revision)
        {Enum.reverse(item.attachments, attachments), Map.put(revisions, item.id, revision)}
      end
    end)
  end

  defp next_bulk_batch_size(processed, remaining, checkpoints) do
    boundary = Enum.find(checkpoints, &(processed < &1))
    to_boundary = if boundary, do: boundary - processed, else: @bulk_batch_size
    min(remaining, min(@bulk_batch_size, to_boundary))
  end

  defp scaling_rows(rows) do
    rows
    |> Enum.with_index()
    |> Enum.map(fn {row, index} ->
      per_document = row.elapsed_us / max(row.documents, 1)
      previous = if index > 0, do: Enum.at(rows, index - 1), else: nil
      previous_cost = if previous, do: previous.elapsed_us / max(previous.documents, 1), else: nil
      next = Enum.at(rows, index + 1)

      %{
        "documents" => row.documents,
        "elapsed_us" => row.elapsed_us,
        "time_per_document_us" => Float.round(per_document, 2),
        "throughput_docs_per_sec" => Statistics.per_sec(row.documents, row.elapsed_us),
        "cost_growth_ratio" =>
          if(previous_cost, do: Float.round(per_document / max(previous_cost, 0.001), 3), else: nil),
        "projected_next_elapsed_us" => if(next, do: round(per_document * next.documents), else: nil)
      }
    end)
  end

  defp attachment_physical_ingest(_uuid, [], _progress, _opts) do
    %{
      stats: %{"files" => 0, "skipped" => true},
      references: %{},
      attachment_index: []
    }
  end

  defp attachment_physical_ingest(uuid, sources, progress, opts) do
    total = length(sources)
    before_bytes = Statistics.cas_bytes(uuid)
    started_at = timestamp()
    concurrency = Keyword.get(opts, :max_concurrency, Attachments.default_batch_concurrency())
    configure_attachment_concurrency!(uuid, concurrency)

    initial = %{
      processed: 0,
      elapsed: 0,
      logical_bytes: 0,
      transactions: 0,
      deduplicated: 0,
      categories: %{},
      references: %{},
      attachment_index: []
    }

    acc =
      ["64_kib", "1_mib", "16_mib", "other"]
      |> Enum.flat_map(fn category -> Enum.filter(sources, &(&1.category == category)) end)
      |> Enum.chunk_by(& &1.category)
      |> Enum.reduce(initial, fn category_sources, acc ->
        Enum.reduce(Enum.chunk_every(category_sources, @attachment_batch_size), acc, fn batch,
                                                                                        acc ->
          upload_attachment_batch(uuid, batch, concurrency, total, progress, acc)
        end)
      end)

    physical_bytes = max(Statistics.cas_bytes(uuid) - before_bytes, 0)

    category_stats =
      Map.new(acc.categories, fn {category, state} ->
        {category,
         Statistics.summarize(Enum.reverse(state.latencies), 1)
         |> Map.merge(%{
           "files" => state.files,
           "logical_bytes" => state.logical_bytes,
           "elapsed_us" => state.elapsed,
           "files_per_sec" => Statistics.per_sec(state.files, state.elapsed),
           "mib_per_sec" => Statistics.mib_per_sec(state.logical_bytes, state.elapsed)
         })}
      end)

    stats = %{
      "started_at" => started_at,
      "ended_at" => timestamp(),
      "files" => total,
      "max_concurrency" => concurrency,
      "metadata_transaction_count" => acc.transactions,
      "logical_bytes" => acc.logical_bytes,
      "physical_bytes" => physical_bytes,
      "deduplicated_count" => acc.deduplicated,
      "elapsed_us" => acc.elapsed,
      "files_per_sec" => Statistics.per_sec(total, acc.elapsed),
      "mib_per_sec" => Statistics.mib_per_sec(acc.logical_bytes, acc.elapsed),
      "by_size_category" => category_stats,
      "peak_rss_bytes" => peak_rss_bytes()
    }

    %{
      stats: stats,
      references: acc.references,
      attachment_index: Enum.reverse(acc.attachment_index)
    }
  end

  defp upload_attachment_batch(uuid, batch, concurrency, total, progress, acc) do
    upload_sources =
      Enum.map(batch, fn source ->
        %{key: {source.id, source.name}, source: IO.file_chunks(source.path)}
      end)

    {elapsed, {:ok, uploaded}} =
      :timer.tc(fn ->
        Attachments.upload_batch(uuid, upload_sources, max_concurrency: concurrency)
      end)

    per_file_latency = div(elapsed, max(length(batch), 1))

    {references, attachment_index, deduplicated} =
      Enum.zip(batch, uploaded)
      |> Enum.reduce(
        {acc.references, acc.attachment_index, acc.deduplicated},
        fn {source, result}, {references, index, deduplicated} ->
          reference = %{"blob" => result.blob, "content_type" => source.content_type}

          references =
            Map.update(
              references,
              source.id,
              %{source: source, refs: %{source.name => reference}},
              fn entry ->
                %{entry | refs: Map.put(entry.refs, source.name, reference)}
              end
            )

          item = %{
            id: source.id,
            name: source.name,
            category: source.category,
            logical_bytes: source.logical_bytes
          }

          {references, [item | index], deduplicated + if(result.deduplicated?, do: 1, else: 0)}
        end
      )

    category = hd(batch).category
    logical_bytes = Enum.sum(Enum.map(batch, & &1.logical_bytes))

    categories =
      Map.update(
        acc.categories,
        category,
        %{
          files: length(batch),
          logical_bytes: logical_bytes,
          elapsed: elapsed,
          latencies: List.duplicate(per_file_latency, length(batch))
        },
        fn state ->
          %{
            files: state.files + length(batch),
            logical_bytes: state.logical_bytes + logical_bytes,
            elapsed: state.elapsed + elapsed,
            latencies: prepend_copies(per_file_latency, length(batch), state.latencies)
          }
        end
      )

    processed = acc.processed + length(batch)
    total_elapsed = acc.elapsed + elapsed
    progress.(processed, total, total_elapsed)

    %{
      acc
      | processed: processed,
        elapsed: total_elapsed,
        logical_bytes: acc.logical_bytes + logical_bytes,
        transactions:
          acc.transactions +
            ceil_div(length(Enum.uniq_by(uploaded, & &1.blob)), @attachment_batch_size),
        deduplicated: deduplicated,
        categories: categories,
        references: references,
        attachment_index: attachment_index
    }
  end

  defp attachment_reference_mutation(_uuid, references, _revisions, _progress, _opts)
       when map_size(references) == 0,
       do: %{"documents" => 0, "skipped" => true}

  defp attachment_reference_mutation(uuid, references, revisions, progress, _opts) do
    entries = Map.to_list(references)
    total = length(entries)
    started_at = timestamp()

    {processed, elapsed, transactions, samples} =
      entries
      |> Enum.chunk_every(@bulk_batch_size)
      |> Enum.reduce({0, 0, 0, []}, fn batch, {processed, elapsed, transactions, samples} ->
        operations =
          Enum.map(batch, fn {id, %{source: source, refs: refs}} ->
            text = File.read!(source.text_path)

            %{
              "type" => "put",
              "id" => id,
              "if_revision" => Map.fetch!(revisions, id),
              "body" => SimpleWiki.document_body(source.article, text),
              "attachments" => refs
            }
          end)

        sample = timed_result!(fn -> Documents.bulk_write(uuid, operations) end)
        next_processed = processed + length(batch)
        next_elapsed = elapsed + sample
        progress.(next_processed, total, next_elapsed)
        {next_processed, next_elapsed, transactions + 1, [sample | samples]}
      end)

    %{
      "started_at" => started_at,
      "ended_at" => timestamp(),
      "documents" => processed,
      "attachment_references" =>
        Enum.sum(Enum.map(references, fn {_id, entry} -> map_size(entry.refs) end)),
      "batch_size" => @bulk_batch_size,
      "transaction_count" => transactions,
      "elapsed_us" => elapsed,
      "docs_per_sec" => Statistics.per_sec(processed, elapsed),
      "batch_latency" => Statistics.summarize(Enum.reverse(samples), 1),
      "peak_rss_bytes" => peak_rss_bytes()
    }
  end

  defp fts_build(uuid, total_documents, progress) do
    before = Statistics.snapshot_bytes(uuid, :bundle, :before_fts)
    started_at = timestamp()
    reporter = start_search_progress_reporter(total_documents, progress)
    {reporter_pid, _monitor} = reporter
    handler_id = {__MODULE__, :fts_progress, self(), make_ref()}

    :ok =
      :telemetry.attach(
        handler_id,
        @search_batch_event,
        &__MODULE__.handle_search_batch/4,
        reporter_pid
      )

    started = System.monotonic_time(:microsecond)

    query_result =
      try do
        Query.create_index(uuid, %{
          "name" => @index_name,
          "type" => "full_text",
          "fields" => ["/title", "/text"]
        })
      after
        :telemetry.detach(handler_id)
      end

    reporter_result = stop_search_progress_reporter(reporter)

    result =
      case {query_result, reporter_result} do
        {query_result, :ok} ->
          query_result

        {{:error, _} = query_error, {:error, _reporter_error}} ->
          query_error

        {_query_result, {:error, reason}} ->
          Mix.raise("search progress reporter failed: #{Exception.format_exit(reason)}")
      end

    {:ok, created} = result
    elapsed = System.monotonic_time(:microsecond) - started

    %{
      "started_at" => started_at,
      "ended_at" => timestamp(),
      "elapsed_us" => elapsed,
      "documents" => total_documents,
      "docs_per_sec" => Statistics.per_sec(total_documents, elapsed),
      "index_id" => VialKeeper.MapAccess.get(created, :index_id),
      "index_growth_bytes" => Statistics.snapshot_bytes(uuid, :bundle, :after_fts) - before,
      "peak_rss_bytes" => peak_rss_bytes()
    }
  end

  @doc "Forwards one bounded search rebuild batch to the benchmark progress reporter."
  @spec handle_search_batch([atom()], map(), map(), pid()) :: :ok
  def handle_search_batch(_event, %{entries: entries, duration: duration}, _metadata, reporter) do
    send(reporter, {:search_batch, entries, duration})
    :ok
  end

  defp start_search_progress_reporter(total, progress) do
    started = System.monotonic_time(:microsecond)
    spawn_monitor(fn -> search_progress_loop(total, progress, 0, started) end)
  end

  defp search_progress_loop(total, progress, processed, started) do
    receive do
      {:search_batch, entries, _duration} ->
        next_processed = processed + entries
        elapsed_us = System.monotonic_time(:microsecond) - started
        progress.(next_processed, total, elapsed_us)
        search_progress_loop(total, progress, next_processed, started)

      {:stop, caller} ->
        send(caller, {:search_progress_stopped, self()})
        :ok
    end
  end

  defp stop_search_progress_reporter({reporter, monitor}) do
    send(reporter, {:stop, self()})

    receive do
      {:search_progress_stopped, ^reporter} ->
        Process.demonitor(monitor, [:flush])
        :ok

      {:DOWN, ^monitor, :process, ^reporter, :normal} ->
        :ok

      {:DOWN, ^monitor, :process, ^reporter, reason} ->
        {:error, reason}
    after
      30_000 ->
        Process.demonitor(monitor, [:flush])
        {:error, :stop_timeout}
    end
  end

  defp fts_search(uuid, queries, opts) do
    warmup = Keyword.get(opts, :warmup, 1)
    iterations = Keyword.get(opts, :iterations, 3)

    Enum.map(@search_concurrencies, fn concurrency ->
      search_concurrency_row(uuid, queries, concurrency, warmup, iterations)
    end)
  end

  defp search_concurrency_row(uuid, queries, concurrency, warmup, iterations) do
    Statistics.times(warmup, fn -> search_many(uuid, queries, concurrency) end)
    samples = Statistics.sample_us(iterations, fn -> search_many(uuid, queries, concurrency) end)
    Map.merge(Statistics.summarize(samples, length(queries)), %{"concurrency" => concurrency})
  end

  defp search_many(uuid, queries, concurrency) do
    queries
    |> Task.async_stream(
      fn text ->
        Query.execute(uuid, %{
          "search" => %{"index" => @index_name, "text" => text, "mode" => "all"},
          "limit" => 50
        })
      end,
      max_concurrency: concurrency,
      timeout: :infinity,
      ordered: false
    )
    |> Stream.run()
  end

  defp attachment_read(_uuid, [], _opts), do: %{"skipped" => true}

  defp attachment_read(uuid, attachments, opts) do
    warmup = Keyword.get(opts, :warmup, 1)
    iterations = Keyword.get(opts, :iterations, 3)

    Enum.map(@search_concurrencies, fn concurrency ->
      attach_concurrency_row(uuid, attachments, concurrency, warmup, iterations)
    end)
  end

  defp attach_concurrency_row(uuid, attachments, concurrency, warmup, iterations) do
    Statistics.times(warmup, fn -> read_many(uuid, attachments, concurrency) end)

    samples =
      Enum.map(1..iterations, fn _ ->
        {elapsed, bytes} = :timer.tc(fn -> read_many(uuid, attachments, concurrency) end)
        {elapsed, bytes}
      end)

    elapsed = Enum.map(samples, &elem(&1, 0))
    bytes = Enum.reduce(samples, 0, &(elem(&1, 1) + &2))

    Map.merge(Statistics.summarize(elapsed, length(attachments)), %{
      "concurrency" => concurrency,
      "successful_bytes" => bytes
    })
  end

  defp read_many(uuid, attachments, concurrency) do
    attachments
    |> Task.async_stream(
      fn item ->
        case Attachments.open_stream(uuid, %{
               "id" => item.id,
               "revision" => nil,
               "name" => item.name
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

  defp mixed_workload(uuid, queries, ids, attachments, opts) do
    iterations = Keyword.get(opts, :iterations, 3)

    Enum.map(@mixed_concurrencies, fn concurrency ->
      mixed_concurrency_row(uuid, queries, ids, attachments, concurrency, iterations)
    end)
  end

  defp mixed_concurrency_row(uuid, queries, ids, attachments, concurrency, iterations) do
    samples =
      Enum.map(1..iterations, fn n ->
        Statistics.timed_us(fn -> mixed_round(uuid, queries, ids, attachments, concurrency, n) end)
      end)

    Map.merge(Statistics.summarize(samples, 100), %{"concurrency" => concurrency})
  end

  defp mixed_round(uuid, queries, ids, attachments, concurrency, seed) do
    mixed_ops(queries, ids, attachments, seed)
    |> Task.async_stream(
      fn op -> run_mixed_op(uuid, op) end,
      max_concurrency: concurrency,
      timeout: :infinity,
      ordered: false
    )
    |> Stream.run()
  end

  defp mixed_ops(queries, ids, attachments, seed) do
    :rand.seed(:exsss, {seed, 7, 99})

    Enum.map(1..100, fn _ ->
      roll = :rand.uniform(100)

      cond do
        roll <= 60 -> {:search, Enum.random(queries ++ ["the"])}
        roll <= 80 -> {:get, Enum.random(ids ++ ["missing"])}
        roll <= 95 and attachments != [] -> {:attach, Enum.random(attachments)}
        true -> {:update, Enum.random(ids)}
      end
    end)
  end

  defp run_mixed_op(uuid, {:search, text}) do
    Query.execute(uuid, %{
      "search" => %{"index" => @index_name, "text" => text, "mode" => "all"},
      "limit" => 25
    })
  end

  defp run_mixed_op(uuid, {:get, id}), do: Documents.get(uuid, %{"id" => id})

  defp run_mixed_op(uuid, {:attach, item}) do
    case Attachments.open_stream(uuid, %{"id" => item.id, "revision" => nil, "name" => item.name}) do
      {:ok, stream} -> IO.consume_stream(stream.body)
      {:error, _} -> 0
    end
  end

  defp run_mixed_op(uuid, {:update, id}) do
    case Documents.get(uuid, %{"id" => id}) do
      {:ok, doc} ->
        body = Map.put(doc.body || %{}, "updated", true)
        Documents.put(uuid, %{"id" => id, "if_revision" => doc.revision, "body" => body})

      {:error, _} ->
        :ok
    end
  end

  defp progress_callback(context, base, results, completed, phase, opts) do
    fn processed, total, elapsed_us ->
      throughput = Statistics.per_sec(processed, elapsed_us)
      projected_us = if processed > 0, do: round(elapsed_us / processed * total), else: nil

      progress = %{
        "processed" => processed,
        "total" => total,
        "elapsed_us" => elapsed_us,
        "current_throughput_per_sec" => throughput,
        "moving_throughput_per_sec" => throughput,
        "projected_total_us" => projected_us,
        "peak_rss_bytes" => peak_rss_bytes(),
        "updated_at" => timestamp()
      }

      write_state!(context, base, results, completed, "running", phase, progress, opts)
      emit_progress(phase, progress)
      notify_progress_observer(phase, progress, opts)
      enforce_diagnostic_ceiling!(phase, processed, projected_us, opts)
      :ok
    end
  end

  defp emit_progress(phase, progress) do
    Mix.shell().info(
      "#{phase}: #{progress["processed"]}/#{progress["total"]} " <>
        "elapsed=#{format_seconds(progress["elapsed_us"])}s " <>
        "rate=#{progress["current_throughput_per_sec"]}/s " <>
        "projected=#{format_seconds(progress["projected_total_us"])}s"
    )
  end

  defp enforce_diagnostic_ceiling!(_phase, _processed, nil, _opts), do: :ok

  defp enforce_diagnostic_ceiling!(phase, processed, projected_us, opts) do
    ceiling = opts[:diagnostic_ceiling_seconds]

    if is_integer(ceiling) and ceiling > 0 and processed > 0 and
         projected_us > ceiling * 1_000_000 do
      Mix.raise(
        "#{phase} watchdog aborted: projected #{format_seconds(projected_us)}s exceeds #{ceiling}s"
      )
    end

    :ok
  end

  defp notify_progress_observer(phase, progress, opts) do
    case opts[:progress_observer] do
      observer when is_function(observer, 2) -> observer.(phase, progress)
      _ -> :ok
    end
  end

  defp write_completed!(context, base, results, completed, opts) do
    write_state!(context, base, results, completed, "running", nil, nil, opts)
  end

  defp write_state!(context, base, results, completed, status, phase, progress, opts) do
    report =
      base
      |> Map.put("status", status)
      |> Map.put("completed_phases", completed)
      |> Map.put("current_phase", phase)
      |> Map.put("progress", progress)
      |> Map.put("results", results)
      |> Map.put("updated_at", timestamp())
      |> maybe_put_finished_at(status)

    case Reports.write(context, "simplewiki-stress.json", report, opts) do
      {:ok, path} -> {:ok, path}
      {:error, message} -> Mix.raise(message)
    end
  end

  defp maybe_put_finished_at(report, "complete"), do: Map.put(report, "finished_at", timestamp())
  defp maybe_put_finished_at(report, _status), do: report

  defp article_text_path(dataset, article) do
    prefix = article_id(article) <> "." <> to_string(article["version"])
    name = get_in(article, ["text", "name"]) || prefix <> ".txt"
    Path.join([dataset, "objects", prefix, name])
  end

  defp article_id(article), do: article["id"]

  defp attachment_size_category(65_536), do: "64_kib"
  defp attachment_size_category(1_048_576), do: "1_mib"
  defp attachment_size_category(16_777_216), do: "16_mib"
  defp attachment_size_category(_bytes), do: "other"

  defp timed_result!(fun) do
    {elapsed, result} = :timer.tc(fun)

    case result do
      :ok -> elapsed
      {:ok, _value} -> elapsed
      {:error, error} -> Mix.raise("benchmark operation failed: #{inspect(error)}")
      other -> Mix.raise("benchmark operation returned unexpectedly: #{inspect(other)}")
    end
  end

  defp configure_attachment_concurrency!(uuid, concurrency) do
    case VialKeeper.Runtime.DatabaseCatalog.command(
           uuid,
           {:command, :update_config,
            %{"attachments" => %{"max_concurrent_attachment_writes" => concurrency}}}
         ) do
      {:ok, _} -> :ok
      {:error, error} -> Mix.raise("cannot configure attachment concurrency: #{inspect(error)}")
    end
  end

  defp ceil_div(count, divisor), do: div(count + divisor - 1, divisor)

  defp prepend_copies(_value, count, tail) when count <= 0, do: tail
  defp prepend_copies(value, count, tail), do: prepend_copies(value, count - 1, [value | tail])

  defp peak_rss_bytes do
    case File.read("/proc/self/status") do
      {:ok, contents} ->
        case Regex.run(~r/^VmHWM:\s+(\d+)\s+kB$/m, contents, capture: :all_but_first) do
          [kilobytes] -> String.to_integer(kilobytes) * 1024
          _ -> :erlang.memory(:total)
        end

      {:error, _reason} ->
        :erlang.memory(:total)
    end
  end

  defp timestamp, do: DateTime.utc_now() |> DateTime.truncate(:second) |> DateTime.to_iso8601()

  defp format_seconds(nil), do: "unknown"
  defp format_seconds(microseconds), do: Float.round(microseconds / 1_000_000, 1)
end
