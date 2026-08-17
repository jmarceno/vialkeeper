defmodule VialKeeper.Bench.FTS do
  @moduledoc """
  TREC-COVID full-text benchmark over the SQLite adapter seam.
  """

  alias VialKeeper.Bench.{
    Beir,
    Marker,
    Metrics,
    Prepare,
    Progress,
    Registry,
    Reports,
    Root,
    Runtime,
    Statistics
  }

  alias VialKeeper.Storage.SQLite.Adapter

  @index_name "trec-covid-text"
  @fts_fields ["/title", "/text"]
  @quality_mode "all"
  @modes ["all", "prefix"]
  @concurrencies [1, 4]
  @retrieve_at 100

  @spec run(keyword()) :: {:ok, map()} | {:error, binary()}
  def run(opts \\ []) do
    with {:ok, context} <- Root.load_or_configure(opts),
         {:ok, spec} <- Registry.fetch("trec-covid"),
         {:ok, _prepared} <- Prepare.prepare("trec-covid", opts),
         {:ok, dataset} <- require_ready(context, spec),
         {:ok, report} <-
           Runtime.with_isolated(context, fn -> measure(context, spec, dataset, opts) end) do
      Reports.write(context, "fts-trec-covid.json", report, opts)
    end
  end

  defp require_ready(context, spec) do
    with {:ok, path} <- Root.dataset_path(context, spec["name"], spec["version"]) do
      if Marker.present?(path) do
        {:ok, path}
      else
        {:error, "dataset trec-covid is not ready; run mix bench.data prepare trec-covid"}
      end
    end
  end

  defp measure(context, spec, dataset, opts) do
    run_id = Integer.to_string(System.unique_integer([:positive]))

    with {:ok, work} <- Root.work_run_path(context, "fts", run_id),
         :ok <- File.mkdir_p(Path.join(work, "tmp")),
         db_path = Path.join(work, "database.sqlite3"),
         {:ok, adapter} <- Adapter.create(db_path, %{storage_mode: :disk}) do
      Progress.with_run(progress_opts(opts, context, adapter, work), fn progress ->
        opts = Keyword.put(opts, :progress, progress)

        try do
          results = measure_adapter(adapter, dataset, opts)
          Progress.complete(progress)
          {:ok, hash} = Marker.manifest_hash(dataset)

          report =
            Reports.envelope(
              context,
              %{
                "benchmark" => "fts",
                "dataset" => spec["name"],
                "dataset_version" => spec["version"],
                "fixture_manifest_hash" => hash,
                "dataset_path" => dataset,
                "work_path" => work,
                "warmup" => Keyword.get(opts, :warmup, 0),
                "iterations" => Keyword.get(opts, :iterations, 1)
              },
              results
            )

          {:ok, report}
        after
          _ = Adapter.close(adapter)
          _ = Root.remove_work_run!(context, work)
        end
      end)
    else
      {:error, reason} -> {:error, format(reason)}
    end
  end

  @doc """
  Runs ingest, index build, quality, and latency against an open SQLite adapter.

  Fixture download is never included. The Mix runner creates the adapter under
  the verified work path; tests may pass a tiny local BEIR fixture.
  """
  @spec measure_adapter(term(), Path.t(), keyword()) :: map()
  def measure_adapter(adapter, dataset, opts \\ []) do
    ingest = ingest_corpus(adapter, dataset, opts)
    index = build_index(adapter, opts)
    quality = evaluate_quality(adapter, dataset, opts)
    latency = measure_latency(adapter, dataset, opts)

    %{
      "ingest" => ingest,
      "fts_build" => index,
      "quality" => quality,
      "latency" => latency
    }
  end

  defp ingest_corpus(adapter, dataset, opts) do
    progress = opts[:progress]
    limit = ingest_limit(opts)
    {:ok, corpus} = Beir.find_file(dataset, "corpus.jsonl")
    started = System.monotonic_time(:microsecond)
    acc = %{docs: 0, bytes: 0, batch: []}
    Progress.phase(progress, "ingest", limit)

    result =
      corpus
      |> Beir.stream_jsonl()
      |> Enum.reduce_while(acc, fn
        {:error, _}, acc ->
          {:cont, acc}

        _row, %{docs: docs} = acc when docs >= limit ->
          {:halt, acc}

        row, acc ->
          case Beir.document_body(row) do
            {:ok, id, body} ->
              op = %{operation: :put, document_id: id, body: body}
              bytes = byte_size(body["title"] || "") + byte_size(body["text"] || "")
              acc = %{acc | docs: acc.docs + 1, bytes: acc.bytes + bytes, batch: [op | acc.batch]}
              Progress.tick(progress)
              {:cont, flush_batch(adapter, acc, 500)}

            {:error, _} ->
              {:cont, acc}
          end
      end)

    :ok = write_batch(adapter, Enum.reverse(result.batch))
    elapsed = System.monotonic_time(:microsecond) - started
    db_size = Statistics.directory_bytes(Path.dirname(adapter.path))

    %{
      "documents" => result.docs,
      "source_text_bytes" => result.bytes,
      "elapsed_us" => elapsed,
      "docs_per_sec" => Statistics.per_sec(result.docs, elapsed),
      "mib_per_sec" => Statistics.mib_per_sec(result.bytes, elapsed),
      "database_bytes_before_fts" => db_size
    }
  end

  defp flush_batch(adapter, acc, limit) do
    if length(acc.batch) >= limit do
      :ok = write_batch(adapter, Enum.reverse(acc.batch))
      %{acc | batch: []}
    else
      acc
    end
  end

  defp ingest_limit(opts) do
    Keyword.get_lazy(opts, :document_limit, fn ->
      Registry.selection_count("trec-covid", Keyword.get(opts, :profile, :standard))
    end)
  end

  defp write_batch(_adapter, []), do: :ok

  defp write_batch(adapter, operations) do
    case Adapter.apply_bulk_mutation(adapter, %{operations: operations}) do
      {:ok, _} -> :ok
      :ok -> :ok
      {:error, error} -> Mix.raise("FTS ingest failed: #{inspect(error)}")
    end
  end

  defp build_index(adapter, opts) do
    progress = opts[:progress]
    before = Statistics.directory_bytes(Path.dirname(adapter.path))
    started = System.monotonic_time(:microsecond)
    Progress.phase(progress, "fts_build", 1)

    {:ok, created} =
      Adapter.create_index(adapter, %{
        "name" => @index_name,
        "type" => "full_text",
        "fields" => @fts_fields
      })

    Progress.tick(progress)

    elapsed = System.monotonic_time(:microsecond) - started
    after_bytes = Statistics.directory_bytes(Path.dirname(adapter.path))

    %{
      "elapsed_us" => elapsed,
      "index_id" => VialKeeper.MapAccess.get(created, :index_id),
      "database_bytes_after_fts" => after_bytes,
      "index_amplification_bytes" => after_bytes - before,
      "memory_bytes" => :erlang.memory(:total)
    }
  end

  defp evaluate_quality(adapter, dataset, opts) do
    progress = opts[:progress]
    {:ok, queries_path} = Beir.find_file(dataset, "queries.jsonl")
    {:ok, qrels_path} = Beir.find_file(dataset, "test.tsv")
    {:ok, queries} = Beir.load_queries(queries_path)
    {:ok, qrels} = Beir.load_qrels(qrels_path)
    Progress.phase(progress, "quality", length(queries))

    scores =
      Enum.map(queries, fn query ->
        {:ok, qid, text} = Beir.query_text(query)
        ranked = search_ids(adapter, text, @quality_mode)
        rel = Map.get(qrels, qid, %{})
        Progress.tick(progress)

        %{
          "query_id" => qid,
          "ndcg@10" => Metrics.ndcg_at(ranked, rel, 10),
          "recall@10" => Metrics.recall_at(ranked, rel, 10),
          "recall@100" => Metrics.recall_at(ranked, rel, 100),
          "map@100" => Metrics.map_at(ranked, rel, 100)
        }
      end)

    %{
      "mode" => @quality_mode,
      "query_count" => length(scores),
      "ndcg@10" => Metrics.mean(Enum.map(scores, & &1["ndcg@10"])),
      "recall@10" => Metrics.mean(Enum.map(scores, & &1["recall@10"])),
      "recall@100" => Metrics.mean(Enum.map(scores, & &1["recall@100"])),
      "map@100" => Metrics.mean(Enum.map(scores, & &1["map@100"])),
      "per_query" => scores
    }
  end

  defp measure_latency(adapter, dataset, opts) do
    progress = opts[:progress]
    {:ok, queries_path} = Beir.find_file(dataset, "queries.jsonl")
    {:ok, queries} = Beir.load_queries(queries_path)
    texts = Enum.map(queries, fn query -> elem(Beir.query_text(query), 2) end)
    warmup = Keyword.get(opts, :warmup, 0)
    iterations = Keyword.get(opts, :iterations, 1)
    Progress.phase(progress, "latency", 1)

    first_pass =
      Enum.map(texts, fn text ->
        {elapsed, _} = :timer.tc(fn -> search_ids(adapter, text, @quality_mode) end)
        elapsed
      end)

    mode_rows = Enum.map(@modes, &latency_for_mode(adapter, texts, &1, warmup, iterations))
    Progress.tick(progress)

    %{
      "first_pass" => Statistics.summarize(first_pass, 1),
      "modes" => mode_rows
    }
  end

  defp latency_for_mode(adapter, texts, mode, warmup, iterations) do
    rows =
      Enum.map(@concurrencies, fn concurrency ->
        latency_for_concurrency(adapter, texts, mode, concurrency, warmup, iterations)
      end)

    %{"mode" => mode, "concurrency" => rows}
  end

  defp latency_for_concurrency(adapter, texts, mode, concurrency, warmup, iterations) do
    query_count = length(texts)
    Statistics.times(warmup, fn -> run_concurrent(adapter, texts, mode, concurrency) end)

    samples =
      Statistics.sample_us(iterations, fn -> run_concurrent(adapter, texts, mode, concurrency) end)

    Map.merge(Statistics.summarize(samples, query_count), %{
      "concurrency" => concurrency,
      "queries" => query_count
    })
  end

  defp run_concurrent(adapter, texts, mode, 1) do
    Enum.each(texts, &search_ids(adapter, &1, mode))
  end

  defp run_concurrent(adapter, texts, mode, concurrency) do
    texts
    |> Task.async_stream(
      fn text -> search_ids(adapter, text, mode) end,
      max_concurrency: concurrency,
      timeout: :infinity,
      ordered: false
    )
    |> Stream.run()
  end

  defp search_ids(adapter, text, mode) do
    request = %{
      search: %{index: @index_name, text: text, mode: mode},
      limit: @retrieve_at
    }

    case Adapter.execute_query(adapter, request) do
      {:ok, %{results: results}} ->
        results
        |> Enum.sort_by(fn doc -> VialKeeper.MapAccess.get(doc, :rank) || 0 end)
        |> Enum.map(fn doc -> VialKeeper.MapAccess.get(doc, :id) end)

      {:error, _} ->
        []
    end
  end

  defp progress_opts(opts, context, adapter, work) do
    [
      label: "fts",
      stall_timeout_ms: Keyword.get(opts, :stall_timeout_ms, Progress.default_stall_timeout_ms()),
      cleanup: fn ->
        _ = Adapter.close(adapter)
        _ = Root.remove_work_run!(context, work)
      end
    ]
  end

  defp format(reason) when is_binary(reason), do: reason
  defp format(reason), do: inspect(reason)
end
