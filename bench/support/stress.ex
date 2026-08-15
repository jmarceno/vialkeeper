defmodule VialKeeper.Bench.Stress do
  @moduledoc "PMC catalog-path stress benchmark."

  alias VialKeeper.Attachments
  alias VialKeeper.Bench.{IO, Marker, Pmc, Registry, Reports, Root, Runtime, Statistics}
  alias VialKeeper.Documents
  alias VialKeeper.Query

  @index_name "pmc-text"
  @search_concurrencies [1, 4, 16]
  @mixed_concurrencies [4, 16, 32]
  @query_algorithm "pmc-query-v1"

  @spec run(keyword()) :: {:ok, map()} | {:error, binary()}
  def run(opts \\ []) do
    with {:ok, context} <- Root.load(opts),
         {:ok, spec} <- Registry.fetch("pmc"),
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
        else: {:error, "dataset pmc is not ready; run mix bench.data prepare pmc"}
    end
  end

  defp read_manifest(dataset) do
    path = Path.join(dataset, "manifest.json")

    case File.read(path) do
      {:ok, body} ->
        case JSON.decode(body) do
          {:ok, manifest} when is_map(manifest) -> {:ok, manifest}
          _ -> {:error, "PMC manifest is invalid"}
        end

      {:error, reason} ->
        {:error, "cannot read PMC manifest: #{inspect(reason)}"}
    end
  end

  defp measure(context, spec, dataset, manifest, opts) do
    run_id = Integer.to_string(System.unique_integer([:positive]))

    with {:ok, work} <- Root.work_run_path(context, "pmc", run_id),
         {:ok, uuid, relative} <- Runtime.create_work_database(context, "pmc", run_id) do
      try do
        ingest = bulk_ingest(uuid, dataset, manifest)
        fts = fts_build(uuid)
        queries = build_queries(dataset, manifest)
        search = fts_search(uuid, queries, opts)
        reads = attachment_read(uuid, ingest["attachment_index"], opts)
        mixed = mixed_workload(uuid, queries, ingest, opts)
        {:ok, hash} = Marker.manifest_hash(dataset)

        report =
          Reports.envelope(
            context,
            %{
              "benchmark" => "stress",
              "dataset" => spec["name"],
              "dataset_version" => spec["version"],
              "fixture_manifest_hash" => hash,
              "dataset_path" => dataset,
              "work_path" => work,
              "query_algorithm" => @query_algorithm
            },
            %{
              "bulk_ingest" => ingest["stats"],
              "fts_build" => fts,
              "fts_search" => search,
              "attachment_read" => reads,
              "mixed" => mixed
            }
          )

        Reports.write(context, "pmc-stress.json", report, opts)
      after
        Runtime.close_work_database(context, uuid, relative)
      end
    end
  end

  defp bulk_ingest(uuid, dataset, manifest) do
    started = System.monotonic_time(:microsecond)
    articles = manifest["articles"] || []

    acc =
      Enum.reduce(articles, %{docs: 0, text_bytes: 0, attach_bytes: 0, attachments: []}, fn article,
                                                                                            acc ->
        ingest_article(uuid, dataset, article, acc)
      end)

    elapsed = System.monotonic_time(:microsecond) - started
    physical = Statistics.cas_bytes(uuid)
    db_size = Statistics.bundle_bytes(uuid)

    stats = %{
      "documents" => acc.docs,
      "elapsed_us" => elapsed,
      "docs_per_sec" => Statistics.per_sec(acc.docs, elapsed),
      "text_mib_per_sec" => Statistics.mib_per_sec(acc.text_bytes, elapsed),
      "attachment_mib_per_sec" => Statistics.mib_per_sec(acc.attach_bytes, elapsed),
      "logical_attachment_bytes" => acc.attach_bytes,
      "physical_attachment_bytes" => physical,
      "database_bytes" => db_size
    }

    %{
      "stats" => stats,
      "attachment_index" => acc.attachments,
      "document_ids" => Enum.map(articles, & &1["pmcid"])
    }
  end

  defp ingest_article(uuid, dataset, article, acc) do
    prefix = article["pmcid"] <> "." <> to_string(article["version"])
    text_name = get_in(article, ["text", "name"]) || prefix <> ".txt"
    text_path = Path.join([dataset, "objects", prefix, text_name])
    text = File.read!(text_path)
    refs = upload_attachments(uuid, dataset, prefix, article["attachments"] || [])

    body = Pmc.document_body(article, text)

    {:ok, _} =
      Documents.put(uuid, %{
        "id" => article["pmcid"],
        "body" => body,
        "attachments" => refs
      })

    attach_bytes =
      Enum.reduce(article["attachments"] || [], 0, fn obj, sum ->
        sum + Statistics.file_size(Path.join([dataset, "objects", prefix, obj["name"]]))
      end)

    attachments =
      acc.attachments ++
        Enum.map(Map.keys(refs), fn name ->
          %{id: article["pmcid"], name: name, category: Pmc.classify_name(name)}
        end)

    %{
      acc
      | docs: acc.docs + 1,
        text_bytes: acc.text_bytes + byte_size(text),
        attach_bytes: acc.attach_bytes + attach_bytes,
        attachments: attachments
    }
  end

  defp upload_attachments(uuid, dataset, prefix, attachments) do
    Enum.reduce(attachments, %{}, fn obj, acc ->
      path = Path.join([dataset, "objects", prefix, obj["name"]])
      content_type = content_type(obj["name"])

      {:ok, %{blob: digest}} = Attachments.upload_stream(uuid, IO.file_chunks(path))

      Map.put(acc, obj["name"], %{"blob" => digest, "content_type" => content_type})
    end)
  end

  defp fts_build(uuid) do
    before = Statistics.snapshot_bytes(uuid, :bundle, :before_fts)
    started = System.monotonic_time(:microsecond)

    {:ok, created} =
      Query.create_index(uuid, %{
        "name" => @index_name,
        "type" => "full_text",
        "fields" => ["/title", "/text"],
        "tokenization" => %{"strategy" => "unicode_words_v1", "diacritics" => "preserve"}
      })

    elapsed = System.monotonic_time(:microsecond) - started

    %{
      "elapsed_us" => elapsed,
      "index_id" => VialKeeper.MapAccess.get(created, :index_id),
      "index_growth_bytes" => Statistics.snapshot_bytes(uuid, :bundle, :after_fts) - before,
      "memory_bytes" => :erlang.memory(:total)
    }
  end

  defp build_queries(dataset, manifest) do
    tokens =
      (manifest["articles"] || [])
      |> Enum.reduce(%{}, &count_article_tokens(dataset, &1, &2))
      |> Enum.reject(fn {_tok, n} -> n < 1 end)
      |> Enum.sort_by(fn {_tok, n} -> n end, :desc)

    common = tokens |> Enum.take(5) |> Enum.map(&elem(&1, 0))
    rare = tokens |> Enum.take(-5) |> Enum.map(&elem(&1, 0))
    medium = tokens |> Enum.drop(div(length(tokens), 3)) |> Enum.take(5) |> Enum.map(&elem(&1, 0))

    multi =
      common
      |> Enum.take(2)
      |> Enum.join(" ")
      |> List.wrap()

    (common ++ medium ++ rare ++ multi ++ ["zzzzzxxyy-no-such-term"])
    |> Enum.reject(&(&1 == ""))
    |> Enum.uniq()
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

  defp mixed_workload(uuid, queries, ingest, opts) do
    ids = ingest["document_ids"]
    attachments = ingest["attachment_index"]
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
    ops = mixed_ops(queries, ids, attachments, seed)

    ops
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
        roll <= 60 -> {:search, Enum.random(queries ++ [""]) |> nonempty_query()}
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

  defp run_mixed_op(uuid, {:get, id}) do
    Documents.get(uuid, %{"id" => id})
  end

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

  defp nonempty_query(""), do: "the"
  defp nonempty_query(text), do: text

  defp count_article_tokens(dataset, article, acc) do
    Enum.reduce(tokenize(article_text(dataset, article)), acc, fn tok, acc ->
      Map.update(acc, tok, 1, &(&1 + 1))
    end)
  end

  defp article_text(dataset, article) do
    prefix = article["pmcid"] <> "." <> to_string(article["version"])
    name = get_in(article, ["text", "name"]) || prefix <> ".txt"
    path = Path.join([dataset, "objects", prefix, name])

    case File.read(path) do
      {:ok, text} -> text
      {:error, _} -> ""
    end
  end

  defp tokenize(text) do
    text
    |> String.downcase()
    |> String.split(~r/[^a-z0-9]+/u, trim: true)
    |> Enum.filter(&(String.length(&1) > 3))
  end

  defp content_type(name) do
    case Pmc.classify_name(name) do
      "pdf" -> "application/pdf"
      "image" -> "image/jpeg"
      _ -> "application/octet-stream"
    end
  end
end
