defmodule VialKeeper.Bench.PerformanceDiagnostics do
  @moduledoc """
  Root-contained component controls for document, database, attachment, and search work.

  The runner deliberately keeps fixture preparation outside timed regions and records
  phase distributions instead of presenting one aggregate runtime as a diagnosis.
  Every generated file lives below the verified external benchmark root.
  """

  alias Exqlite.Sqlite3
  alias VialKeeper.Attachments
  alias VialKeeper.Attachments.{FilesystemStore, Representation, StoreRef}

  alias VialKeeper.Bench.{Reports, Root, Runtime, Statistics}
  alias VialKeeper.DatabaseBundle
  alias VialKeeper.DurableFS
  alias VialKeeper.Documents
  alias VialKeeper.JSON.Canonical
  alias VialKeeper.Query
  alias VialKeeper.Revisions.Id
  alias VialKeeper.Search
  alias VialKeeper.Search.Tantivy
  alias VialKeeper.Storage.Services
  alias VialKeeper.Storage.SQLite.{Adapter, Connection, Schema, TermBlob}

  @sections [:documents, :database, :attachments, :search]
  @default_counts [100, 1_000, 10_000]
  @default_batch_sizes [1, 10, 100, 500]
  @default_search_batch_sizes [500, 1_000, 2_000, 5_000]
  @default_attachment_sizes [65_536, 1_048_576, 16_777_216]
  @default_attachment_chunk_sizes [65_536, 262_144, 1_048_576]
  @default_attachment_concurrency [1, 2, 4, 8, 16]
  @database_create_samples 20
  @chunk_size 65_536
  @mutation_phase_event [:vial_keeper, :document, :mutation, :phase]
  @attachment_phase_events [
    [:vial_keeper, :attachment, :store, :phase],
    [:vial_keeper, :attachment, :upload, :phase]
  ]

  @begin_sql "BEGIN IMMEDIATE"
  @commit_sql "COMMIT"
  @rollback_sql "ROLLBACK"

  @document_insert_sql """
  INSERT INTO documents(
    document_id, winning_revision, winning_body_json, winning_body_term,
    winning_deleted, update_sequence
  ) VALUES (?, ?, ?, ?, 0, ?)
  """

  @revision_insert_sql """
  INSERT INTO revisions(
    doc_key, revision_id, generation, parent_revision, history_id, digest,
    deleted, body_json, body_term, insertion_sequence, is_leaf
  ) VALUES (?, ?, 1, NULL, ?, ?, 0, ?, ?, 0, 1)
  """

  @change_insert_sql """
  INSERT INTO changes(
    sequence, doc_key, document_id, winning_revision, winning_deleted,
    leaf_set_json, leaf_set_term, origin
  ) VALUES (?, ?, ?, ?, 0, ?, ?, 'local')
  """

  @sequence_update_sql "UPDATE db_meta SET current_sequence = ? WHERE id = 1"

  @pending_upsert_sql """
  INSERT INTO local_records(namespace, record_key, record_version, value_json)
  VALUES ('replication_state', 'pending_local_causal', 1, ?)
  ON CONFLICT(namespace, record_key) DO UPDATE SET
    record_version = record_version + 1,
    value_json = excluded.value_json
  """

  @pending_json Canonical.encode!(%{"pending_any" => true, "peers" => %{}})

  @type phase_samples :: %{optional(atom()) => [non_neg_integer()]}

  @spec run(keyword()) :: {:ok, binary()} | {:error, binary()}
  def run(opts \\ []) when is_list(opts) do
    with {:ok, context} <- Root.load_or_configure(opts),
         {:ok, config} <- configuration(opts),
         {:ok, work_path} <- Root.work_run_path(context, "diagnostics", run_id()),
         :ok <- File.mkdir_p(work_path) do
      try do
        Runtime.with_isolated(context, fn ->
          results = run_sections(context, work_path, config)

          report =
            Reports.envelope(
              context,
              %{
                "benchmark" => "performance_diagnostics",
                "configuration" => json_configuration(config),
                "sqlite" => sqlite_settings(),
                "tantivy" => tantivy_settings()
              },
              results
            )

          Reports.write(context, "performance-diagnostics.json", report, opts)
        end)
      after
        _ = Root.remove_work_run!(context, work_path)
      end
    end
  end

  defp configuration(opts) do
    with {:ok, sections} <- parse_sections(opts[:section]),
         {:ok, document_mode} <- parse_document_mode(opts[:document_mode]),
         {:ok, attachment_mode} <- parse_attachment_mode(opts[:attachment_mode]),
         {:ok, counts} <- parse_positive_list(opts[:counts], @default_counts, "counts"),
         {:ok, batch_sizes} <-
           parse_bounded_list(opts[:batch_sizes], @default_batch_sizes, "batch-sizes", 500),
         {:ok, search_batch_sizes} <-
           parse_bounded_list(
             opts[:search_batch_sizes],
             @default_search_batch_sizes,
             "search-batch-sizes",
             5_000
           ),
         {:ok, attachment_sizes} <-
           parse_positive_list(
             opts[:attachment_sizes],
             @default_attachment_sizes,
             "attachment-sizes"
           ),
         {:ok, attachment_chunk_sizes} <-
           parse_positive_list(
             opts[:attachment_chunk_sizes],
             @default_attachment_chunk_sizes,
             "attachment-chunk-sizes"
           ),
         {:ok, attachment_concurrency} <-
           parse_bounded_list(
             opts[:attachment_concurrency],
             @default_attachment_concurrency,
             "attachment-concurrency",
             64
           ),
         :ok <- validate_iterations(opts[:iterations]),
         :ok <- validate_warmup(opts[:warmup]) do
      {:ok,
       %{
         sections: sections,
         document_mode: document_mode,
         attachment_mode: attachment_mode,
         counts: counts,
         batch_sizes: batch_sizes,
         search_batch_sizes: search_batch_sizes,
         attachment_sizes: attachment_sizes,
         attachment_chunk_sizes: attachment_chunk_sizes,
         attachment_concurrency: attachment_concurrency,
         iterations: opts[:iterations] || 3,
         warmup: opts[:warmup] || 0
       }}
    end
  end

  defp run_sections(context, work_path, config) do
    Map.new(config.sections, fn
      :documents -> {"documents", document_controls(context, work_path, config)}
      :database -> {"database_create", database_create_controls(context, work_path)}
      :attachments -> {"attachments", attachment_controls(context, work_path, config)}
      :search -> {"search", search_controls(context, work_path, config)}
    end)
  end

  defp document_controls(context, work_path, config) do
    Enum.map(config.counts, fn count ->
      documents = Enum.map(0..(count - 1), &fixture_document/1)

      case config.document_mode do
        :all ->
          %{
            "documents" => count,
            "raw_sqlite_per_document" => raw_document_control(work_path, documents),
            "storage_service_per_document" => service_document_control(work_path, documents),
            "documents_put" => public_put_control(context, documents),
            "documents_bulk_write" => bulk_document_controls(context, documents, config)
          }

        :bulk ->
          %{
            "documents" => count,
            "documents_bulk_write" => bulk_document_controls(context, documents, config)
          }
      end
    end)
  end

  defp bulk_document_controls(context, documents, config) do
    Enum.map(config.batch_sizes, fn batch_size ->
      public_bulk_control(context, documents, batch_size)
    end)
  end

  defp raw_document_control(work_path, documents) do
    with_adapter(work_path, "raw-documents", fn adapter ->
      statements = prepare_raw_statements(adapter.conn)

      try do
        {total_us, samples, phases} =
          Enum.reduce(documents, {0, [], %{}}, fn document, {total, samples, phases} ->
            {elapsed, phase_row} = raw_insert_document(adapter.conn, statements, document)
            {total + elapsed, [elapsed | samples], merge_phases(phases, phase_row)}
          end)

        phase_report(total_us, Enum.reverse(samples), phases, length(documents))
        |> Map.put("pragmas", connection_pragmas(adapter.conn))
      after
        release_statements(adapter.conn, statements)
      end
    end)
  end

  defp service_document_control(work_path, documents) do
    with_adapter(work_path, "service-documents", fn adapter ->
      context = Adapter.to_context(adapter)

      samples =
        Enum.map(documents, fn document ->
          timed_result!(fn ->
            Services.apply_local_mutation(context, %{
              operation: :put,
              document_id: document.id,
              history_id: document.history_id,
              body: document.body,
              attachments: %{}
            })
          end)
        end)

      summarize_operations(samples, length(documents))
      |> Map.put("pragmas", connection_pragmas(adapter.conn))
    end)
  end

  defp public_put_control(context, documents) do
    with_public_database(context, "documents-put", fn uuid ->
      {samples, phase_samples} = capture_mutation_phases(fn -> put_documents(uuid, documents) end)

      summarize_operations(samples, length(documents))
      |> Map.put("mutation_phases", mutation_phase_report(phase_samples))
    end)
  end

  defp put_documents(uuid, documents) do
    Enum.map(documents, fn document ->
      timed_result!(fn ->
        Documents.put(uuid, %{"id" => document.id, "body" => document.body})
      end)
    end)
  end

  defp capture_mutation_phases(fun) when is_function(fun, 0) do
    handler_id = {__MODULE__, self(), make_ref()}
    collector = self()

    :ok =
      :telemetry.attach(
        handler_id,
        @mutation_phase_event,
        &__MODULE__.handle_mutation_phase/4,
        collector
      )

    try do
      samples = fun.()
      {samples, drain_mutation_phases(%{})}
    after
      :telemetry.detach(handler_id)
    end
  end

  @doc "Forwards mutation phase telemetry to the diagnostic collector process."
  @spec handle_mutation_phase([atom()], map(), map(), pid()) :: :ok
  def handle_mutation_phase(_event, measurements, metadata, collector) do
    send(collector, {:diagnostic_mutation_phase, measurements, metadata})
    :ok
  end

  defp drain_mutation_phases(acc) do
    receive do
      {:diagnostic_mutation_phase, %{duration: duration}, %{phase: phase, operation: :put}}
      when is_integer(duration) and is_atom(phase) ->
        duration_us = System.convert_time_unit(duration, :native, :microsecond)
        drain_mutation_phases(Map.update(acc, phase, [duration_us], &[duration_us | &1]))
    after
      0 -> Map.new(acc, fn {phase, samples} -> {phase, Enum.reverse(samples)} end)
    end
  end

  defp mutation_phase_report(phase_samples) do
    distributions =
      Map.new(phase_samples, fn {phase, samples} ->
        {Atom.to_string(phase),
         Statistics.summarize(samples, 1)
         |> Map.put("total_elapsed_us", Enum.sum(samples))
         |> Map.put("sample_count", length(samples))}
      end)

    additive_phases = Map.drop(phase_samples, [:catalog_route, :validation])

    top_three =
      additive_phases
      |> Enum.map(fn {phase, samples} ->
        %{"phase" => Atom.to_string(phase), "total_elapsed_us" => Enum.sum(samples)}
      end)
      |> Enum.sort_by(& &1["total_elapsed_us"], :desc)
      |> Enum.take(3)

    %{
      "distributions" => distributions,
      "top_three_additive_contributors" => top_three,
      "inclusive_phases" => ["catalog_route", "validation"]
    }
  end

  defp public_bulk_control(context, documents, batch_size) do
    with_public_database(context, "documents-bulk-#{batch_size}", fn uuid ->
      batches = Enum.chunk_every(documents, batch_size)

      samples =
        Enum.map(batches, fn batch ->
          operations =
            Enum.map(batch, fn document ->
              %{"type" => "put", "id" => document.id, "body" => document.body}
            end)

          timed_result!(fn -> Documents.bulk_write(uuid, operations) end)
        end)

      operation_count = length(documents)

      summarize_operations(samples, operation_count)
      |> Map.merge(%{
        "batch_size" => batch_size,
        "transaction_count" => length(batches)
      })
    end)
  end

  defp database_create_controls(_context, work_path) do
    raw_rows =
      Enum.map(1..@database_create_samples, fn sample ->
        path = unique_path(work_path, "raw-create-#{sample}", ".sqlite3")
        raw_create_sample(path)
      end)

    adapter_rows =
      Enum.map(1..@database_create_samples, fn sample ->
        path = unique_path(work_path, "adapter-create-#{sample}", ".sqlite3")
        profiled_adapter_create_sample(path)
      end)

    %{
      "samples" => @database_create_samples,
      "raw_sqlite_open_configure_schema" => profile_rows(raw_rows),
      "sqlite_adapter_create" => profile_rows(adapter_rows)
    }
  end

  defp raw_create_sample(path) do
    schema_path = Path.join([Application.app_dir(:vial_keeper), "priv", "sqlite", "schema_v1.sql"])
    schema_sql = File.read!(schema_path)
    started = System.monotonic_time(:microsecond)

    {open_us, {:ok, conn}} = :timer.tc(fn -> Sqlite3.open(path) end)
    {configure_us, :ok} = :timer.tc(fn -> Schema.configure(conn) end)
    {begin_us, :ok} = :timer.tc(fn -> Sqlite3.execute(conn, @begin_sql) end)
    {schema_us, :ok} = :timer.tc(fn -> execute_script(conn, schema_sql) end)
    {commit_us, :ok} = :timer.tc(fn -> Sqlite3.execute(conn, @commit_sql) end)
    {close_us, :ok} = :timer.tc(fn -> Sqlite3.close(conn) end)

    %{
      total: System.monotonic_time(:microsecond) - started,
      phases: %{
        open: open_us,
        configure: configure_us,
        transaction_begin: begin_us,
        schema: schema_us,
        commit: commit_us,
        close: close_us
      }
    }
  end

  defp profiled_adapter_create_sample(path) do
    started = System.monotonic_time(:microsecond)
    preparation_started = System.monotonic_time(:microsecond)
    {:ok, config} = VialKeeper.Config.merge_and_bound(VialKeeper.Config.defaults())
    {:ok, config_json} = Canonical.encode(config)
    uuid = VialKeeper.UUID.v4()
    preparation_us = System.monotonic_time(:microsecond) - preparation_started
    {open_us, {:ok, conn}} = :timer.tc(fn -> Connection.open(path) end)
    {configure_us, :ok} = :timer.tc(fn -> Schema.configure(conn) end)
    {create_us, :ok} = :timer.tc(fn -> Schema.create(conn, uuid, config_json) end)
    {validate_us, {:ok, _identity}} = :timer.tc(fn -> Schema.validate(conn) end)
    {close_us, :ok} = :timer.tc(fn -> Connection.close(conn) end)

    %{
      total: System.monotonic_time(:microsecond) - started,
      phases: %{
        preparation: preparation_us,
        open: open_us,
        configure: configure_us,
        schema_and_metadata_transaction: create_us,
        validate: validate_us,
        close: close_us
      }
    }
  end

  defp attachment_controls(context, work_path, config) do
    case config.attachment_mode do
      :all ->
        %{
          "by_size" => attachment_size_controls(context, work_path, config),
          "bounded_batch_concurrency" => attachment_concurrency_control(context, config),
          "metadata_batch_800" => attachment_metadata_batch_control(context, config)
        }

      :concurrency ->
        %{
          "bounded_batch_concurrency" => attachment_concurrency_control(context, config)
        }
    end
  end

  defp attachment_size_controls(context, work_path, config) do
    by_size =
      Enum.map(config.attachment_sizes, fn size ->
        payload = deterministic_payload(size, "size-#{size}")

        by_chunk_size =
          Enum.map(config.attachment_chunk_sizes, fn chunk_size ->
            chunks = payload_chunks(payload, chunk_size)

            %{
              "chunk_bytes" => chunk_size,
              "raw_non_durable_copy" => raw_filesystem_control(work_path, chunks, size, config),
              "raw_durable_cas" => raw_durable_cas_control(work_path, chunks, payload, config),
              "filesystem_store" => filesystem_store_control(work_path, chunks, size, config),
              "attachments_facade" => attachment_facade_control(context, chunks, size, config)
            }
          end)

        %{
          "logical_bytes" => size,
          "by_chunk_size" => by_chunk_size,
          "document_reference" => attachment_reference_control(context, payload, config)
        }
      end)

    by_size
  end

  defp raw_filesystem_control(work_path, chunks, logical_bytes, config) do
    rows =
      repeated_samples(config, fn sample ->
        directory = unique_path(work_path, "raw-copy-#{sample}", "")
        :ok = File.mkdir_p(directory)
        tmp = Path.join(directory, "upload.tmp")
        dest = Path.join(directory, "installed.bin")
        started = System.monotonic_time(:microsecond)
        {write_us, :ok} = :timer.tc(fn -> write_chunks(tmp, chunks) end)
        {install_us, :ok} = :timer.tc(fn -> File.rename(tmp, dest) end)

        %{
          total: System.monotonic_time(:microsecond) - started,
          phases: %{payload_write: write_us, rename_install: install_us}
        }
      end)

    summarize_attachment_rows(rows, logical_bytes)
  end

  defp raw_durable_cas_control(work_path, chunks, payload, config) do
    rows =
      repeated_samples(config, fn sample ->
        bundle_path = unique_path(work_path, "raw-durable-#{sample}", ".vialkeeper")
        {:ok, bundle} = DatabaseBundle.create(bundle_path)
        raw_durable_cas_sample(bundle, chunks, payload)
      end)

    summarize_attachment_rows(rows, byte_size(payload))
  end

  defp filesystem_store_control(work_path, chunks, logical_bytes, config) do
    rows =
      repeated_samples(config, fn sample ->
        bundle_path = unique_path(work_path, "store-attachment-#{sample}", ".vialkeeper")
        {:ok, bundle} = DatabaseBundle.create(bundle_path)
        ref = StoreRef.bundle_local(bundle)

        {row, phases} =
          capture_attachment_phases(fn ->
            started = System.monotonic_time(:microsecond)
            {:ok, writer} = FilesystemStore.begin_put(ref, logical_bytes + 1, %{})
            Enum.each(chunks, fn chunk -> :ok = FilesystemStore.write_chunk(writer, chunk) end)
            {:ok, result} = FilesystemStore.finish_put(writer)

            %{
              total: System.monotonic_time(:microsecond) - started,
              encoding: result.encoding
            }
          end)

        Map.put(row, :phases, Map.get(phases, :store, %{}))
      end)

    report = summarize_attachment_rows(rows, logical_bytes)
    encodings = rows |> Enum.map(&Atom.to_string(&1.encoding)) |> Enum.uniq()
    Map.put(report, "encodings", encodings)
  end

  defp attachment_facade_control(context, chunks, logical_bytes, config) do
    rows =
      repeated_samples(config, fn sample ->
        with_public_database(context, "attachment-facade-#{sample}", fn uuid ->
          {row, phases} =
            capture_attachment_phases(fn ->
              started = System.monotonic_time(:microsecond)
              {:ok, result} = Attachments.upload_stream(uuid, chunks)

              %{
                total: System.monotonic_time(:microsecond) - started,
                encoding: result.encoding
              }
            end)

          Map.put(row, :phases, flatten_attachment_phases(phases))
        end)
      end)

    report = summarize_attachment_rows(rows, logical_bytes)
    encodings = rows |> Enum.map(&Atom.to_string(&1.encoding)) |> Enum.uniq()
    Map.put(report, "encodings", encodings)
  end

  defp attachment_reference_control(context, payload, config) do
    rows =
      repeated_samples(config, fn sample ->
        with_public_database(context, "attachment-reference-#{sample}", fn uuid ->
          {:ok, uploaded} = Attachments.upload_stream(uuid, payload_chunks(payload, @chunk_size))
          digest = Map.fetch!(uploaded, :blob)

          %{
            total:
              timed_result!(fn ->
                Documents.put(uuid, %{
                  "id" => "attachment-document",
                  "body" => %{"title" => "attachment diagnostic"},
                  "attachments" => %{
                    "payload.bin" => %{
                      "blob" => digest,
                      "content_type" => "application/octet-stream"
                    }
                  }
                })
              end),
            phases: %{}
          }
        end)
      end)

    summarize_attachment_rows(rows, 0)
  end

  defp attachment_concurrency_control(context, config) do
    files = 16
    bytes_per_file = 1_048_576

    Enum.map(config.attachment_concurrency, fn concurrency ->
      rows =
        repeated_samples(config, fn sample ->
          with_public_database(
            context,
            "attachment-concurrency-#{concurrency}-#{sample}",
            fn uuid ->
              configure_attachment_write_limit!(uuid, Enum.max(config.attachment_concurrency))

              sources =
                Enum.map(1..files, fn index ->
                  payload = deterministic_payload(bytes_per_file, "batch-#{sample}-#{index}")

                  %{
                    key: index,
                    source: payload_chunks(payload, 262_144)
                  }
                end)

              {row, phases} =
                capture_attachment_phases(fn ->
                  started = System.monotonic_time(:microsecond)

                  {:ok, uploaded} =
                    Attachments.upload_batch(uuid, sources, max_concurrency: concurrency)

                  _ = uploaded
                  %{total: System.monotonic_time(:microsecond) - started}
                end)

              Map.put(row, :phases, flatten_attachment_phases(phases))
            end
          )
        end)

      summarize_attachment_rows(rows, files * bytes_per_file)
      |> Map.merge(%{
        "max_concurrency" => concurrency,
        "files_per_sample" => files,
        "files_per_second" =>
          Statistics.per_sec(files * length(rows), Enum.sum(Enum.map(rows, & &1.total)))
      })
    end)
  end

  defp attachment_metadata_batch_control(context, config) do
    files = 800
    bytes_per_file = 65_536
    concurrency = 1

    rows =
      repeated_samples(config, fn sample ->
        with_public_database(context, "attachment-metadata-800-#{sample}", fn uuid ->
          sources =
            Enum.map(1..files, fn index ->
              payload = deterministic_payload(bytes_per_file, "metadata-#{sample}-#{index}")
              %{key: index, source: [payload]}
            end)

          started = System.monotonic_time(:microsecond)

          {:ok, uploaded} =
            Attachments.upload_batch(
              uuid,
              sources,
              max_concurrency: concurrency,
              protection_batch_size: 500
            )

          %{
            total: System.monotonic_time(:microsecond) - started,
            phases: %{},
            protected: length(uploaded)
          }
        end)
      end)

    summarize_attachment_rows(rows, files * bytes_per_file)
    |> Map.merge(%{
      "files_per_sample" => files,
      "metadata_transactions_per_sample" => ceil_div(files, 500),
      "max_concurrency" => concurrency,
      "files_per_second" =>
        Statistics.per_sec(files * length(rows), Enum.sum(Enum.map(rows, & &1.total)))
    })
  end

  defp search_controls(context, work_path, config) do
    count = List.last(config.counts)
    documents = Enum.map(0..(count - 1), &search_document/1)
    definition = %{"index_id" => "diagnostic-search", "fields" => ["/title", "/text"]}

    raw =
      Enum.map(config.search_batch_sizes, fn batch_size ->
        raw_tantivy_control(work_path, documents, definition, batch_size)
      end)

    boundary =
      Enum.map(config.search_batch_sizes, fn batch_size ->
        tantivy_boundary_control(work_path, documents, definition, batch_size)
      end)

    owner =
      Enum.map(config.search_batch_sizes, fn batch_size ->
        search_owner_control(work_path, documents, definition, batch_size)
      end)

    full_rebuild = full_search_rebuild_control(context, documents)

    %{
      "documents" => count,
      "raw_tantivy_ex_by_batch_size" => raw,
      "vial_keeper_tantivy_by_batch_size" => boundary,
      "search_owner_by_batch_size" => owner,
      "storage_winner_stream_full_rebuild" => full_rebuild,
      "writer_settings" => tantivy_settings()
    }
  end

  defp raw_tantivy_control(work_path, documents, definition, batch_size) do
    path = unique_path(work_path, "raw-tantivy", "")
    {schema_us, schema} = :timer.tc(&Tantivy.schema/0)
    {index_us, {:ok, index}} = :timer.tc(fn -> TantivyEx.Index.create_in_dir(path, schema) end)

    {writer_us, {:ok, writer}} =
      :timer.tc(fn ->
        TantivyEx.IndexWriter.new(index, VialKeeper.Config.search_writer_memory_bytes())
      end)

    {prepare_us, prepared} =
      :timer.tc(fn ->
        Enum.map(documents, fn document ->
          {:ok, content} = Tantivy.content(document.body, definition)
          %{"id" => document.id, "content" => content}
        end)
      end)

    {add_us, :ok} =
      :timer.tc(fn ->
        prepared
        |> Enum.chunk_every(batch_size)
        |> Enum.each(&raw_native_tantivy_batch(writer, &1, schema))
      end)

    {commit_us, :ok} = :timer.tc(fn -> TantivyEx.IndexWriter.commit(writer) end)
    {searcher_us, {:ok, _searcher}} = :timer.tc(fn -> TantivyEx.Searcher.new(index) end)
    total = schema_us + index_us + writer_us + prepare_us + add_us + commit_us + searcher_us

    phase_report(
      total,
      [total],
      %{
        schema: [schema_us],
        index: [index_us],
        writer: [writer_us],
        content_preparation: [prepare_us],
        add_batch: [add_us],
        commit: [commit_us],
        searcher: [searcher_us]
      },
      length(documents)
    )
    |> Map.put("batch_size", batch_size)
    |> Map.put("batch_count", ceil_div(length(documents), batch_size))
  end

  defp tantivy_boundary_control(work_path, documents, definition, batch_size) do
    path = unique_path(work_path, "vialkeeper-tantivy", "")
    {create_us, {:ok, handle}} = :timer.tc(fn -> Tantivy.create(path, definition) end)

    batches = documents |> Enum.map(&{&1.id, &1.body}) |> Enum.chunk_every(batch_size)

    {add_us, add_result} =
      :timer.tc(fn ->
        Enum.reduce_while(batches, :ok, fn batch, :ok ->
          case Tantivy.add_batch(handle, batch) do
            :ok -> {:cont, :ok}
            {:error, _} = error -> {:halt, error}
          end
        end)
      end)

    :ok = add_result
    {commit_us, {:ok, _committed}} = :timer.tc(fn -> Tantivy.commit(handle) end)
    total = create_us + add_us + commit_us

    phase_report(
      total,
      [total],
      %{create: [create_us], add_batch: [add_us], commit_and_searcher: [commit_us]},
      length(documents)
    )
    |> Map.put("batch_size", batch_size)
    |> Map.put("batch_count", length(batches))
  end

  defp raw_native_tantivy_batch(_writer, [], _schema), do: :ok

  defp raw_native_tantivy_batch(writer, documents, schema) do
    expected = length(documents)

    with result when is_binary(result) <-
           TantivyEx.Native.writer_add_document_batch(writer, documents, schema),
         {:ok, %{"successful" => ^expected, "errors" => 0}} <- Jason.decode(result) do
      :ok
    else
      other -> Mix.raise("raw Tantivy batch failed: #{inspect(other)}")
    end
  end

  defp search_owner_control(work_path, documents, definition, batch_size) do
    bundle_path = unique_path(work_path, "search-owner", ".vialkeeper")
    {:ok, bundle} = DatabaseBundle.create(bundle_path)
    {:ok, adapter} = Adapter.create(Adapter.artifact_path(bundle.root))
    context = Adapter.to_context(adapter)
    index_id = Map.fetch!(definition, "index_id")

    try do
      {begin_us, :ok} = :timer.tc(fn -> Search.begin_rebuild(context, index_id, definition) end)

      {batches_us, added} =
        :timer.tc(fn ->
          documents
          |> Enum.chunk_every(batch_size)
          |> Enum.reduce(0, fn batch, count ->
            {:ok, batch_count} = Search.rebuild_batch(context, index_id, batch)
            count + batch_count
          end)
        end)

      {finish_us, {:ok, ^added}} =
        :timer.tc(fn -> Search.finish_rebuild(context, index_id) end)

      total = begin_us + batches_us + finish_us

      phase_report(
        total,
        [total],
        %{begin_rebuild: [begin_us], owner_batches: [batches_us], finish_publish: [finish_us]},
        length(documents)
      )
      |> Map.put("batch_size", batch_size)
      |> Map.put("batch_count", ceil_div(length(documents), batch_size))
    after
      Search.stop(context)
      _ = Adapter.close(adapter)
    end
  end

  defp full_search_rebuild_control(context, documents) do
    with_public_database(context, "search-full-rebuild", fn uuid ->
      documents
      |> Enum.chunk_every(500)
      |> Enum.each(fn batch ->
        operations =
          Enum.map(batch, fn document ->
            %{"type" => "put", "id" => document.id, "body" => document.body}
          end)

        {:ok, _results} = Documents.bulk_write(uuid, operations)
      end)

      elapsed =
        timed_result!(fn ->
          Query.create_index(uuid, %{
            "name" => "diagnostic-search",
            "type" => "full_text",
            "fields" => ["/title", "/text"]
          })
        end)

      summarize_operations([elapsed], length(documents))
    end)
  end

  defp with_adapter(work_path, label, fun) do
    path = unique_path(work_path, label, ".sqlite3")

    case Adapter.create(path) do
      {:ok, adapter} ->
        try do
          fun.(adapter)
        after
          _ = Adapter.close(adapter)
        end

      {:error, error} ->
        Mix.raise("could not create diagnostic adapter: #{inspect(error)}")
    end
  end

  defp with_public_database(context, label, fun) do
    id = run_id()

    case Runtime.create_work_database(context, "diagnostics/#{label}", id) do
      {:ok, uuid, relative} ->
        try do
          fun.(uuid)
        after
          Runtime.close_work_database(context, uuid, relative)
        end

      {:error, message} ->
        Mix.raise(message)
    end
  end

  defp raw_insert_document(conn, statements, document) do
    total_started = System.monotonic_time(:microsecond)
    {begin_us, _} = :timer.tc(fn -> raw_run!(conn, statements.begin) end)
    mutation_started = System.monotonic_time(:microsecond)

    try do
      raw_run!(conn, statements.document_insert, [
        document.id,
        document.revision_id,
        document.body_json,
        TermBlob.bind(document.body_term),
        document.sequence
      ])

      {:ok, doc_key} = Sqlite3.last_insert_rowid(conn)

      raw_run!(conn, statements.revision_insert, [
        doc_key,
        document.revision_id,
        document.history_id,
        document.digest,
        document.body_json,
        TermBlob.bind(document.body_term)
      ])

      raw_run!(conn, statements.change_insert, [
        document.sequence,
        doc_key,
        document.id,
        document.revision_id,
        document.leaf_json,
        TermBlob.bind(document.leaf_term)
      ])

      raw_run!(conn, statements.sequence_update, [document.sequence])
      raw_run!(conn, statements.pending_upsert, [@pending_json])
      mutation_us = System.monotonic_time(:microsecond) - mutation_started
      {commit_us, _} = :timer.tc(fn -> raw_run!(conn, statements.commit) end)
      total_us = System.monotonic_time(:microsecond) - total_started
      {total_us, %{transaction_begin: [begin_us], mutation_sql: [mutation_us], commit: [commit_us]}}
    rescue
      exception ->
        _ = raw_run(conn, statements.rollback, [])
        reraise exception, __STACKTRACE__
    end
  end

  defp prepare_raw_statements(conn) do
    Map.new(
      [
        begin: @begin_sql,
        commit: @commit_sql,
        rollback: @rollback_sql,
        document_insert: @document_insert_sql,
        revision_insert: @revision_insert_sql,
        change_insert: @change_insert_sql,
        sequence_update: @sequence_update_sql,
        pending_upsert: @pending_upsert_sql
      ],
      fn {name, sql} ->
        {:ok, statement} = Sqlite3.prepare(conn, String.trim(sql))
        {name, statement}
      end
    )
  end

  defp release_statements(conn, statements) do
    Enum.each(statements, fn {_name, statement} -> _ = Sqlite3.release(conn, statement) end)
  end

  defp raw_run!(conn, statement, params \\ []) do
    case raw_run(conn, statement, params) do
      {:ok, rows} -> rows
      {:error, reason} -> Mix.raise("raw diagnostic SQL failed: #{inspect(reason)}")
    end
  end

  defp raw_run(conn, statement, params) do
    with :ok <- Sqlite3.reset(statement),
         :ok <- Sqlite3.bind(statement, params) do
      raw_step(conn, statement, [])
    end
  end

  defp raw_step(conn, statement, rows) do
    case Sqlite3.step(conn, statement) do
      {:row, row} -> raw_step(conn, statement, [row | rows])
      :done -> {:ok, Enum.reverse(rows)}
      :busy -> {:error, :busy}
      {:error, reason} -> {:error, reason}
      other -> {:error, other}
    end
  end

  defp connection_pragmas(conn) do
    Map.new(
      ["journal_mode", "synchronous", "foreign_keys", "locking_mode", "trusted_schema"],
      fn pragma ->
        value =
          case Connection.pragma(conn, pragma) do
            {:ok, [[value]]} -> value
            _ -> nil
          end

        {pragma, value}
      end
    )
  end

  defp sqlite_settings do
    path = ":memory:"
    {:ok, adapter} = Adapter.create(path, %{storage_mode: :memory})

    try do
      %{
        "disk_expected" => %{"journal_mode" => "wal", "synchronous" => "full"},
        "memory_observed" => connection_pragmas(adapter.conn),
        "busy_timeout_ms" => 0
      }
    after
      _ = Adapter.close(adapter)
    end
  end

  defp tantivy_settings do
    %{
      "backend_version" => Tantivy.backend_version(),
      "schema_fingerprint" => Tantivy.schema_fingerprint(),
      "writer_memory_bytes" => VialKeeper.Config.search_writer_memory_bytes()
    }
  end

  defp fixture_document(index) do
    id = "diagnostic-" <> String.pad_leading(Integer.to_string(index), 8, "0")
    history_id = deterministic_uuid("history", id)
    body = document_body(index)
    {:ok, revision_id} = Id.calculate(id, history_id, nil, false, body, %{})
    body_json = Canonical.encode!(body)

    leaf = [%{"revision" => revision_id, "history_id" => history_id, "deleted" => false}]
    leaf_json = Canonical.encode!(leaf)

    %{
      id: id,
      history_id: history_id,
      revision_id: revision_id,
      digest: revision_id |> String.split("-", parts: 2) |> List.last(),
      body: body,
      body_json: body_json,
      body_term: encode_term!(body, body_json),
      leaf_json: leaf_json,
      leaf_term: encode_term!(leaf, leaf_json),
      sequence: index + 1
    }
  end

  defp search_document(index) do
    %{
      id: "search-" <> String.pad_leading(Integer.to_string(index), 8, "0"),
      body: document_body(index)
    }
  end

  defp document_body(index) do
    %{
      "category" => if(rem(index, 4) == 0, do: "task", else: "article"),
      "priority" => rem(index, 100),
      "title" => "Simple Wikipedia diagnostic article #{index}",
      "text" => "deterministic benchmark content for article #{index}",
      "tags" => ["benchmark", "simplewiki"]
    }
  end

  defp encode_term!(value, json) do
    case TermBlob.encode(value, json) do
      {:ok, binary} -> binary
      {:error, error} -> Mix.raise("could not encode diagnostic term: #{inspect(error)}")
    end
  end

  defp deterministic_payload(size, label) do
    key = :crypto.hash(:sha256, "vialkeeper-attachment-diagnostic:" <> label)
    :crypto.crypto_one_time(:aes_256_ctr, key, <<0::128>>, :binary.copy(<<0>>, size), true)
  end

  defp payload_chunks(payload, chunk_size), do: do_payload_chunks(payload, chunk_size, [])

  defp do_payload_chunks(<<>>, _chunk_size, acc), do: Enum.reverse(acc)

  defp do_payload_chunks(payload, chunk_size, acc) when byte_size(payload) <= chunk_size,
    do: Enum.reverse([payload | acc])

  defp do_payload_chunks(payload, chunk_size, acc) do
    <<chunk::binary-size(^chunk_size), rest::binary>> = payload
    do_payload_chunks(rest, chunk_size, [chunk | acc])
  end

  defp repeated_samples(config, fun) when is_function(fun, 1) do
    total = config.warmup + config.iterations

    1..total
    |> Enum.map(fun)
    |> Enum.drop(config.warmup)
  end

  defp write_chunks(path, chunks) do
    {:ok, fd} = File.open(path, [:write, :binary, :raw, :exclusive])

    try do
      Enum.each(chunks, fn chunk -> :ok = :file.write(fd, chunk) end)
      :ok
    after
      :ok = File.close(fd)
    end
  end

  defp raw_durable_cas_sample(bundle, chunks, payload) do
    tmp = Path.join(bundle.tmp_path, "raw-control-upload")
    started = System.monotonic_time(:microsecond)
    {open_us, {:ok, fd}} = :timer.tc(fn -> File.open(tmp, [:write, :binary, :raw, :exclusive]) end)

    {hash_ctx, hash_us, write_us} =
      Enum.reduce(chunks, {:crypto.hash_init(:sha256), 0, 0}, fn chunk,
                                                                 {hash_ctx, hash_us, write_us} ->
        {elapsed_hash, hash_ctx} = :timer.tc(fn -> :crypto.hash_update(hash_ctx, chunk) end)
        {elapsed_write, :ok} = :timer.tc(fn -> :file.write(fd, chunk) end)
        {hash_ctx, hash_us + elapsed_hash, write_us + elapsed_write}
      end)

    {digest_us, digest} =
      :timer.tc(fn -> :crypto.hash_final(hash_ctx) |> Base.encode16(case: :lower) end)

    {:ok, descriptor} =
      Representation.descriptor(%{
        encoding: :raw,
        logical_digest: digest,
        logical_length: byte_size(payload),
        payload_length: byte_size(payload),
        payload_sha256: digest
      })

    {:ok, trailer} = Representation.encode_trailer(descriptor)
    {trailer_us, :ok} = :timer.tc(fn -> :file.write(fd, trailer) end)
    {sync_us, :ok} = :timer.tc(fn -> :file.sync(fd) end)
    {close_us, :ok} = :timer.tc(fn -> File.close(fd) end)
    destination_dir = Path.join(bundle.blobs_path, String.slice(digest, 0, 2))
    :ok = File.mkdir_p(destination_dir)
    destination = Path.join(destination_dir, digest <> ".blob")

    {install_us, :ok} =
      :timer.tc(fn ->
        :ok = :file.make_link(tmp, destination)
        :ok = File.rm(tmp)
      end)

    {directory_sync_us, :ok} = :timer.tc(fn -> DurableFS.sync_directory(destination_dir) end)

    %{
      total: System.monotonic_time(:microsecond) - started,
      phases: %{
        begin: open_us,
        logical_hash: hash_us,
        digest_finalize: digest_us,
        payload_write: write_us,
        trailer_write: trailer_us,
        file_sync: sync_us,
        file_close: close_us,
        cas_install: install_us,
        directory_sync: directory_sync_us
      }
    }
  end

  defp capture_attachment_phases(fun) when is_function(fun, 0) do
    handler_id = {__MODULE__, :attachment, self(), make_ref()}
    collector = self()

    :ok =
      :telemetry.attach_many(
        handler_id,
        @attachment_phase_events,
        &__MODULE__.handle_attachment_phase/4,
        collector
      )

    try do
      row = fun.()
      {row, drain_attachment_phases(%{store: %{}, upload: %{}})}
    after
      :telemetry.detach(handler_id)
    end
  end

  @doc "Forwards attachment phase telemetry to the diagnostic collector process."
  @spec handle_attachment_phase([atom()], map(), map(), pid()) :: :ok
  def handle_attachment_phase(event, measurements, metadata, collector) do
    send(collector, {:diagnostic_attachment_phase, event, measurements, metadata})
    :ok
  end

  defp drain_attachment_phases(acc) do
    receive do
      {:diagnostic_attachment_phase, event, %{duration: duration}, %{phase: phase}}
      when is_integer(duration) and is_atom(phase) ->
        kind = if event == hd(@attachment_phase_events), do: :store, else: :upload
        duration_us = System.convert_time_unit(duration, :native, :microsecond)

        updated =
          update_in(acc, [kind], fn phases ->
            Map.update(phases, phase, [duration_us], &[duration_us | &1])
          end)

        drain_attachment_phases(updated)
    after
      0 ->
        Map.new(acc, fn {kind, phases} ->
          {kind, Map.new(phases, fn {phase, values} -> {phase, Enum.reverse(values)} end)}
        end)
    end
  end

  defp flatten_attachment_phases(groups) do
    Map.new(
      for {group, phases} <- groups,
          {phase, samples} <- phases,
          do: {"#{group}.#{phase}", samples}
    )
  end

  defp summarize_attachment_rows(rows, logical_bytes_per_sample) do
    total_samples = Enum.map(rows, & &1.total)

    phases =
      Enum.reduce(rows, %{}, fn row, acc ->
        Map.merge(acc, Map.get(row, :phases, %{}), fn _phase, left, right ->
          List.wrap(left) ++ List.wrap(right)
        end)
      end)

    report =
      summarize_operations(total_samples, length(rows))
      |> Map.put(
        "phases",
        Map.new(phases, fn {phase, samples} ->
          samples = List.wrap(samples)

          {to_string(phase),
           Statistics.summarize(samples, 1)
           |> Map.put("total_elapsed_us", Enum.sum(samples))
           |> Map.put("sample_count", length(samples))}
        end)
      )

    if logical_bytes_per_sample > 0 do
      Map.put(
        report,
        "mib_per_sec",
        Statistics.mib_per_sec(
          logical_bytes_per_sample * length(rows),
          Enum.sum(total_samples)
        )
      )
    else
      report
    end
  end

  defp configure_attachment_write_limit!(uuid, limit) do
    case VialKeeper.Runtime.DatabaseCatalog.command(
           uuid,
           {:command, :update_config,
            %{"attachments" => %{"max_concurrent_attachment_writes" => limit}}}
         ) do
      {:ok, _} -> :ok
      {:error, error} -> Mix.raise("could not configure attachment concurrency: #{inspect(error)}")
    end
  end

  defp timed_result!(fun) do
    {elapsed, result} = :timer.tc(fun)

    case result do
      :ok -> elapsed
      {:ok, _} -> elapsed
      {:ok, _, _} -> elapsed
      {:error, error} -> Mix.raise("diagnostic operation failed: #{inspect(error)}")
      other -> Mix.raise("diagnostic operation returned unexpectedly: #{inspect(other)}")
    end
  end

  defp summarize_operations(samples, operation_count) do
    total_us = Enum.sum(samples)

    Statistics.summarize(samples, 1)
    |> Map.merge(%{
      "count" => operation_count,
      "total_elapsed_us" => total_us,
      "operations_per_second" => Statistics.per_sec(operation_count, total_us)
    })
  end

  defp phase_report(total_us, samples, phases, operation_count) do
    summarize_operations(samples, operation_count)
    |> Map.put(
      "phases",
      Map.new(phases, fn {phase, values} ->
        {Atom.to_string(phase), Statistics.summarize(values, 1)}
      end)
    )
    |> Map.put("total_elapsed_us", total_us)
  end

  defp profile_rows(rows) do
    total_samples = Enum.map(rows, & &1.total)

    phases =
      rows
      |> Enum.flat_map(fn row -> Map.to_list(row.phases) end)
      |> Enum.group_by(&elem(&1, 0), &elem(&1, 1))

    phase_report(Enum.sum(total_samples), total_samples, phases, length(rows))
  end

  defp merge_phases(acc, row) do
    Map.merge(acc, row, fn _phase, left, right -> left ++ right end)
  end

  defp parse_sections(nil), do: {:ok, @sections}
  defp parse_sections("all"), do: {:ok, @sections}

  defp parse_sections(value) do
    requested = value |> String.split(",", trim: true) |> Enum.map(&section/1)

    if requested != [] and Enum.all?(requested, &is_atom/1) do
      {:ok, requested}
    else
      {:error, "--section must contain documents,database,attachments,search, or all"}
    end
  end

  defp parse_document_mode(nil), do: {:ok, :all}
  defp parse_document_mode("all"), do: {:ok, :all}
  defp parse_document_mode("bulk"), do: {:ok, :bulk}

  defp parse_document_mode(_value),
    do: {:error, "--document-mode must be all or bulk"}

  defp parse_attachment_mode(nil), do: {:ok, :all}
  defp parse_attachment_mode("all"), do: {:ok, :all}
  defp parse_attachment_mode("concurrency"), do: {:ok, :concurrency}

  defp parse_attachment_mode(_value),
    do: {:error, "--attachment-mode must be all or concurrency"}

  defp section("documents"), do: :documents
  defp section("database"), do: :database
  defp section("attachments"), do: :attachments
  defp section("search"), do: :search
  defp section(_), do: nil

  defp parse_positive_list(nil, default, _label), do: {:ok, default}

  defp parse_positive_list(value, _default, label) do
    parse_integer_list(value, label, fn number -> number > 0 end)
  end

  defp parse_bounded_list(nil, default, _label, _max), do: {:ok, default}

  defp parse_bounded_list(value, _default, label, max) do
    parse_integer_list(value, label, fn number -> number > 0 and number <= max end)
  end

  defp parse_integer_list(value, label, valid?) do
    numbers = value |> String.split(",", trim: true) |> Enum.map(&String.to_integer/1)

    if numbers != [] and Enum.all?(numbers, valid?),
      do: {:ok, Enum.uniq(numbers)},
      else: {:error, "--#{label} contains an invalid value"}
  rescue
    ArgumentError -> {:error, "--#{label} must be a comma-separated integer list"}
  end

  defp validate_iterations(nil), do: :ok
  defp validate_iterations(value) when is_integer(value) and value > 0, do: :ok
  defp validate_iterations(_), do: {:error, "--iterations must be positive"}

  defp validate_warmup(nil), do: :ok
  defp validate_warmup(value) when is_integer(value) and value >= 0, do: :ok
  defp validate_warmup(_), do: {:error, "--warmup must be non-negative"}

  defp json_configuration(config) do
    %{
      "sections" => Enum.map(config.sections, &Atom.to_string/1),
      "document_mode" => Atom.to_string(config.document_mode),
      "attachment_mode" => Atom.to_string(config.attachment_mode),
      "document_counts" => config.counts,
      "batch_sizes" => config.batch_sizes,
      "search_batch_sizes" => config.search_batch_sizes,
      "attachment_sizes" => config.attachment_sizes,
      "attachment_chunk_sizes" => config.attachment_chunk_sizes,
      "attachment_concurrency" => config.attachment_concurrency,
      "iterations" => config.iterations,
      "warmup" => config.warmup
    }
  end

  defp deterministic_uuid(namespace, value) do
    hex = :crypto.hash(:sha256, namespace <> ":" <> value) |> Base.encode16(case: :lower)

    <<first::binary-size(8), second::binary-size(4), third::binary-size(4), fourth::binary-size(4),
      last::binary-size(12), _rest::binary>> = hex

    "#{first}-#{second}-4#{binary_part(third, 1, 3)}-8#{binary_part(fourth, 1, 3)}-#{last}"
  end

  defp execute_script(conn, script) do
    script
    |> String.split(";")
    |> Enum.map(&String.trim/1)
    |> Enum.reject(&(&1 == ""))
    |> Enum.reduce_while(:ok, fn statement, :ok ->
      case Sqlite3.execute(conn, statement) do
        :ok -> {:cont, :ok}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
  end

  defp ceil_div(count, divisor), do: div(count + divisor - 1, divisor)

  defp unique_path(work_path, label, suffix) do
    Path.join(work_path, "#{label}-#{run_id()}#{suffix}")
  end

  defp run_id do
    "#{System.system_time(:microsecond)}-#{System.unique_integer([:positive])}"
  end
end
