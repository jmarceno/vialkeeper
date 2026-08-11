defmodule ElixirDB.Benchmarks.ExqliteOverhead do
  @moduledoc """
  Paired low-noise benchmark of ElixirDB SQLite work against direct ExQLite.

  Every case opens three independent databases with the same schema and the
  same deterministic fixture. The measured variants are:

    * `pure_exqlite` — prepared statements through `Exqlite.Sqlite3`.
    * `elixir_db_connection` — the same SQL through the ElixirDB connection
      wrapper and its statement cache.
    * `elixir_db_adapter` — the public SQLite adapter operation.

  Samples are paired by operation number and the variant order alternates. A
  sample's garbage collection is performed before, rather than inside, the
  timed region. Setup, fixture loading, index creation, statement preparation,
  and cleanup are outside the measured region.

  The direct write control is a physical SQLite baseline: it writes the same
  final document, revision, change-feed, metadata, and replication-state rows
  in one prepared transaction. It intentionally does not reproduce ElixirDB's
  validation, revision lookup, conflict handling, JSON hashing, or retention
  orchestration. This makes the write result a useful end-to-end overhead
  number over SQLite, not a claim that those semantics are free in SQLite.
  """

  alias Exqlite.Sqlite3
  alias ElixirDB.JSON.Canonical
  alias ElixirDB.Revisions.Id
  alias ElixirDB.Storage.SQLite.{Adapter, Connection}
  alias ElixirDB.Storage.SQLite.TermBlob
  alias ElixirDB.Benchmarks.ExqliteOverhead.Raw

  @scenarios [:point_read, :bulk_write, :changes_read, :indexed_query]
  @modes [:memory, :disk]
  @variants [:pure_exqlite, :elixir_db_connection, :elixir_db_adapter]

  @default_iterations 30
  @default_warmup 10
  @default_dataset_size 500
  @default_batch_size 50
  @default_read_count 100
  @query_limit 50

  @document_select_sql """
  SELECT doc_key, document_id, winning_revision, winning_body_json,
         winning_deleted, update_sequence
  FROM documents
  WHERE document_id = ?
  """

  @revision_select_sql """
  SELECT revision_id, generation, parent_revision, history_id, digest,
         deleted, body_json, body_term, insertion_sequence
  FROM revisions
  WHERE doc_key = ? AND revision_id = ?
  """

  @attachment_select_sql """
  SELECT attachment_name, blob_digest, logical_size, content_type
  FROM revision_attachments
  WHERE doc_key = ? AND revision_id = ?
  ORDER BY attachment_name
  """

  @changes_select_sql """
  SELECT sequence, document_id, winning_revision, winning_deleted,
         leaf_set_term, origin
  FROM changes
  WHERE sequence > ?
  ORDER BY sequence
  LIMIT ?
  """

  @changes_exists_sql "SELECT EXISTS(SELECT 1 FROM changes WHERE sequence > ?)"

  @indexed_query_sql """
  SELECT document_id, winning_revision, winning_body_term,
         (SELECT count(*)
          FROM documents AS candidate_count
          WHERE candidate_count.winning_deleted = 0
            AND json_type(candidate_count.winning_body_json, '$."category"') = 'text'
            AND json_extract(candidate_count.winning_body_json, '$."category"') = ?)
  FROM documents
  WHERE winning_deleted = 0
    AND json_type(winning_body_json, '$."category"') = 'text'
    AND json_extract(winning_body_json, '$."category"') = ?
  ORDER BY document_id
  LIMIT ?
  """

  @begin_sql "BEGIN IMMEDIATE"
  @rollback_sql "ROLLBACK"
  @commit_sql "COMMIT"

  @document_insert_sql """
  INSERT INTO documents(
    document_id, winning_revision, winning_body_json, winning_body_term,
    winning_deleted, update_sequence
  ) VALUES (?, ?, ?, ?, ?, ?)
  """

  @revision_insert_sql """
  INSERT INTO revisions(
    doc_key, revision_id, generation, parent_revision, history_id, digest,
    deleted, body_json, body_term, insertion_sequence, is_leaf
  ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
  """

  @change_insert_sql """
  INSERT INTO changes(
    sequence, doc_key, document_id, winning_revision, winning_deleted,
    leaf_set_json, leaf_set_term, origin
  ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
  """

  @sequence_update_sql "UPDATE db_meta SET current_sequence = ? WHERE id = 1"

  @local_record_upsert_sql """
  INSERT INTO local_records(namespace, record_key, record_version, value_json)
  VALUES (?, ?, 1, ?)
  ON CONFLICT(namespace, record_key) DO UPDATE SET
    record_version = record_version + 1,
    value_json = excluded.value_json
  """

  @pending_namespace "replication_state"
  @pending_key "pending_local_causal"
  @pending_json Canonical.encode!(%{"pending_any" => true, "peers" => %{}})

  defstruct [:kind, :mode, :adapter, :conn, :path, statements: %{}]

  @doc false
  @spec main([binary()]) :: :ok
  def main(argv) do
    options = parse_options(argv)
    {:ok, _started} = Application.ensure_all_started(:elixir_db)

    config = benchmark_config(options)
    started_at = DateTime.utc_now() |> DateTime.to_iso8601()
    modes = parse_modes(options[:mode])
    scenarios = parse_scenarios(options[:scenario])

    results =
      for mode <- modes, scenario <- scenarios do
        run_case(mode, scenario, config)
      end

    report = %{
      "schema_version" => 1,
      "benchmark" => "elixir_db_overhead_vs_exqlite",
      "started_at" => started_at,
      "git_revision" => git_revision(),
      "runtime" => runtime_metadata(),
      "configuration" => config,
      "results" => results
    }

    output = options[:output] || default_output_path()
    write_report(report, output)
    print_summary(report, output)
    :ok
  end

  defp parse_options(argv) do
    argv = if List.first(argv) == "--", do: tl(argv), else: argv

    {options, positional, invalid} =
      OptionParser.parse(argv,
        strict: [
          mode: :string,
          scenario: :string,
          iterations: :integer,
          warmup: :integer,
          dataset: :integer,
          batch: :integer,
          reads: :integer,
          output: :string,
          help: :boolean
        ],
        aliases: [m: :mode, s: :scenario, o: :output]
      )

    if options[:help] do
      IO.puts(usage())
      System.halt(0)
    end

    if positional != [] or invalid != [] do
      Mix.raise("invalid benchmark arguments: #{inspect(positional ++ invalid)}\n\n#{usage()}")
    end

    options
  end

  defp benchmark_config(options) do
    iterations =
      positive_option(options, :iterations, "ELIXIRDB_OVERHEAD_ITERATIONS", @default_iterations)

    warmup = non_negative_option(options, :warmup, "ELIXIRDB_OVERHEAD_WARMUP", @default_warmup)

    dataset_size =
      positive_option(options, :dataset, "ELIXIRDB_OVERHEAD_DATASET", @default_dataset_size)

    batch_size = positive_option(options, :batch, "ELIXIRDB_OVERHEAD_BATCH", @default_batch_size)
    read_count = positive_option(options, :reads, "ELIXIRDB_OVERHEAD_READS", @default_read_count)

    if batch_size > 500 do
      Mix.raise("--batch must be at most the configured host bulk limit (500)")
    end

    %{
      "iterations" => iterations,
      "warmup" => warmup,
      "dataset_size" => dataset_size,
      "batch_size" => batch_size,
      "read_count" => read_count,
      "query_limit" => @query_limit,
      "gc_before_sample" => true,
      "pair_order" => "alternating",
      "timed_variants" => Enum.map(@variants, &Atom.to_string/1)
    }
  end

  defp positive_option(options, key, env, default) do
    value = options[key] || env_integer(env, default)

    if value > 0 do
      value
    else
      Mix.raise("#{env} / --#{key} must be positive")
    end
  end

  defp non_negative_option(options, key, env, default) do
    value = options[key] || env_integer(env, default)

    if value >= 0 do
      value
    else
      Mix.raise("#{env} / --#{key} must be non-negative")
    end
  end

  defp env_integer(env, default) do
    case System.get_env(env) do
      nil -> default
      value -> String.to_integer(value)
    end
  rescue
    ArgumentError -> Mix.raise("#{env} must be an integer")
  end

  defp parse_modes(nil), do: [:memory]
  defp parse_modes("both"), do: @modes
  defp parse_modes(value), do: parse_atoms(value, @modes, "mode")

  defp parse_scenarios(nil), do: @scenarios
  defp parse_scenarios("all"), do: @scenarios
  defp parse_scenarios(value), do: parse_atoms(value, @scenarios, "scenario")

  defp parse_atoms(value, allowed, label) do
    atoms =
      value
      |> String.split(",", trim: true)
      |> Enum.map(&String.to_existing_atom/1)

    if atoms != [] and Enum.all?(atoms, &(&1 in allowed)) do
      atoms
    else
      Mix.raise(
        "unknown #{label} #{inspect(value)}; allowed: " <>
          Enum.join(Enum.map(allowed, &Atom.to_string/1), ", ")
      )
    end
  rescue
    ArgumentError -> Mix.raise("unknown #{label} #{inspect(value)}")
  end

  defp run_case(mode, scenario, config) do
    variants = open_variants!(mode)

    try do
      fixture = seed_variants!(variants, config)
      index_id = setup_index!(variants, scenario)
      variants = prepare_variants(variants, scenario)
      measured = measure_case(variants, scenario, config, fixture, index_id)
      validate_measured_state!(variants, scenario, config)

      %{
        "storage_mode" => Atom.to_string(mode),
        "scenario" => Atom.to_string(scenario),
        "operation_count" => operation_count(scenario, config, fixture),
        "dataset_size" => config["dataset_size"],
        "warmup" => config["warmup"],
        "iterations" => config["iterations"],
        "fixture" => fixture_metadata(fixture),
        "variants" => measured["variants"],
        "overhead_vs_pure_exqlite" => measured["overhead_vs_pure_exqlite"],
        "sample_order" => measured["sample_order"]
      }
    after
      Enum.each(variants, &close_variant/1)
    end
  end

  defp open_variants!(mode) do
    Enum.reduce(@variants, [], fn kind, opened ->
      try do
        [open_variant!(kind, mode) | opened]
      rescue
        exception ->
          Enum.each(opened, &close_variant/1)
          reraise exception, __STACKTRACE__
      end
    end)
    |> Enum.reverse()
  end

  defp open_variant!(kind, mode) do
    path =
      case mode do
        :memory -> ":memory:"
        :disk -> Path.join(System.tmp_dir!(), "elixirdb-exqlite-overhead-#{unique_suffix()}.db")
      end

    options = %{
      storage_mode: mode,
      database_uuid: deterministic_uuid("database", Atom.to_string(mode))
    }

    case Adapter.create(path, options) do
      {:ok, adapter} ->
        %__MODULE__{kind: kind, mode: mode, adapter: adapter, conn: adapter.conn, path: path}

      {:error, error} ->
        Mix.raise("could not create #{kind} benchmark database: #{inspect(error)}")
    end
  end

  defp close_variant(%__MODULE__{
         kind: :pure_exqlite,
         conn: conn,
         adapter: adapter,
         path: path,
         statements: statements
       }) do
    Raw.release_all(conn, Map.values(statements))
    _ = Adapter.close(adapter)
    cleanup_path(path)
  end

  defp close_variant(%__MODULE__{adapter: adapter, path: path}) do
    _ = Adapter.close(adapter)
    cleanup_path(path)
  end

  defp seed_variants!(variants, config) do
    documents = Enum.map(0..(config["dataset_size"] - 1), &fixture_document/1)

    Enum.each(variants, fn variant ->
      seed_database!(variant.conn, documents)
      validate_fixture!(variant.conn, config["dataset_size"])
    end)

    %{
      documents: documents,
      category_match_count: Enum.count(documents, &(&1.body["category"] == "task"))
    }
  end

  defp seed_database!(conn, documents) do
    statements =
      Raw.prepare_many(conn, [
        {:begin, @begin_sql},
        {:rollback, @rollback_sql},
        {:commit, @commit_sql},
        {:document_insert, @document_insert_sql},
        {:revision_insert, @revision_insert_sql},
        {:change_insert, @change_insert_sql},
        {:sequence_update, @sequence_update_sql},
        {:local_record_upsert, @local_record_upsert_sql}
      ])

    try do
      Raw.run!(conn, statements.begin)

      Enum.each(documents, fn document ->
        Raw.run!(conn, statements.document_insert, [
          document.id,
          document.revision_id,
          document.body_json,
          TermBlob.bind(document.body_term),
          0,
          document.sequence
        ])

        {:ok, doc_key} = Sqlite3.last_insert_rowid(conn)

        Raw.run!(conn, statements.revision_insert, [
          doc_key,
          document.revision_id,
          1,
          nil,
          document.history_id,
          document.digest,
          0,
          document.body_json,
          TermBlob.bind(document.body_term),
          0,
          1
        ])

        Raw.run!(conn, statements.change_insert, [
          document.sequence,
          doc_key,
          document.id,
          document.revision_id,
          0,
          document.leaf_json,
          TermBlob.bind(document.leaf_term),
          "local"
        ])
      end)

      Raw.run!(conn, statements.sequence_update, [length(documents)])

      Raw.run!(conn, statements.local_record_upsert, [
        @pending_namespace,
        @pending_key,
        @pending_json
      ])

      Raw.run!(conn, statements.commit)
    rescue
      exception ->
        _ = Raw.run(conn, Map.get(statements, :rollback, nil), [])
        reraise exception, __STACKTRACE__
    after
      Raw.release_all(conn, Map.values(statements))
    end
  end

  defp validate_fixture!(conn, dataset_size) do
    documents = Raw.one_off_query!(conn, "SELECT count(*) FROM documents")
    revisions = Raw.one_off_query!(conn, "SELECT count(*) FROM revisions")
    changes = Raw.one_off_query!(conn, "SELECT count(*) FROM changes")
    sequence = Raw.one_off_query!(conn, "SELECT current_sequence FROM db_meta WHERE id = 1")

    expected = [[dataset_size]]

    if documents != expected or revisions != expected or changes != expected or sequence != expected do
      Mix.raise(
        "benchmark fixture mismatch: #{inspect(%{documents: documents, revisions: revisions, changes: changes, sequence: sequence})}"
      )
    end
  end

  defp validate_measured_state!(variants, scenario, config) do
    measured_batches =
      case scenario do
        :bulk_write -> config["warmup"] + config["iterations"]
        _ -> 0
      end

    expected_documents = config["dataset_size"] + measured_batches * config["batch_size"]

    Enum.each(variants, fn variant ->
      [[document_count]] = Raw.one_off_query!(variant.conn, "SELECT count(*) FROM documents")
      [[revision_count]] = Raw.one_off_query!(variant.conn, "SELECT count(*) FROM revisions")
      [[change_count]] = Raw.one_off_query!(variant.conn, "SELECT count(*) FROM changes")

      [[sequence]] =
        Raw.one_off_query!(variant.conn, "SELECT current_sequence FROM db_meta WHERE id = 1")

      expected_sequence = expected_documents

      if {document_count, revision_count, change_count, sequence} !=
           {expected_documents, expected_documents, expected_documents, expected_sequence} do
        Mix.raise(
          "benchmark measured-state mismatch for #{variant.kind}: " <>
            inspect(%{
              documents: document_count,
              revisions: revision_count,
              changes: change_count,
              sequence: sequence,
              expected: expected_documents
            })
        )
      end
    end)
  end

  defp setup_index!(variants, :indexed_query) do
    definition = %{
      "name" => "by-category",
      "type" => "structured",
      "fields" => [%{"path" => "/category", "type" => "string", "direction" => "asc"}]
    }

    index_ids =
      Enum.map(variants, fn variant ->
        case Adapter.create_index(variant.adapter, definition) do
          {:ok, result} -> value(result, :index_id)
          {:error, error} -> Mix.raise("could not create benchmark index: #{inspect(error)}")
        end
      end)

    if Enum.uniq(index_ids) |> length() != 1 do
      Mix.raise("benchmark variants created different index IDs: #{inspect(index_ids)}")
    end

    Enum.each(variants, &validate_index_fixture!/1)
    List.first(index_ids)
  end

  defp setup_index!(_variants, _scenario), do: nil

  defp validate_index_fixture!(variant) do
    rows = Raw.one_off_query!(variant.conn, "SELECT name FROM sqlite_master WHERE type = 'index'")

    unless Enum.any?(rows, fn [name] -> is_binary(name) and String.starts_with?(name, "exdb_s_") end) do
      Mix.raise("benchmark structured index is missing for #{variant.kind}")
    end

    plan =
      Raw.one_off_query!(variant.conn, "EXPLAIN QUERY PLAN " <> @indexed_query_sql, [
        "task",
        "task",
        @query_limit + 1
      ])

    details = Enum.map(plan, &List.last/1) |> Enum.map(&to_string/1)

    unless Enum.any?(details, &String.contains?(&1, "exdb_s_")) do
      Mix.raise("benchmark indexed query is not using the structured index: #{inspect(plan)}")
    end
  end

  defp prepare_variants(variants, scenario) do
    Enum.map(variants, fn
      %__MODULE__{kind: :pure_exqlite} = variant ->
        %{variant | statements: Raw.prepare_many(variant.conn, raw_statements(scenario))}

      variant ->
        variant
    end)
  end

  defp raw_statements(:point_read) do
    [
      {:document_select, @document_select_sql},
      {:revision_select, @revision_select_sql},
      {:attachment_select, @attachment_select_sql}
    ]
  end

  defp raw_statements(:bulk_write) do
    [
      {:begin, @begin_sql},
      {:rollback, @rollback_sql},
      {:commit, @commit_sql},
      {:document_insert, @document_insert_sql},
      {:revision_insert, @revision_insert_sql},
      {:change_insert, @change_insert_sql},
      {:sequence_update, @sequence_update_sql},
      {:local_record_upsert, @local_record_upsert_sql}
    ]
  end

  defp raw_statements(:changes_read) do
    [{:changes_select, @changes_select_sql}, {:changes_exists, @changes_exists_sql}]
  end

  defp raw_statements(:indexed_query), do: [{:indexed_query, @indexed_query_sql}]

  defp measure_case(variants, scenario, config, fixture, index_id) do
    Enum.each(sequence(config["warmup"]), fn number ->
      token = {:warmup, number, number - 1}

      variants
      |> ordered_variants(number - 1)
      |> Enum.each(&invoke!(&1, scenario, token, config, fixture, index_id))
    end)

    samples =
      Enum.map(sequence(config["iterations"]), fn number ->
        absolute = config["warmup"] + number - 1
        token = {:sample, number, absolute}

        durations =
          ordered_variants(variants, absolute)
          |> Enum.map(fn variant ->
            :erlang.garbage_collect()

            {elapsed_us, _result} =
              :timer.tc(fn -> invoke!(variant, scenario, token, config, fixture, index_id) end)

            {variant.kind, elapsed_us}
          end)

        Map.new(durations)
      end)

    variants_report =
      Map.new(variants, fn variant ->
        key = Atom.to_string(variant.kind)
        values = Enum.map(samples, &Map.fetch!(&1, variant.kind))
        {key, summarize_samples(values, operation_count(scenario, config, fixture))}
      end)

    pure = Map.fetch!(variants_report, "pure_exqlite")

    overhead =
      variants_report
      |> Map.delete("pure_exqlite")
      |> Map.new(fn {variant, summary} ->
        {variant, overhead_summary(pure, summary)}
      end)

    %{
      "variants" => variants_report,
      "overhead_vs_pure_exqlite" => overhead,
      "sample_order" =>
        Enum.map(sequence(config["iterations"]), &sample_order(&1, config["warmup"]))
    }
  end

  defp ordered_variants(variants, absolute) do
    if rem(absolute, 2) == 0, do: variants, else: Enum.reverse(variants)
  end

  defp sample_order(number, warmup) do
    absolute = warmup + number - 1
    order = if rem(absolute, 2) == 0, do: @variants, else: Enum.reverse(@variants)
    Enum.map(order, &Atom.to_string/1)
  end

  defp invoke!(variant, :point_read, token, config, _fixture, _index_id) do
    ids = Enum.map(0..(config["dataset_size"] - 1), &document_id/1)
    read_count = config["read_count"]
    start = rem(token_number(token) * read_count, config["dataset_size"])

    Enum.each(0..(read_count - 1), fn offset ->
      id = Enum.at(ids, rem(start + offset, config["dataset_size"]))
      invoke_point_read!(variant, id)
    end)

    :ok
  end

  defp invoke!(variant, :bulk_write, token, config, _fixture, _index_id) do
    batch = batch_documents(config, token_number(token))
    invoke_bulk_write!(variant, batch)
  end

  defp invoke!(variant, :changes_read, _token, config, _fixture, _index_id) do
    limit = min(config["batch_size"], config["dataset_size"])
    invoke_changes_read!(variant, limit, config["dataset_size"])
  end

  defp invoke!(variant, :indexed_query, _token, config, fixture, _index_id) do
    invoke_indexed_query!(variant, config["query_limit"], fixture.category_match_count)
  end

  defp invoke_point_read!(%__MODULE__{kind: :pure_exqlite, conn: conn, statements: statements}, id) do
    [[doc_key, ^id, revision_id, _body_json, 0, _sequence]] =
      Raw.run!(conn, statements.document_select, [id])

    [[^revision_id, 1, nil, _history_id, _digest, 0, _revision_body, _revision_term, 0]] =
      Raw.run!(conn, statements.revision_select, [doc_key, revision_id])

    [] = Raw.run!(conn, statements.attachment_select, [doc_key, revision_id])
    :ok
  end

  defp invoke_point_read!(%__MODULE__{kind: :elixir_db_connection, conn: conn}, id) do
    [[doc_key, ^id, revision_id, _body_json, 0, _sequence]] =
      connection_query!(conn, @document_select_sql, [id])

    [[^revision_id, 1, nil, _history_id, _digest, 0, _revision_body, _revision_term, 0]] =
      connection_query!(conn, @revision_select_sql, [doc_key, revision_id])

    [] = connection_query!(conn, @attachment_select_sql, [doc_key, revision_id])
    :ok
  end

  defp invoke_point_read!(%__MODULE__{kind: :elixir_db_adapter, adapter: adapter}, id) do
    case Adapter.get_document(adapter, %{document_id: id}) do
      {:ok, %{id: ^id, deleted: false, body: body}} when is_map(body) -> :ok
      other -> Mix.raise("point-read adapter result was invalid: #{inspect(other)}")
    end
  end

  defp invoke_bulk_write!(
         %__MODULE__{kind: :pure_exqlite, conn: conn, statements: statements},
         batch
       ) do
    raw_bulk_write!(conn, statements, batch)
  end

  defp invoke_bulk_write!(%__MODULE__{kind: :elixir_db_connection, conn: conn}, batch) do
    connection_bulk_write!(conn, batch)
  end

  defp invoke_bulk_write!(%__MODULE__{kind: :elixir_db_adapter, adapter: adapter}, batch) do
    operations =
      Enum.map(batch, fn document ->
        %{
          operation: :put,
          document_id: document.id,
          history_id: document.history_id,
          body: document.body
        }
      end)

    case Adapter.apply_bulk_mutation(adapter, %{operations: operations}) do
      {:ok, results} when is_list(results) and length(results) == length(batch) -> :ok
      other -> Mix.raise("bulk-write adapter result was invalid: #{inspect(other)}")
    end
  end

  defp raw_bulk_write!(conn, statements, batch) do
    Raw.run!(conn, statements.begin)

    try do
      Enum.each(batch, fn document ->
        Raw.run!(conn, statements.document_insert, [
          document.id,
          document.revision_id,
          document.body_json,
          TermBlob.bind(document.body_term),
          0,
          document.sequence
        ])

        {:ok, doc_key} = Sqlite3.last_insert_rowid(conn)

        Raw.run!(conn, statements.revision_insert, [
          doc_key,
          document.revision_id,
          1,
          nil,
          document.history_id,
          document.digest,
          0,
          document.body_json,
          TermBlob.bind(document.body_term),
          0,
          1
        ])

        Raw.run!(conn, statements.change_insert, [
          document.sequence,
          doc_key,
          document.id,
          document.revision_id,
          0,
          document.leaf_json,
          TermBlob.bind(document.leaf_term),
          "local"
        ])
      end)

      last_sequence = List.last(batch).sequence
      Raw.run!(conn, statements.sequence_update, [last_sequence])

      Raw.run!(conn, statements.local_record_upsert, [
        @pending_namespace,
        @pending_key,
        @pending_json
      ])

      Raw.run!(conn, statements.commit)
      :ok
    rescue
      exception ->
        _ = Raw.run(conn, statements.rollback, [])
        reraise exception, __STACKTRACE__
    end
  end

  defp connection_bulk_write!(conn, batch) do
    connection_execute!(conn, @begin_sql)

    try do
      Enum.each(batch, fn document ->
        connection_execute!(conn, @document_insert_sql, [
          document.id,
          document.revision_id,
          document.body_json,
          TermBlob.bind(document.body_term),
          0,
          document.sequence
        ])

        {:ok, doc_key} = Sqlite3.last_insert_rowid(conn)

        connection_execute!(conn, @revision_insert_sql, [
          doc_key,
          document.revision_id,
          1,
          nil,
          document.history_id,
          document.digest,
          0,
          document.body_json,
          TermBlob.bind(document.body_term),
          0,
          1
        ])

        connection_execute!(conn, @change_insert_sql, [
          document.sequence,
          doc_key,
          document.id,
          document.revision_id,
          0,
          document.leaf_json,
          TermBlob.bind(document.leaf_term),
          "local"
        ])
      end)

      last_sequence = List.last(batch).sequence
      connection_execute!(conn, @sequence_update_sql, [last_sequence])

      connection_execute!(conn, @local_record_upsert_sql, [
        @pending_namespace,
        @pending_key,
        @pending_json
      ])

      connection_execute!(conn, @commit_sql)
      :ok
    rescue
      exception ->
        _ = Connection.execute(conn, @rollback_sql)
        reraise exception, __STACKTRACE__
    end
  end

  defp invoke_changes_read!(
         %__MODULE__{kind: :pure_exqlite, conn: conn, statements: statements},
         limit,
         dataset_size
       ) do
    rows = Raw.run!(conn, statements.changes_select, [0, limit])
    [[has_more]] = Raw.run!(conn, statements.changes_exists, [List.last(rows, [0]) |> List.first()])

    expected_has_more = if limit < dataset_size, do: 1, else: 0

    if length(rows) == limit and has_more == expected_has_more,
      do: :ok,
      else: Mix.raise("changes baseline result was invalid")
  end

  defp invoke_changes_read!(
         %__MODULE__{kind: :elixir_db_connection, conn: conn},
         limit,
         dataset_size
       ) do
    rows = connection_query!(conn, @changes_select_sql, [0, limit])

    [[has_more]] =
      connection_query!(conn, @changes_exists_sql, [List.last(rows, [0]) |> List.first()])

    expected_has_more = if limit < dataset_size, do: 1, else: 0

    if length(rows) == limit and has_more == expected_has_more,
      do: :ok,
      else: Mix.raise("changes connection result was invalid")
  end

  defp invoke_changes_read!(
         %__MODULE__{kind: :elixir_db_adapter, adapter: adapter},
         limit,
         dataset_size
       ) do
    expected_has_more = limit < dataset_size

    case Adapter.read_changes(adapter, %{since: 0, limit: limit}) do
      {:ok, %{results: results, has_more: ^expected_has_more}} when length(results) == limit ->
        :ok

      other ->
        Mix.raise("changes adapter result was invalid: #{inspect(other)}")
    end
  end

  defp invoke_indexed_query!(
         %__MODULE__{kind: :pure_exqlite, conn: conn, statements: statements},
         limit,
         expected_count
       ) do
    rows = Raw.run!(conn, statements.indexed_query, ["task", "task", limit + 1])

    if length(rows) == min(limit + 1, expected_count) and indexed_count(rows) == expected_count,
      do: :ok,
      else: Mix.raise("indexed-query baseline result was invalid")
  end

  defp invoke_indexed_query!(
         %__MODULE__{kind: :elixir_db_connection, conn: conn},
         limit,
         expected_count
       ) do
    rows = connection_query!(conn, @indexed_query_sql, ["task", "task", limit + 1])

    if length(rows) == min(limit + 1, expected_count) and indexed_count(rows) == expected_count,
      do: :ok,
      else: Mix.raise("indexed-query connection result was invalid")
  end

  defp invoke_indexed_query!(
         %__MODULE__{kind: :elixir_db_adapter, adapter: adapter},
         limit,
         expected_count
       ) do
    request = %{selector: %{"/category" => "task"}, index: "by-category", limit: limit}

    case Adapter.execute_query(adapter, request) do
      {:ok, result} ->
        results = value(result, :results) || value(result, :documents) || []

        if length(results) == min(limit, expected_count),
          do: :ok,
          else: Mix.raise("indexed-query adapter result was invalid")

      other ->
        Mix.raise("indexed-query adapter result was invalid: #{inspect(other)}")
    end
  end

  defp indexed_count([]), do: 0
  defp indexed_count(rows), do: rows |> List.last() |> List.last()

  defp connection_query!(conn, sql, params) do
    case Connection.query(conn, sql, params) do
      {:ok, rows} -> rows
      {:error, reason} -> Mix.raise("ElixirDB connection query failed: #{inspect(reason)}")
    end
  end

  defp connection_execute!(conn, sql, params \\ []) do
    case Connection.execute(conn, sql, params) do
      :ok -> :ok
      {:error, reason} -> Mix.raise("ElixirDB connection execute failed: #{inspect(reason)}")
    end
  end

  defp summarize_samples(samples, operation_count) do
    values = Enum.sort(samples)
    count = length(values)
    median_us = percentile(values, 0.50)
    p95_us = percentile(values, 0.95)
    p99_us = percentile(values, 0.99)
    mean_us = Enum.sum(values) / count
    mad_us = median_absolute_deviation(values, median_us)

    %{
      "sample_us" => values,
      "count" => count,
      "min_us" => List.first(values),
      "max_us" => List.last(values),
      "mean_us" => Float.round(mean_us, 2),
      "median_us" => median_us,
      "p95_us" => p95_us,
      "p99_us" => p99_us,
      "median_absolute_deviation_us" => mad_us,
      "coefficient_of_variation_pct" => coefficient_of_variation(values, mean_us),
      "median_us_per_operation" => Float.round(median_us / operation_count, 2),
      "median_operations_per_second" => Float.round(operation_count * 1_000_000 / median_us, 2)
    }
  end

  defp overhead_summary(reference, candidate) do
    deltas =
      Enum.zip(reference["sample_us"], candidate["sample_us"])
      |> Enum.map(fn {baseline, measured} -> measured - baseline end)

    %{
      "median_delta_us" => candidate["median_us"] - reference["median_us"],
      "median_overhead_pct" => percentage_delta(reference["median_us"], candidate["median_us"]),
      "p95_delta_us" => candidate["p95_us"] - reference["p95_us"],
      "p95_overhead_pct" => percentage_delta(reference["p95_us"], candidate["p95_us"]),
      "paired_delta_us" => deltas,
      "paired_median_delta_us" => percentile(Enum.sort(deltas), 0.50),
      "paired_mean_delta_us" => Float.round(Enum.sum(deltas) / length(deltas), 2),
      "paired_overhead_pct" =>
        percentage_delta(
          reference["median_us"],
          percentile(Enum.sort(deltas), 0.50) + reference["median_us"]
        )
    }
  end

  defp percentage_delta(reference, measured) when reference > 0,
    do: Float.round((measured / reference - 1) * 100, 2)

  defp percentage_delta(_reference, _measured), do: 0.0

  defp median_absolute_deviation(values, median) do
    values
    |> Enum.map(&abs(&1 - median))
    |> Enum.sort()
    |> percentile(0.50)
  end

  defp coefficient_of_variation(_values, mean) when mean == 0, do: 0.0

  defp coefficient_of_variation(values, mean) do
    variance = Enum.reduce(values, 0.0, &(&2 + :math.pow(&1 - mean, 2))) / length(values)
    Float.round(:math.sqrt(variance) / mean * 100, 2)
  end

  defp percentile(values, fraction) do
    index = max(1, ceil(length(values) * fraction)) - 1
    Enum.at(values, index)
  end

  defp operation_count(:point_read, config, _fixture), do: config["read_count"]
  defp operation_count(:bulk_write, config, _fixture), do: config["batch_size"]

  defp operation_count(:changes_read, config, _fixture),
    do: min(config["batch_size"], config["dataset_size"])

  defp operation_count(:indexed_query, _config, fixture), do: fixture.category_match_count

  defp fixture_metadata(fixture) do
    %{
      "documents" => length(fixture.documents),
      "category_task_documents" => fixture.category_match_count,
      "seed_sequence" => length(fixture.documents),
      "body_shape" => "category, priority, title, tags",
      "attachments" => "none"
    }
  end

  defp fixture_document(index) do
    id = document_id(index)
    history_id = deterministic_uuid("seed-history", Integer.to_string(index))
    body = benchmark_body(index)
    revision_id = revision_id!(id, history_id, body)
    body_json = Canonical.encode!(body)
    leaf_json = leaf_json(revision_id, history_id)

    %{
      id: id,
      history_id: history_id,
      revision_id: revision_id,
      digest: digest(revision_id),
      body: body,
      body_json: body_json,
      body_term: term_blob!(body, body_json),
      leaf_json: leaf_json,
      leaf_term: term_blob!(leaf_value(revision_id, history_id), leaf_json),
      sequence: index + 1
    }
  end

  defp batch_documents(config, batch_index) do
    Enum.map(0..(config["batch_size"] - 1), fn offset ->
      value = config["dataset_size"] + batch_index * config["batch_size"] + offset

      id =
        "bench-#{String.pad_leading(Integer.to_string(batch_index), 6, "0")}-#{String.pad_leading(Integer.to_string(offset), 4, "0")}"

      history_id = deterministic_uuid("bench-history", id)
      body = benchmark_body(value)
      revision_id = revision_id!(id, history_id, body)
      body_json = Canonical.encode!(body)
      leaf_json = leaf_json(revision_id, history_id)

      %{
        id: id,
        history_id: history_id,
        revision_id: revision_id,
        digest: digest(revision_id),
        body: body,
        body_json: body_json,
        body_term: term_blob!(body, body_json),
        leaf_json: leaf_json,
        leaf_term: term_blob!(leaf_value(revision_id, history_id), leaf_json),
        sequence: config["dataset_size"] + batch_index * config["batch_size"] + offset + 1
      }
    end)
  end

  defp benchmark_body(value) do
    %{
      "category" => if(rem(value, 4) == 0, do: "task", else: "note"),
      "priority" => rem(value, 100),
      "title" => "Benchmark document #{value}",
      "tags" => ["benchmark", "v1"]
    }
  end

  defp document_id(index),
    do: "seed-" <> String.pad_leading(Integer.to_string(index), 6, "0")

  defp leaf_value(revision_id, history_id),
    do: [%{"revision" => revision_id, "history_id" => history_id, "deleted" => false}]

  defp leaf_json(revision_id, history_id),
    do: Canonical.encode!(leaf_value(revision_id, history_id))

  defp term_blob!(value, json) do
    case TermBlob.encode(value, json) do
      {:ok, blob} -> blob
      {:error, error} -> Mix.raise("could not encode benchmark term BLOB: #{inspect(error)}")
    end
  end

  defp revision_id!(document_id, history_id, body) do
    case Id.calculate(document_id, history_id, nil, false, body, %{}) do
      {:ok, revision_id} -> revision_id
      {:error, error} -> Mix.raise("could not calculate benchmark revision: #{inspect(error)}")
    end
  end

  defp digest(revision_id), do: revision_id |> String.split("-", parts: 2) |> List.last()

  defp token_number({_phase, _number, absolute}), do: absolute

  defp deterministic_uuid(namespace, value) do
    hex = :crypto.hash(:sha256, namespace <> ":" <> value) |> Base.encode16(case: :lower)

    <<first::binary-size(8), second::binary-size(4), third::binary-size(4), fourth::binary-size(4),
      last::binary-size(12), _rest::binary>> = hex

    "#{first}-#{second}-4#{binary_part(third, 1, 3)}-8#{binary_part(fourth, 1, 3)}-#{last}"
  end

  defp unique_suffix,
    do: "#{System.system_time(:microsecond)}-#{System.unique_integer([:positive])}"

  defp value(map, key), do: Map.get(map, key, Map.get(map, Atom.to_string(key)))

  defp write_report(report, "-") do
    IO.puts(JSON.encode_to_iodata!(report))
  end

  defp write_report(report, path) do
    File.mkdir_p!(Path.dirname(path))
    json = JSON.encode_to_iodata!(report) |> IO.iodata_to_binary()
    File.write!(path, json <> "\n")
  end

  defp print_summary(report, output) do
    IO.puts("ElixirDB overhead benchmark report: #{output}")

    Enum.each(report["results"], fn result ->
      adapter_variant = result["variants"]["elixir_db_adapter"]
      connection_variant = result["variants"]["elixir_db_connection"]
      adapter = result["overhead_vs_pure_exqlite"]["elixir_db_adapter"]
      connection = result["overhead_vs_pure_exqlite"]["elixir_db_connection"]

      IO.puts(
        "  #{result["storage_mode"]}/#{result["scenario"]}: " <>
          "adapter median #{adapter_variant["median_us"]} us " <>
          "(overhead +#{adapter["median_delta_us"]} us / #{adapter["median_overhead_pct"]}%), " <>
          "connection median #{connection_variant["median_us"]} us " <>
          "(overhead #{connection["median_delta_us"]} us / #{connection["median_overhead_pct"]}%)"
      )
    end)
  end

  defp runtime_metadata do
    %{
      "elixir" => System.version(),
      "otp" => :erlang.system_info(:otp_release) |> to_string(),
      "sqlite" => ElixirDB.Diagnostics.runtime() |> Map.get(:sqlite),
      "schedulers_online" => :erlang.system_info(:schedulers_online),
      "os" => :os.type() |> inspect()
    }
  end

  defp git_revision do
    case System.cmd("git", ["rev-parse", "HEAD"], stderr_to_stdout: true) do
      {revision, 0} -> String.trim(revision)
      _ -> "unknown"
    end
  end

  defp default_output_path do
    timestamp = DateTime.utc_now() |> Calendar.strftime("%Y%m%dT%H%M%SZ")
    Path.join("output/benchmarks", "exqlite-overhead-#{timestamp}.json")
  end

  defp cleanup_path(":memory:"), do: :ok

  defp cleanup_path(path) do
    for suffix <- ["", "-journal", "-wal", "-shm"] do
      _ = File.rm(path <> suffix)
    end

    :ok
  end

  defp sequence(0), do: []
  defp sequence(count), do: 1..count

  defp usage do
    """
    Usage:
      MIX_ENV=prod mix run bench/sqlite_exqlite_overhead_benchmark.exs -- [options]

    Options:
      --mode memory|disk|both       SQLite mode (default: memory)
      --scenario NAME|all           Comma-separated scenario list (default: all)
      --iterations N                Measured paired samples (default: 30)
      --warmup N                    Paired warmup samples (default: 10)
      --dataset N                   Seeded documents (default: 500)
      --batch N                     Bulk-write/changes batch size (default: 50, max: 500)
      --reads N                     Point reads per sample (default: 100)
      --output PATH                 JSON report path (default: output/benchmarks/...json)

    Environment equivalents:
      ELIXIRDB_OVERHEAD_ITERATIONS, ELIXIRDB_OVERHEAD_WARMUP,
      ELIXIRDB_OVERHEAD_DATASET, ELIXIRDB_OVERHEAD_BATCH,
      ELIXIRDB_OVERHEAD_READS
    """
  end

  defmodule Raw do
    @moduledoc false

    alias Exqlite.Sqlite3

    def prepare_many(conn, definitions) do
      Map.new(definitions, fn {name, sql} -> {name, prepare!(conn, sql)} end)
    end

    def prepare!(conn, sql) do
      case Sqlite3.prepare(conn, String.trim(sql)) do
        {:ok, statement} -> statement
        {:error, reason} -> Mix.raise("could not prepare benchmark SQL: #{inspect(reason)}")
      end
    end

    def run!(conn, statement, params \\ []) do
      case run(conn, statement, params) do
        {:ok, rows} -> rows
        {:error, reason} -> Mix.raise("pure ExQLite benchmark SQL failed: #{inspect(reason)}")
      end
    end

    def run(_conn, nil, _params), do: {:error, :missing_statement}

    def run(conn, statement, params) do
      with :ok <- Sqlite3.bind(statement, params) do
        step(conn, statement, [])
      end
    end

    def one_off_query!(conn, sql, params \\ []) do
      statement = prepare!(conn, sql)

      try do
        run!(conn, statement, params)
      after
        _ = Sqlite3.release(conn, statement)
      end
    end

    def release_all(conn, statements) do
      Enum.each(statements, fn statement ->
        _ = Sqlite3.release(conn, statement)
      end)

      :ok
    end

    defp step(conn, statement, rows) do
      case Sqlite3.step(conn, statement) do
        {:row, row} -> step(conn, statement, [row | rows])
        :done -> {:ok, Enum.reverse(rows)}
        :busy -> {:error, :busy}
        {:error, reason} -> {:error, reason}
        other -> {:error, other}
      end
    end
  end
end

ElixirDB.Benchmarks.ExqliteOverhead.main(System.argv())
