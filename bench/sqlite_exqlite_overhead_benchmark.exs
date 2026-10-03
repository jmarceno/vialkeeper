Code.require_file("overhead/stats.exs", __DIR__)
Code.require_file("overhead/sampler.exs", __DIR__)
Code.require_file("overhead/environment.exs", __DIR__)

defmodule VialKeeper.Benchmarks.ExqliteOverhead do
  @moduledoc """
  Paired low-noise benchmark of VialKeeper SQLite work against direct ExQLite.

  Every case opens one database per variant, seeds each with the same
  deterministic fixture through the SQLite adapter, and measures:

    * `pure_exqlite` — hand-written prepared statements through `Exqlite.Sqlite3`.
    * `vial_keeper_connection` — the same SQL through the VialKeeper connection
      wrapper and its statement cache.
    * `vial_keeper_adapter` — the public SQLite adapter operation.

  Measurement rules (see `VialKeeper.Benchmarks.Overhead.Sampler`):

    * per-sample inputs (document IDs, write batches, revision hashes, term
      blobs) are built before the timer starts, so harness work is never
      charged to any variant;
    * samples are paired by index and the variant order rotates;
    * timing is nanosecond monotonic time; raw samples are reported in
      collection order so paired statistics can be recomputed from the JSON;
    * no garbage collection is forced before a sample;
    * collection stops when the paired-ratio confidence intervals are narrow
      enough, or at the iteration/time cap.

  The direct write control is a physical SQLite baseline: it writes the same
  final document, revision, change-feed, metadata, and replication-state rows
  in one prepared transaction. It intentionally does not reproduce VialKeeper's
  validation, revision lookup, conflict handling, JSON hashing, or retention
  orchestration.
  """

  alias Exqlite.Sqlite3
  alias VialKeeper.Benchmarks.ExqliteOverhead.Raw
  alias VialKeeper.Benchmarks.Overhead.{Environment, Sampler, Stats}
  alias VialKeeper.JSON.Canonical
  alias VialKeeper.Revisions.Id
  alias VialKeeper.Storage.SQLite.{Adapter, Connection, TermBlob}

  @scenarios [:point_read, :bulk_write, :changes_read, :indexed_query]
  @modes [:memory, :disk]
  @variants [:pure_exqlite, :vial_keeper_connection, :vial_keeper_adapter]
  @reference :pure_exqlite

  @default_warmup 10
  @default_min_iterations 30
  @default_max_iterations 300
  @default_target_ci_pct 1.0
  @default_budget_ms 30_000
  @default_dataset_size 500
  @default_batch_size 50
  @default_read_count 100
  @default_repeat 20
  @default_work_dir "tmp/bench/vialkeeper/work/overhead"
  @seed_chunk 500
  @query_limit 50

  @winner_select_sql """
  SELECT d.document_id, d.winning_revision, d.winning_body_json, d.winning_body_term,
         d.winning_deleted, d.update_sequence,
         a.attachment_name, a.blob_digest, a.logical_size, a.content_type
  FROM documents AS d
  LEFT JOIN revision_attachments AS a
    ON a.doc_key = d.doc_key AND a.revision_id = d.winning_revision
  WHERE d.document_id = ?
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
    config = benchmark_config(options)

    with_isolated_runtime(config, fn ->
      started_at = DateTime.utc_now() |> DateTime.to_iso8601()
      modes = parse_modes(options[:mode])
      scenarios = parse_scenarios(options[:scenario])

      results =
        for mode <- modes, scenario <- scenarios do
          run_case(mode, scenario, config)
        end

      report = %{
        "schema_version" => 2,
        "benchmark" => "vial_keeper_overhead_vs_exqlite",
        "started_at" => started_at,
        "environment" => Environment.metadata(sqlite_metadata()),
        "configuration" => config,
        "results" => results
      }

      output = options[:output] || default_output_path()
      write_report(report, output)
      print_summary(report, output)
    end)
  end

  defp with_isolated_runtime(config, fun) do
    run_dir = Path.join(config["work_dir"], "run-#{unique_suffix()}")
    root = Path.join(run_dir, "runtime")
    previous_root = Application.get_env(:vial_keeper, :database_root)
    previous_listener = Application.get_env(:vial_keeper, :listener)
    ensure_application_stopped!()
    File.mkdir_p!(root)
    Application.put_env(:vial_keeper, :database_root, root)
    Application.put_env(:vial_keeper, :listener, ip: {127, 0, 0, 1}, port: 0)
    Process.put({__MODULE__, :run_dir}, run_dir)

    try do
      {:ok, _started} = Application.ensure_all_started(:vial_keeper)
      fun.()
    after
      _ = Application.stop(:vial_keeper)
      restore_application_env(:database_root, previous_root)
      restore_application_env(:listener, previous_listener)
      _ = File.rm_rf(run_dir)
    end
  end

  defp restore_application_env(key, nil), do: Application.delete_env(:vial_keeper, key)
  defp restore_application_env(key, value), do: Application.put_env(:vial_keeper, key, value)

  defp ensure_application_stopped! do
    if Enum.any?(Application.started_applications(), &match?({:vial_keeper, _, _}, &1)) do
      Mix.raise("benchmark must be launched with mix run --no-start")
    end
  end

  defp parse_options(argv) do
    argv = if List.first(argv) == "--", do: tl(argv), else: argv

    {options, positional, invalid} =
      OptionParser.parse(argv,
        strict: [
          mode: :string,
          scenario: :string,
          iterations: :integer,
          min_iterations: :integer,
          max_iterations: :integer,
          target_ci_pct: :float,
          budget_ms: :integer,
          warmup: :integer,
          dataset: :integer,
          batch: :integer,
          reads: :integer,
          repeat: :integer,
          work_dir: :string,
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
    fixed = options[:iterations] || env_integer("VIALKEEPER_OVERHEAD_ITERATIONS", nil)

    {min_iterations, max_iterations} =
      case fixed do
        nil ->
          {positive(options, :min_iterations, @default_min_iterations),
           positive(options, :max_iterations, @default_max_iterations)}

        count when count > 0 ->
          {count, count}

        _ ->
          Mix.raise("--iterations must be positive")
      end

    if min_iterations > max_iterations do
      Mix.raise("--min-iterations must not exceed --max-iterations")
    end

    batch_size =
      positive(options, :batch, env_integer("VIALKEEPER_OVERHEAD_BATCH", @default_batch_size))

    if batch_size > 500 do
      Mix.raise("--batch must be at most the configured host bulk limit (500)")
    end

    target_ci_pct = options[:target_ci_pct] || @default_target_ci_pct

    if target_ci_pct <= 0 do
      Mix.raise("--target-ci-pct must be positive")
    end

    %{
      "warmup" =>
        non_negative(options, :warmup, env_integer("VIALKEEPER_OVERHEAD_WARMUP", @default_warmup)),
      "min_iterations" => min_iterations,
      "max_iterations" => max_iterations,
      "target_ci_pct" => target_ci_pct,
      "budget_ms" => positive(options, :budget_ms, @default_budget_ms),
      "dataset_size" =>
        positive(
          options,
          :dataset,
          env_integer("VIALKEEPER_OVERHEAD_DATASET", @default_dataset_size)
        ),
      "batch_size" => batch_size,
      "read_count" =>
        positive(options, :reads, env_integer("VIALKEEPER_OVERHEAD_READS", @default_read_count)),
      "repeat" => positive(options, :repeat, @default_repeat),
      "query_limit" => @query_limit,
      "work_dir" => Path.expand(options[:work_dir] || @default_work_dir),
      "timer" => "monotonic_ns",
      "gc_before_sample" => false,
      "pair_order" => "rotating",
      "reference_variant" => Atom.to_string(@reference),
      "timed_variants" => Enum.map(@variants, &Atom.to_string/1)
    }
  end

  defp positive(options, key, default) do
    case options[key] || default do
      value when is_integer(value) and value > 0 -> value
      _ -> Mix.raise("--#{key |> Atom.to_string() |> String.replace("_", "-")} must be positive")
    end
  end

  defp non_negative(options, key, default) do
    case options[key] || default do
      value when is_integer(value) and value >= 0 -> value
      _ -> Mix.raise("--#{key} must be non-negative")
    end
  end

  defp env_integer(env, default) do
    case System.get_env(env) do
      nil ->
        default

      value ->
        case Integer.parse(value) do
          {integer, ""} -> integer
          _ -> Mix.raise("#{env} must be an integer")
        end
    end
  end

  defp parse_modes(nil), do: [:memory]
  defp parse_modes("both"), do: @modes
  defp parse_modes(value), do: parse_atoms(value, @modes, "mode")

  defp parse_scenarios(nil), do: @scenarios
  defp parse_scenarios("all"), do: @scenarios
  defp parse_scenarios(value), do: parse_atoms(value, @scenarios, "scenario")

  defp parse_atoms(value, allowed, label) do
    by_name = Map.new(allowed, &{Atom.to_string(&1), &1})
    names = String.split(value, ",", trim: true)

    case Enum.map(names, &Map.fetch(by_name, &1)) do
      [_ | _] = found ->
        if Enum.all?(found, &match?({:ok, _}, &1)) do
          Enum.map(found, fn {:ok, atom} -> atom end)
        else
          unknown_option!(label, value, allowed)
        end

      [] ->
        unknown_option!(label, value, allowed)
    end
  end

  defp unknown_option!(label, value, allowed) do
    Mix.raise(
      "unknown #{label} #{inspect(value)}; allowed: " <>
        Enum.join(Enum.map(allowed, &Atom.to_string/1), ", ")
    )
  end

  defp run_case(mode, scenario, config) do
    variants = open_variants!(mode)

    try do
      fixture = seed_variants!(variants, config)
      setup_index!(variants, scenario)
      variants = prepare_variants(variants, scenario)
      by_kind = Map.new(variants, &{&1.kind, &1})
      operations = operation_count(scenario, config)

      collected =
        Sampler.run(
          variants: @variants,
          reference: @reference,
          prepare: &prepare_input(scenario, &1, config),
          invoke: fn kind, input ->
            invoke!(Map.fetch!(by_kind, kind), scenario, input, config, fixture)
          end,
          warmup: config["warmup"],
          min_iterations: config["min_iterations"],
          max_iterations: config["max_iterations"],
          target_ci_pct: config["target_ci_pct"],
          budget_ms: config["budget_ms"]
        )

      validate_measured_state!(variants, scenario, config, length(collected.samples))
      case_report(mode, scenario, config, fixture, operations, collected)
    after
      Enum.each(variants, &close_variant/1)
    end
  end

  defp case_report(mode, scenario, config, fixture, operations, collected) do
    samples = collected.samples
    series = fn kind, key -> Enum.map(samples, &get_in(&1, [kind, key])) end
    reference_ns = series.(@reference, :ns)

    variants =
      Map.new(@variants, fn kind ->
        ns = series.(kind, :ns)

        summary =
          ns
          |> Stats.summarize(operations)
          |> Map.put("samples_ns", ns)
          |> Map.put(
            "median_reductions_per_operation",
            per_op(series.(kind, :reductions), operations)
          )
          |> Map.put("gcs_per_sample_median", Stats.round_float(Stats.median(series.(kind, :gcs))))

        {Atom.to_string(kind), summary}
      end)

    comparisons =
      @variants
      |> Enum.reject(&(&1 == @reference))
      |> Map.new(fn kind ->
        {Atom.to_string(kind), Stats.compare(reference_ns, series.(kind, :ns))}
      end)

    %{
      "storage_mode" => Atom.to_string(mode),
      "scenario" => Atom.to_string(scenario),
      "operations_per_sample" => operations,
      "dataset_size" => config["dataset_size"],
      "warmup" => config["warmup"],
      "iterations" => length(samples),
      "stop_reason" => collected.stop_reason,
      "elapsed_ms" => collected.elapsed_ms,
      "fixture" => fixture_metadata(fixture),
      "variants" => variants,
      "vs_reference" => comparisons,
      "sample_order" =>
        Enum.map(samples, fn sample -> Enum.map(sample.__order__, &Atom.to_string/1) end)
    }
  end

  defp per_op(values, operations), do: Stats.round_float(Stats.median(values) / operations)

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
        :disk -> Path.join(Process.get({__MODULE__, :run_dir}), "#{kind}-#{unique_suffix()}.db")
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

  # Every variant is seeded through the product write path with deterministic
  # history IDs, so all databases hold byte-identical rows that the adapter
  # itself would have produced.
  defp seed_variants!(variants, config) do
    documents = Enum.map(0..(config["dataset_size"] - 1), &fixture_document/1)

    Enum.each(variants, fn variant ->
      documents
      |> Enum.chunk_every(@seed_chunk)
      |> Enum.each(fn chunk ->
        case Adapter.apply_bulk_mutation(variant.adapter, %{
               operations: Enum.map(chunk, &put_operation/1)
             }) do
          {:ok, results} when length(results) == length(chunk) -> :ok
          other -> Mix.raise("benchmark seed failed for #{variant.kind}: #{inspect(other)}")
        end
      end)

      validate_fixture!(variant.conn, documents)
    end)

    %{
      documents: documents,
      category_match_count: Enum.count(documents, &(&1.body["category"] == "task"))
    }
  end

  defp put_operation(document),
    do: %{
      operation: :put,
      document_id: document.id,
      history_id: document.history_id,
      body: document.body
    }

  defp validate_fixture!(conn, documents) do
    dataset_size = length(documents)
    counts = table_counts(conn)
    expected = {dataset_size, dataset_size, dataset_size, dataset_size}

    if counts != expected do
      Mix.raise("benchmark fixture mismatch: #{inspect(counts)} expected #{inspect(expected)}")
    end

    # The raw write control computes revision IDs itself; they must match what
    # the adapter stored for the same document, history and body.
    sample = List.first(documents)

    [[revision]] =
      Raw.one_off_query!(conn, "SELECT winning_revision FROM documents WHERE document_id = ?", [
        sample.id
      ])

    if revision != sample.revision_id do
      Mix.raise("benchmark revision mismatch: adapter #{revision}, harness #{sample.revision_id}")
    end
  end

  defp table_counts(conn) do
    [[documents]] = Raw.one_off_query!(conn, "SELECT count(*) FROM documents")
    [[revisions]] = Raw.one_off_query!(conn, "SELECT count(*) FROM revisions")
    [[changes]] = Raw.one_off_query!(conn, "SELECT count(*) FROM changes")
    [[sequence]] = Raw.one_off_query!(conn, "SELECT current_sequence FROM db_meta WHERE id = 1")
    {documents, revisions, changes, sequence}
  end

  defp validate_measured_state!(variants, scenario, config, samples) do
    batches = if scenario == :bulk_write, do: config["warmup"] + samples, else: 0
    expected = config["dataset_size"] + batches * config["batch_size"]

    Enum.each(variants, fn variant ->
      counts = table_counts(variant.conn)

      if counts != {expected, expected, expected, expected} do
        Mix.raise(
          "benchmark measured-state mismatch for #{variant.kind}: #{inspect(counts)} expected #{expected}"
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
  end

  defp setup_index!(_variants, _scenario), do: :ok

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

  defp raw_statements(:point_read), do: [{:winner_select, @winner_select_sql}]

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

  defp raw_statements(:changes_read),
    do: [{:changes_select, @changes_select_sql}, {:changes_exists, @changes_exists_sql}]

  defp raw_statements(:indexed_query), do: [{:indexed_query, @indexed_query_sql}]

  # Inputs are built here, outside every timed region.
  defp prepare_input(:point_read, {_phase, absolute}, config) do
    dataset_size = config["dataset_size"]
    read_count = config["read_count"]
    start = rem(absolute * read_count, dataset_size)
    Enum.map(0..(read_count - 1), &document_id(rem(start + &1, dataset_size)))
  end

  defp prepare_input(:bulk_write, {_phase, absolute}, config) do
    batch = batch_documents(config, absolute)
    %{batch: batch, operations: Enum.map(batch, &put_operation/1)}
  end

  defp prepare_input(_scenario, _token, _config), do: nil

  defp invoke!(variant, :point_read, ids, _config, _fixture) do
    Enum.each(ids, &invoke_point_read!(variant, &1))
  end

  defp invoke!(variant, :bulk_write, input, _config, _fixture),
    do: invoke_bulk_write!(variant, input)

  defp invoke!(variant, :changes_read, _input, config, _fixture) do
    limit = min(config["batch_size"], config["dataset_size"])
    repeat(config["repeat"], fn -> invoke_changes_read!(variant, limit, config["dataset_size"]) end)
  end

  defp invoke!(variant, :indexed_query, _input, config, fixture) do
    repeat(config["repeat"], fn ->
      invoke_indexed_query!(variant, config["query_limit"], fixture.category_match_count)
    end)
  end

  defp repeat(0, _fun), do: :ok

  defp repeat(count, fun) do
    :ok = fun.()
    repeat(count - 1, fun)
  end

  defp invoke_point_read!(%__MODULE__{kind: :pure_exqlite, conn: conn, statements: statements}, id) do
    [[^id, _revision_id, _body_json, _body_term, 0, _sequence, nil, _digest, _size, _type]] =
      Raw.run!(conn, statements.winner_select, [id])

    :ok
  end

  defp invoke_point_read!(%__MODULE__{kind: :vial_keeper_connection, conn: conn}, id) do
    [[^id, _revision_id, _body_json, _body_term, 0, _sequence, nil, _digest, _size, _type]] =
      connection_query!(conn, @winner_select_sql, [id])

    :ok
  end

  defp invoke_point_read!(%__MODULE__{kind: :vial_keeper_adapter, adapter: adapter}, id) do
    case Adapter.get_document(adapter, %{document_id: id}) do
      {:ok, %{id: ^id, deleted: false, body: body}} when is_map(body) -> :ok
      other -> Mix.raise("point-read adapter result was invalid: #{inspect(other)}")
    end
  end

  defp invoke_bulk_write!(%__MODULE__{kind: :pure_exqlite, conn: conn, statements: statements}, %{
         batch: batch
       }) do
    run = fn name, params -> Raw.run!(conn, Map.fetch!(statements, name), params) end
    physical_bulk_write!(conn, batch, run, fn -> Raw.run(conn, statements.rollback, []) end)
  end

  defp invoke_bulk_write!(%__MODULE__{kind: :vial_keeper_connection, conn: conn}, %{batch: batch}) do
    run = fn name, params -> connection_execute!(conn, connection_sql(name), params) end
    physical_bulk_write!(conn, batch, run, fn -> Connection.execute(conn, @rollback_sql) end)
  end

  defp invoke_bulk_write!(%__MODULE__{kind: :vial_keeper_adapter, adapter: adapter}, %{
         operations: operations
       }) do
    case Adapter.apply_bulk_mutation(adapter, %{operations: operations}) do
      {:ok, results} when length(results) == length(operations) -> :ok
      other -> Mix.raise("bulk-write adapter result was invalid: #{inspect(other)}")
    end
  end

  defp connection_sql(:begin), do: @begin_sql
  defp connection_sql(:commit), do: @commit_sql
  defp connection_sql(:document_insert), do: @document_insert_sql
  defp connection_sql(:revision_insert), do: @revision_insert_sql
  defp connection_sql(:change_insert), do: @change_insert_sql
  defp connection_sql(:sequence_update), do: @sequence_update_sql
  defp connection_sql(:local_record_upsert), do: @local_record_upsert_sql

  # One transaction writing the final document, revision, change, sequence and
  # replication-state rows. `run` executes a named statement for the variant.
  defp physical_bulk_write!(conn, batch, run, rollback) do
    run.(:begin, [])

    try do
      Enum.each(batch, fn document ->
        run.(:document_insert, [
          document.id,
          document.revision_id,
          document.body_json,
          TermBlob.bind(document.body_term),
          0,
          document.sequence
        ])

        {:ok, doc_key} = Sqlite3.last_insert_rowid(conn)

        run.(:revision_insert, [
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

        run.(:change_insert, [
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

      run.(:sequence_update, [List.last(batch).sequence])
      run.(:local_record_upsert, [@pending_namespace, @pending_key, @pending_json])
      run.(:commit, [])
      :ok
    rescue
      exception ->
        _ = rollback.()
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
    check_changes!(rows, has_more, limit, dataset_size)
  end

  defp invoke_changes_read!(
         %__MODULE__{kind: :vial_keeper_connection, conn: conn},
         limit,
         dataset_size
       ) do
    rows = connection_query!(conn, @changes_select_sql, [0, limit])

    [[has_more]] =
      connection_query!(conn, @changes_exists_sql, [List.last(rows, [0]) |> List.first()])

    check_changes!(rows, has_more, limit, dataset_size)
  end

  defp invoke_changes_read!(
         %__MODULE__{kind: :vial_keeper_adapter, adapter: adapter},
         limit,
         dataset_size
       ) do
    expected_has_more = limit < dataset_size

    case Adapter.read_changes(adapter, %{since: 0, limit: limit}) do
      {:ok, %{results: results, has_more: ^expected_has_more}} when length(results) == limit -> :ok
      other -> Mix.raise("changes adapter result was invalid: #{inspect(other)}")
    end
  end

  defp check_changes!(rows, has_more, limit, dataset_size) do
    expected_has_more = if limit < dataset_size, do: 1, else: 0

    if length(rows) == limit and has_more == expected_has_more,
      do: :ok,
      else: Mix.raise("changes control result was invalid")
  end

  defp invoke_indexed_query!(
         %__MODULE__{kind: :pure_exqlite, conn: conn, statements: statements},
         limit,
         expected
       ) do
    rows = Raw.run!(conn, statements.indexed_query, ["task", "task", limit + 1])
    check_indexed_rows!(rows, limit, expected)
  end

  defp invoke_indexed_query!(
         %__MODULE__{kind: :vial_keeper_connection, conn: conn},
         limit,
         expected
       ) do
    rows = connection_query!(conn, @indexed_query_sql, ["task", "task", limit + 1])
    check_indexed_rows!(rows, limit, expected)
  end

  defp invoke_indexed_query!(
         %__MODULE__{kind: :vial_keeper_adapter, adapter: adapter},
         limit,
         expected
       ) do
    request = %{selector: %{"/category" => "task"}, index: "by-category", limit: limit}

    case Adapter.execute_query(adapter, request) do
      {:ok, result} ->
        results = value(result, :results) || value(result, :documents) || []

        if length(results) == min(limit, expected),
          do: :ok,
          else: Mix.raise("indexed-query adapter result was invalid")

      other ->
        Mix.raise("indexed-query adapter result was invalid: #{inspect(other)}")
    end
  end

  defp check_indexed_rows!(rows, limit, expected) do
    count = if rows == [], do: 0, else: rows |> List.last() |> List.last()

    if length(rows) == min(limit + 1, expected) and count == expected,
      do: :ok,
      else: Mix.raise("indexed-query control result was invalid")
  end

  defp connection_query!(conn, sql, params) do
    case Connection.query(conn, sql, params) do
      {:ok, rows} -> rows
      {:error, reason} -> Mix.raise("VialKeeper connection query failed: #{inspect(reason)}")
    end
  end

  defp connection_execute!(conn, sql, params) do
    case Connection.execute(conn, sql, params) do
      :ok -> :ok
      {:error, reason} -> Mix.raise("VialKeeper connection execute failed: #{inspect(reason)}")
    end
  end

  defp operation_count(:point_read, config), do: config["read_count"]
  defp operation_count(:bulk_write, config), do: config["batch_size"]
  defp operation_count(:changes_read, config), do: config["repeat"]
  defp operation_count(:indexed_query, config), do: config["repeat"]

  defp fixture_metadata(fixture) do
    %{
      "documents" => length(fixture.documents),
      "category_task_documents" => fixture.category_match_count,
      "seed_sequence" => length(fixture.documents),
      "seeded_through" => "Adapter.apply_bulk_mutation",
      "body_shape" => "category, priority, title, tags",
      "attachments" => "none"
    }
  end

  defp fixture_document(index) do
    build_document(
      document_id(index),
      deterministic_uuid("seed-history", Integer.to_string(index)),
      index,
      index + 1
    )
  end

  defp batch_documents(config, batch_index) do
    Enum.map(0..(config["batch_size"] - 1), fn offset ->
      value = config["dataset_size"] + batch_index * config["batch_size"] + offset

      id =
        "bench-#{String.pad_leading(Integer.to_string(batch_index), 6, "0")}-#{String.pad_leading(Integer.to_string(offset), 4, "0")}"

      build_document(id, deterministic_uuid("bench-history", id), value, value + 1)
    end)
  end

  defp build_document(id, history_id, value, sequence) do
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
      sequence: sequence
    }
  end

  defp benchmark_body(value) do
    %{
      "category" => if(rem(value, 4) == 0, do: "task", else: "note"),
      "priority" => rem(value, 100),
      "title" => "Benchmark document #{value}",
      "tags" => ["benchmark", "v1"]
    }
  end

  defp document_id(index), do: "seed-" <> String.pad_leading(Integer.to_string(index), 6, "0")

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

  defp deterministic_uuid(namespace, value) do
    hex = :crypto.hash(:sha256, namespace <> ":" <> value) |> Base.encode16(case: :lower)

    <<first::binary-size(8), second::binary-size(4), third::binary-size(4), fourth::binary-size(4),
      last::binary-size(12), _rest::binary>> = hex

    "#{first}-#{second}-4#{binary_part(third, 1, 3)}-8#{binary_part(fourth, 1, 3)}-#{last}"
  end

  defp unique_suffix,
    do: "#{System.system_time(:microsecond)}-#{System.unique_integer([:positive])}"

  defp value(map, key), do: Map.get(map, key, Map.get(map, Atom.to_string(key)))

  defp sqlite_metadata do
    {:ok, conn} = Sqlite3.open(":memory:")

    try do
      [[version, source_id]] =
        Raw.one_off_query!(conn, "SELECT sqlite_version(), sqlite_source_id()")

      options = conn |> Raw.one_off_query!("PRAGMA compile_options") |> Enum.map(&List.first/1)
      %{"version" => version, "source_id" => source_id, "compile_options" => options}
    after
      Sqlite3.close(conn)
    end
  end

  defp write_report(report, "-"), do: IO.puts(JSON.encode_to_iodata!(report))

  defp write_report(report, path) do
    File.mkdir_p!(Path.dirname(path))
    File.write!(path, [JSON.encode_to_iodata!(report), "\n"])
  end

  defp print_summary(report, output) do
    IO.puts("VialKeeper overhead benchmark report: #{output}")

    Enum.each(report["results"], fn result ->
      IO.puts(
        "  #{result["storage_mode"]}/#{result["scenario"]} " <>
          "(#{result["iterations"]} samples, #{result["stop_reason"]}, " <>
          "#{result["operations_per_sample"]} ops/sample)"
      )

      reference = result["variants"][Atom.to_string(@reference)]

      IO.puts(
        "    #{pad(Atom.to_string(@reference))} #{format_us(reference["median_ns_per_operation"])}/op"
      )

      Enum.each(result["vs_reference"], fn {variant, comparison} ->
        summary = result["variants"][variant]
        [low, high] = comparison["paired_ratio_ci95"]

        IO.puts(
          "    #{pad(variant)} #{format_us(summary["median_ns_per_operation"])}/op  " <>
            "x#{comparison["paired_ratio_median"]} [#{low}, #{high}] vs #{@reference}"
        )
      end)
    end)
  end

  defp pad(name), do: String.pad_trailing(name, 24)

  defp format_us(ns), do: "#{:erlang.float_to_binary(ns / 1000, decimals: 2)} us"

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

  defp usage do
    """
    Usage:
      MIX_ENV=prod mix run --no-start bench/sqlite_exqlite_overhead_benchmark.exs -- [options]
      scripts/bench_overhead.sh [options]     # same, with pinned CPUs and quiet scheduler flags

    Options:
      --mode memory|disk|both       SQLite mode (default: memory)
      --scenario NAME|all           Comma-separated scenario list (default: all)
      --warmup N                    Untimed warmup samples (default: #{@default_warmup})
      --min-iterations N            Samples before the stop rule applies (default: #{@default_min_iterations})
      --max-iterations N            Sample cap (default: #{@default_max_iterations})
      --target-ci-pct F             Stop when every paired-ratio 95% CI half-width
                                    is at most F percent (default: #{@default_target_ci_pct})
      --budget-ms N                 Wall-clock cap per case (default: #{@default_budget_ms})
      --iterations N                Fixed sample count (sets min = max = N)
      --dataset N                   Seeded documents (default: #{@default_dataset_size})
      --batch N                     Bulk-write/changes batch size (default: #{@default_batch_size}, max: 500)
      --reads N                     Point reads per sample (default: #{@default_read_count})
      --repeat N                    Changes/query operations per sample (default: #{@default_repeat})
      --work-dir PATH               Disk-mode databases and runtime root
                                    (default: #{@default_work_dir})
      --output PATH                 JSON report path (default: output/benchmarks/...json)

    Environment equivalents:
      VIALKEEPER_OVERHEAD_ITERATIONS, VIALKEEPER_OVERHEAD_WARMUP,
      VIALKEEPER_OVERHEAD_DATASET, VIALKEEPER_OVERHEAD_BATCH,
      VIALKEEPER_OVERHEAD_READS
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

VialKeeper.Benchmarks.ExqliteOverhead.main(System.argv())
