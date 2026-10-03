Code.require_file("overhead/stats.exs", __DIR__)
Code.require_file("overhead/sampler.exs", __DIR__)
Code.require_file("overhead/environment.exs", __DIR__)
Code.require_file("overhead/native.exs", __DIR__)
Code.require_file("overhead/capture.exs", __DIR__)

defmodule VialKeeper.Benchmarks.ExqliteOverhead do
  @moduledoc """
  Layer-by-layer latency ladder from native SQLite up to the HTTP router.

  Every variant runs the same scenario on its own database, seeded with the
  same deterministic fixture through the SQLite adapter. The ladder, from the
  floor up:

    * `native_replay` (L0) — a C program built from ExQLite's own `sqlite3.c`
      replays the exact statements the adapter executed (see `Capture`).
    * `exqlite_replay` (L1) — the same statements through `Exqlite.Sqlite3`.
    * `connection_replay` (L2) — the same statements through VialKeeper's
      `Connection` wrapper and statement cache.
    * `vial_keeper_adapter` (L3) — the public SQLite adapter operation.
    * `vial_keeper_service` (L4, disk mode) — `Documents`, `Changes`, and
      `Query` through the database catalog, admission, owner, and read pool.
    * `vial_keeper_http` (L5, disk mode) — the `/v1` Plug router in process
      (request decoding, routing, response encoding; no socket).

  `exqlite_minimal` is a side reference: hand-written minimal SQL through
  ExQLite. Comparing it with `exqlite_replay` shows what the SQL the adapter
  chooses to issue costs, separately from the cost of issuing it.

  Measurement rules (see `VialKeeper.Benchmarks.Overhead.Sampler`):

    * per-sample inputs (document IDs, write batches, revision hashes, term
      blobs, captured statements, encoded native requests, HTTP requests) are
      built before the timer starts;
    * samples are paired by index and the variant order rotates;
    * timing is nanosecond monotonic time; the native control times itself
      around its SQLite calls only;
    * no garbage collection is forced before a sample;
    * collection stops when the paired-ratio confidence intervals are narrow
      enough, or at the iteration/time cap;
    * replays must return exactly the rows the captured run returned, and every
      database must end in the expected state.
  """

  alias Exqlite.Sqlite3
  alias VialKeeper.Benchmarks.ExqliteOverhead.Raw
  alias VialKeeper.Benchmarks.Overhead.{Capture, Environment, Native, Sampler, Stats}
  alias VialKeeper.JSON.Canonical
  alias VialKeeper.Revisions.Id
  alias VialKeeper.Runtime.DatabaseCatalog
  alias VialKeeper.Storage.SQLite.{Adapter, Connection, TermBlob}

  @scenarios [:point_read, :bulk_write, :changes_read, :indexed_query]
  @modes [:memory, :disk]
  @ladder [
    :native_replay,
    :exqlite_replay,
    :connection_replay,
    :vial_keeper_adapter,
    :vial_keeper_service,
    :vial_keeper_http
  ]
  @references [:exqlite_minimal]
  @all_variants @ladder ++ @references
  @disk_only [:vial_keeper_service, :vial_keeper_http]
  @replay_variants [:native_replay, :exqlite_replay, :connection_replay]
  @adapter_level [
    :native_replay,
    :exqlite_replay,
    :connection_replay,
    :vial_keeper_adapter,
    :exqlite_minimal
  ]

  @step_descriptions %{
    {:native_replay, :exqlite_replay} =>
      "ExQLite NIF boundary: dirty-scheduler hops, parameter binding calls, row term construction",
    {:exqlite_replay, :connection_replay} => "VialKeeper Connection wrapper and statement cache",
    {:connection_replay, :vial_keeper_adapter} =>
      "Adapter work between the same statements: validation, revision logic, encoding and decoding",
    {:vial_keeper_adapter, :vial_keeper_service} =>
      "Service validation, catalog routing, admission, owner and read-pool process hops",
    {:vial_keeper_service, :vial_keeper_http} =>
      "Plug router, JSON request decoding and response encoding (no socket)"
  }

  # Pragmas copied from the adapter connection to the native control and read
  # back to prove both run with the same settings.
  @aligned_pragmas ~w(journal_mode synchronous foreign_keys locking_mode trusted_schema
                      cache_size temp_store wal_autocheckpoint mmap_size automatic_index)

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
  @index_definition %{
    "name" => "by-category",
    "type" => "structured",
    "fields" => [%{"path" => "/category", "type" => "string", "direction" => "asc"}]
  }

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

  defstruct [:kind, :mode, :adapter, :conn, :path, :port, :uuid, statements: %{}]

  @doc false
  @spec main([binary()]) :: :ok
  def main(argv) do
    options = parse_options(argv)
    config = benchmark_config(options)
    modes = parse_modes(options[:mode])
    scenarios = parse_scenarios(options[:scenario])
    requested = parse_variants(options[:variants])
    native = if :native_replay in requested, do: Native.build!()

    with_isolated_runtime(config, fn ->
      started_at = DateTime.utc_now() |> DateTime.to_iso8601()

      results =
        for mode <- modes, scenario <- scenarios do
          run_case(mode, scenario, config, applicable_variants(requested, mode), native)
        end

      report = %{
        "schema_version" => 3,
        "benchmark" => "vial_keeper_layer_ladder",
        "started_at" => started_at,
        "environment" =>
          sqlite_metadata() |> Environment.metadata() |> Map.put("native_control", native),
        "configuration" => Map.put(config, "variants", Enum.map(requested, &Atom.to_string/1)),
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
          variants: :string,
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
      "pair_order" => "rotating"
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

  defp parse_variants(nil), do: @all_variants
  defp parse_variants("all"), do: @all_variants

  defp parse_variants(value) do
    selected = parse_atoms(value, @all_variants, "variant")
    Enum.filter(@all_variants, &(&1 in selected))
  end

  defp applicable_variants(requested, :memory), do: requested -- @disk_only
  defp applicable_variants(requested, :disk), do: requested

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

  ## Case lifecycle

  defp run_case(mode, scenario, config, selected, native) do
    if selected == [], do: Mix.raise("no selected variant applies to #{mode} mode")
    documents = Enum.map(0..(config["dataset_size"] - 1), &fixture_document/1)
    reset_statement_registries()
    state = %{mode: mode, scenario: scenario, config: config, documents: documents}
    variants = open_variants!(selected, mode)
    Process.put({__MODULE__, :open_variants}, variants)
    worker = if Enum.any?(selected, &(&1 in @replay_variants)), do: start_capture(mode, documents)

    try do
      state = Map.merge(state, %{variants: variants, worker: worker, selected: selected})
      verification = setup_case!(state)

      {variants, native_verification} = finalize_native(state.variants, native, mode)
      variants = prepare_minimal(variants, scenario)
      verification = Map.put(verification, "native_engine", native_verification)
      state = %{state | variants: variants}

      state =
        Map.merge(state, %{
          fixture: fixture(documents),
          router_opts: VialKeeper.HTTP.Router.init([])
        })

      reference = Enum.find(@ladder, List.first(selected), &(&1 in selected))

      collected =
        Sampler.run(
          variants: selected,
          reference: reference,
          prepare: &prepare_input(state, &1),
          invoke: &invoke!(state, &1, &2),
          warmup: config["warmup"],
          min_iterations: config["min_iterations"],
          max_iterations: config["max_iterations"],
          target_ci_pct: config["target_ci_pct"],
          budget_ms: config["budget_ms"]
        )

      validate_measured_state!(state, length(collected.samples))
      case_report(state, reference, verification, collected)
    after
      variants = Process.delete({__MODULE__, :open_variants}) || variants
      # Statements must be finalized before their connection closes.
      release_statement_registries(variants)
      Enum.each(Map.values(variants), &close_variant/1)
      if worker, do: stop_capture(worker)
    end
  end

  defp setup_case!(state) do
    adapter_variants = adapter_variants(state)

    Enum.each(adapter_variants, &seed_adapter!(&1.adapter, state.documents))
    Enum.each(service_variants(state), &seed_service!(&1.uuid, state.documents))

    %{"index_plan" => if(state.scenario == :indexed_query, do: setup_indexes!(state))}
  end

  defp adapter_variants(state),
    do: state.variants |> Map.values() |> Enum.filter(&(&1.kind in @adapter_level))

  defp service_variants(state),
    do: state.variants |> Map.values() |> Enum.filter(&(&1.kind in @disk_only))

  defp open_variants!(selected, mode) do
    Enum.reduce(selected, %{}, fn kind, opened ->
      try do
        Map.put(opened, kind, open_variant!(kind, mode))
      rescue
        exception ->
          Enum.each(Map.values(opened), &close_variant/1)
          reraise exception, __STACKTRACE__
      end
    end)
  end

  defp open_variant!(kind, :disk) when kind in @disk_only do
    relative = "overhead-#{kind}-#{System.unique_integer([:positive])}.vialkeeper"

    with {:ok, identity} <- DatabaseCatalog.create(relative),
         uuid = identity.database_uuid,
         {:ok, _} <- DatabaseCatalog.open(uuid),
         :ok <- VialKeeper.View.Manager.await_resumed(uuid) do
      %__MODULE__{kind: kind, mode: :disk, uuid: uuid}
    else
      other -> Mix.raise("could not create #{kind} catalog database: #{inspect(other)}")
    end
  end

  defp open_variant!(kind, mode) do
    adapter = create_adapter!(kind, mode)
    %__MODULE__{kind: kind, mode: mode, adapter: adapter, conn: adapter.conn, path: adapter.path}
  end

  defp create_adapter!(label, mode, run_dir \\ Process.get({__MODULE__, :run_dir})) do
    path =
      case mode do
        :memory -> ":memory:"
        :disk -> Path.join(run_dir, "#{label}-#{unique_suffix()}.db")
      end

    options = %{
      storage_mode: mode,
      database_uuid: deterministic_uuid("database", Atom.to_string(mode))
    }

    case Adapter.create(path, options) do
      {:ok, adapter} ->
        adapter

      {:error, error} ->
        Mix.raise("could not create #{label} benchmark database: #{inspect(error)}")
    end
  end

  defp close_variant(%__MODULE__{kind: kind, uuid: uuid}) when kind in @disk_only do
    _ = DatabaseCatalog.close(uuid)
    _ = DatabaseCatalog.unregister(uuid)
    :ok
  end

  defp close_variant(%__MODULE__{port: port, path: path}) when is_port(port) do
    Native.stop(port)
    cleanup_path(path)
  end

  defp close_variant(%__MODULE__{kind: :exqlite_minimal, conn: conn, adapter: adapter} = variant) do
    Raw.release_all(conn, Map.values(variant.statements))
    _ = Adapter.close(adapter)
    cleanup_path(variant.path)
  end

  defp close_variant(%__MODULE__{adapter: nil, path: path}), do: cleanup_path(path)

  defp close_variant(%__MODULE__{adapter: adapter, path: path}) do
    _ = Adapter.close(adapter)
    cleanup_path(path)
  end

  ## Capture worker

  defp start_capture(mode, documents) do
    run_dir = Process.get({__MODULE__, :run_dir})

    Capture.start(fn ->
      adapter = create_adapter!(:capture, mode, run_dir)
      seed_adapter!(adapter, documents)
      adapter
    end)
  end

  defp stop_capture(worker) do
    path = Capture.run(worker, fn adapter -> _ = Adapter.close(adapter) && adapter.path end)
    Capture.stop(worker)
    cleanup_path(path)
  end

  ## Seeding and verification

  # Every adapter-level database (including the capture database) is seeded
  # through the product write path with deterministic history IDs, so they all
  # hold byte-identical rows that the adapter itself would have produced.
  defp seed_adapter!(adapter, documents) do
    documents
    |> Enum.chunk_every(@seed_chunk)
    |> Enum.each(fn chunk ->
      case Adapter.apply_bulk_mutation(adapter, %{operations: Enum.map(chunk, &put_operation/1)}) do
        {:ok, results} when length(results) == length(chunk) -> :ok
        other -> Mix.raise("benchmark seed failed: #{inspect(other)}")
      end
    end)

    validate_fixture!(adapter.conn, documents)
  end

  # Catalog databases accept no client history IDs, so revision IDs differ
  # from the adapter-level fixture; document IDs, bodies, sequence numbers and
  # row counts are the same.
  defp seed_service!(uuid, documents) do
    documents
    |> Enum.chunk_every(@seed_chunk)
    |> Enum.each(fn chunk ->
      case VialKeeper.Documents.bulk_write(uuid, Enum.map(chunk, &service_operation/1)) do
        {:ok, results} when length(results) == length(chunk) -> :ok
        other -> Mix.raise("service seed failed: #{inspect(other)}")
      end
    end)

    assert_service_sequence!(uuid, length(documents))
  end

  defp put_operation(document),
    do: %{
      operation: :put,
      document_id: document.id,
      history_id: document.history_id,
      body: document.body
    }

  defp service_operation(document), do: %{type: :put, id: document.id, body: document.body}

  defp validate_fixture!(conn, documents) do
    dataset_size = length(documents)
    counts = table_counts(conn)
    expected = {dataset_size, dataset_size, dataset_size, dataset_size}

    if counts != expected do
      Mix.raise("benchmark fixture mismatch: #{inspect(counts)} expected #{inspect(expected)}")
    end

    # The minimal write control computes revision IDs itself; they must match
    # what the adapter stored for the same document, history and body.
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

  @count_queries [
    "SELECT count(*) FROM documents",
    "SELECT count(*) FROM revisions",
    "SELECT count(*) FROM changes",
    "SELECT current_sequence FROM db_meta WHERE id = 1"
  ]

  defp native_table_counts(port) do
    @count_queries
    |> Enum.map(fn sql -> port |> Native.scalar(sql) |> List.first() |> String.to_integer() end)
    |> List.to_tuple()
  end

  defp assert_service_sequence!(uuid, expected) do
    case VialKeeper.Changes.read(uuid, %{since: expected - 1, limit: 10}) do
      {:ok, %{results: [%{sequence: ^expected}], has_more: false}} ->
        :ok

      other ->
        Mix.raise("service database is not at sequence #{expected}: #{inspect(other)}")
    end
  end

  defp validate_measured_state!(state, samples) do
    config = state.config
    batches = if state.scenario == :bulk_write, do: config["warmup"] + samples, else: 0
    expected = config["dataset_size"] + batches * config["batch_size"]
    expected_counts = {expected, expected, expected, expected}

    observed =
      Enum.map(Map.values(state.variants), fn
        %__MODULE__{kind: kind, uuid: uuid} when kind in @disk_only ->
          assert_service_sequence!(uuid, expected)
          {kind, expected_counts}

        %__MODULE__{kind: kind, port: port} when is_port(port) ->
          {kind, native_table_counts(port)}

        %__MODULE__{kind: kind, conn: conn} ->
          {kind, table_counts(conn)}
      end)

    observed =
      if state.worker,
        do: [{:capture, Capture.run(state.worker, &table_counts(&1.conn))} | observed],
        else: observed

    Enum.each(observed, fn {kind, counts} ->
      if counts != expected_counts do
        Mix.raise(
          "measured-state mismatch for #{kind}: #{inspect(counts)} expected #{inspect(expected_counts)}"
        )
      end
    end)
  end

  defp setup_indexes!(state) do
    index_ids =
      Enum.map(adapter_variants(state), fn variant -> create_index!(variant.adapter) end)

    index_ids =
      if state.worker,
        do: [Capture.run(state.worker, &create_index!/1) | index_ids],
        else: index_ids

    if Enum.uniq(index_ids) |> length() != 1 do
      Mix.raise("benchmark variants created different index IDs: #{inspect(index_ids)}")
    end

    Enum.each(service_variants(state), fn variant ->
      case VialKeeper.Query.create_index(variant.uuid, @index_definition) do
        {:ok, _} -> verify_service_plan!(variant.uuid)
        other -> Mix.raise("could not create service index: #{inspect(other)}")
      end
    end)

    Enum.each(adapter_variants(state), &verify_index_exists!(&1.conn))

    %{
      "adapter_plan_uses_index" => if(state.worker, do: verify_adapter_plan!(state)),
      "minimal_sql_uses_index" =>
        if(Map.has_key?(state.variants, :exqlite_minimal),
          do: verify_minimal_plan!(state.variants.exqlite_minimal.conn)
        ),
      "service_plan_uses_index" => if(service_variants(state) != [], do: true)
    }
  end

  defp create_index!(adapter) do
    case Adapter.create_index(adapter, @index_definition) do
      {:ok, result} -> value(result, :index_id)
      {:error, error} -> Mix.raise("could not create benchmark index: #{inspect(error)}")
    end
  end

  defp verify_index_exists!(conn) do
    rows = Raw.one_off_query!(conn, "SELECT name FROM sqlite_master WHERE type = 'index'")

    unless Enum.any?(rows, fn [name] -> is_binary(name) and String.starts_with?(name, "exdb_s_") end) do
      Mix.raise("benchmark structured index is missing")
    end
  end

  defp verify_minimal_plan!(conn) do
    plan_uses_index!(conn, @indexed_query_sql, ["task", "task", @query_limit + 1], "minimal SQL")
  end

  # Runs one adapter query in the capture worker and checks that at least one
  # statement it executed is planned through the structured index.
  defp verify_adapter_plan!(state) do
    request = indexed_request(state.config)
    {_, ops} = Capture.capture(state.worker, &Adapter.execute_query(&1, request))
    queries = Enum.filter(ops, &(&1.kind == :query))

    Capture.run(state.worker, fn adapter ->
      if Enum.any?(queries, &plan_uses_index?(adapter.conn, &1.sql, &1.params)),
        do: true,
        else: Mix.raise("adapter indexed query did not use the structured index")
    end)
  end

  defp verify_service_plan!(uuid) do
    case VialKeeper.Query.explain(uuid, %{selector: %{"/category" => "task"}, limit: @query_limit}) do
      {:ok, %{full_scan: false, selected_indexes: [_ | _]}} -> :ok
      other -> Mix.raise("service indexed query did not select the index: #{inspect(other)}")
    end
  end

  defp plan_uses_index!(conn, sql, params, label) do
    if plan_uses_index?(conn, sql, params),
      do: true,
      else: Mix.raise("#{label} indexed query is not using the structured index")
  end

  defp plan_uses_index?(conn, sql, params) do
    conn
    |> Raw.one_off_query!("EXPLAIN QUERY PLAN " <> sql, params)
    |> Enum.any?(fn row -> row |> List.last() |> to_string() |> String.contains?("exdb_s_") end)
  end

  ## Native control

  defp finalize_native(variants, nil, _mode), do: {variants, nil}

  defp finalize_native(%{native_replay: variant} = variants, native, mode) do
    pragmas =
      Enum.flat_map(@aligned_pragmas, fn name ->
        case pragma_value(variant.conn, name) do
          nil -> []
          value -> [{name, value}]
        end
      end)

    engine = sqlite_metadata(variant.conn)
    port = Native.start(native["path"])
    variant = %{variant | port: port}

    try do
      open_native!(port, variant, mode)

      Enum.each(pragmas, fn {name, value} -> Native.exec(port, "PRAGMA #{name} = #{value}") end)

      mismatched =
        Enum.reject(pragmas, fn {name, value} ->
          Native.scalar(port, "PRAGMA #{name}") == [value]
        end)

      if mismatched != [] do
        Mix.raise("native control pragmas differ from the adapter: #{inspect(mismatched)}")
      end

      verify_native_engine!(port, engine)
      variants = Map.put(variants, :native_replay, %{variant | adapter: nil, conn: nil})
      Process.put({__MODULE__, :open_variants}, variants)

      {variants,
       %{
         "sqlite_version" => engine["version"],
         "source_id_and_compile_options_match" => true,
         "aligned_pragmas" => Map.new(pragmas),
         # Compile options are compared without the COMPILER entry: ExQLite may
         # ship a precompiled NIF built by a different compiler version.
         "exqlite_compiler" =>
           Enum.find(engine["compile_options"], &String.starts_with?(&1, "COMPILER=")),
         "native_compiler" => native["compiler"]
       }}
    rescue
      exception ->
        Native.stop(port)
        reraise exception, __STACKTRACE__
    end
  end

  defp finalize_native(variants, _native, _mode), do: {variants, nil}

  defp open_native!(port, variant, :memory) do
    {:ok, image} = Sqlite3.serialize(variant.conn, "main")
    _ = Adapter.close(variant.adapter)
    Native.open_image(port, image)
  end

  defp open_native!(port, variant, :disk) do
    _ = Adapter.close(variant.adapter)
    Native.open_file(port, variant.path)
  end

  defp verify_native_engine!(port, engine) do
    native = %{
      "version" => port |> Native.scalar("SELECT sqlite_version()") |> List.first(),
      "source_id" => port |> Native.scalar("SELECT sqlite_source_id()") |> List.first(),
      "compile_options" => Native.scalar(port, "PRAGMA compile_options")
    }

    comparable = fn metadata ->
      Map.update!(metadata, "compile_options", fn options ->
        Enum.reject(options, &String.starts_with?(&1, "COMPILER="))
      end)
    end

    if comparable.(native) != comparable.(engine) do
      Mix.raise(
        "native control SQLite build differs from ExQLite:\n" <>
          "native: #{inspect(native)}\nexqlite: #{inspect(engine)}"
      )
    end
  end

  defp pragma_value(conn, name) do
    case Connection.pragma(conn, name) do
      {:ok, [[value]]} -> to_string(value)
      # Some pragmas (mmap_size on an in-memory database) have no value.
      {:ok, []} -> nil
      other -> Mix.raise("could not read PRAGMA #{name}: #{inspect(other)}")
    end
  end

  ## Inputs (built outside every timed region)

  defp prepare_input(state, token) do
    base = base_input(state.scenario, token, state.config)
    input = %{base: base}

    input =
      if state.worker,
        do: Map.put(input, :replay, capture_replay(state, base)),
        else: input

    if Map.has_key?(state.variants, :vial_keeper_http) do
      flush_plug_test_messages()
      Map.put(input, :http, http_requests(state, base))
    else
      input
    end
  end

  # Plug's test adapter sends every response to the calling process. Drop
  # them between samples so the mailbox does not grow across the run.
  defp flush_plug_test_messages do
    receive do
      {:plug_conn, :sent} ->
        flush_plug_test_messages()

      {ref, {status, _headers, _body}} when is_reference(ref) and is_integer(status) ->
        flush_plug_test_messages()
    after
      0 -> :ok
    end
  end

  defp base_input(:point_read, {_phase, absolute}, config) do
    dataset_size = config["dataset_size"]
    read_count = config["read_count"]
    start = rem(absolute * read_count, dataset_size)
    Enum.map(0..(read_count - 1), &document_id(rem(start + &1, dataset_size)))
  end

  defp base_input(:bulk_write, {_phase, absolute}, config) do
    batch = batch_documents(config, absolute)

    %{
      batch: batch,
      operations: Enum.map(batch, &put_operation/1),
      service_operations: Enum.map(batch, &service_operation/1)
    }
  end

  defp base_input(_scenario, _token, _config), do: nil

  defp capture_replay(state, base) do
    {:ok, ops} = Capture.capture(state.worker, &adapter_operation!(&1, state.scenario, base, state))
    record_capture_profile(ops)

    replay = %{
      ops: ops,
      expected_rows: ops |> Enum.filter(&(&1.kind == :query)) |> Enum.map(& &1.rows) |> Enum.sum(),
      statements: length(ops)
    }

    replay =
      if Map.has_key?(state.variants, :exqlite_replay),
        do: Map.put(replay, :exqlite, exqlite_ops(state.variants.exqlite_replay.conn, ops)),
        else: replay

    if Map.has_key?(state.variants, :native_replay),
      do: Map.put(replay, :native, native_request(state.variants.native_replay.port, ops)),
      else: replay
  end

  defp exqlite_ops(conn, ops) do
    Enum.map(ops, fn
      %{kind: :exec, sql: sql} ->
        {:exec, sql}

      %{kind: kind, sql: sql, params: params} ->
        {:stmt, exqlite_statement(conn, sql), params, kind == :query}
    end)
  end

  defp native_request(port, ops) do
    ops
    |> Enum.map(fn
      %{kind: :exec, sql: sql} ->
        {:exec, sql}

      %{kind: kind, sql: sql, params: params} ->
        {:stmt, kind == :query, native_statement(port, sql), params}
    end)
    |> Native.encode_run()
    |> IO.iodata_to_binary()
  end

  @exqlite_registry {__MODULE__, :exqlite_statements}
  @native_registry {__MODULE__, :native_statements}

  defp reset_statement_registries do
    Process.put(@exqlite_registry, %{})
    Process.put(@native_registry, %{})
  end

  defp release_statement_registries(variants) do
    case variants do
      %{exqlite_replay: %{conn: conn}} ->
        Raw.release_all(conn, Map.values(Process.get(@exqlite_registry, %{})))

      _ ->
        :ok
    end

    reset_statement_registries()
  end

  defp exqlite_statement(conn, sql) do
    registry = Process.get(@exqlite_registry)

    case Map.fetch(registry, sql) do
      {:ok, statement} ->
        statement

      :error ->
        statement = Raw.prepare!(conn, sql)
        Process.put(@exqlite_registry, Map.put(registry, sql, statement))
        statement
    end
  end

  defp native_statement(port, sql) do
    registry = Process.get(@native_registry)

    case Map.fetch(registry, sql) do
      {:ok, id} ->
        id

      :error ->
        id = map_size(registry)
        :ok = Native.prepare(port, id, sql)
        Process.put(@native_registry, Map.put(registry, sql, id))
        id
    end
  end

  defp http_requests(state, base) do
    prefix = "/v1/databases/#{state.variants.vial_keeper_http.uuid}"
    config = state.config

    case state.scenario do
      :point_read ->
        Enum.map(base, &http_request(prefix <> "/documents/get", %{"id" => &1}))

      :bulk_write ->
        body =
          Enum.map(base.batch, &%{"type" => "put", "id" => &1.id, "body" => &1.body})

        [http_request(prefix <> "/documents/bulk-write", body)]

      :changes_read ->
        body = %{"since" => 0, "limit" => changes_limit(config)}
        List.duplicate(http_request(prefix <> "/changes", body), config["repeat"])

      :indexed_query ->
        body = %{"selector" => %{"/category" => "task"}, "limit" => config["query_limit"]}
        List.duplicate(http_request(prefix <> "/query", body), config["repeat"])
    end
  end

  defp http_request(path, body) do
    Plug.Test.conn(:post, path, JSON.encode!(body))
    |> Plug.Conn.put_req_header("content-type", "application/json")
  end

  ## Timed operations

  defp invoke!(state, :native_replay, %{replay: replay}) do
    result = Native.run(state.variants.native_replay.port, replay.native)
    check_rows!(:native_replay, result.rows, replay.expected_rows)
    {:timed, result.ns, %{vm_steps: result.vm_steps, statements: result.statements}}
  end

  defp invoke!(state, :exqlite_replay, %{replay: replay}) do
    rows = run_exqlite_ops(state.variants.exqlite_replay.conn, replay.exqlite, 0)
    check_rows!(:exqlite_replay, rows, replay.expected_rows)
  end

  defp invoke!(state, :connection_replay, %{replay: replay}) do
    rows = run_connection_ops(state.variants.connection_replay.conn, replay.ops, 0)
    check_rows!(:connection_replay, rows, replay.expected_rows)
  end

  defp invoke!(state, :vial_keeper_adapter, %{base: base}) do
    adapter_operation!(state.variants.vial_keeper_adapter.adapter, state.scenario, base, state)
  end

  defp invoke!(state, :vial_keeper_service, %{base: base}) do
    service_operation!(state.variants.vial_keeper_service.uuid, state.scenario, base, state)
  end

  defp invoke!(state, :vial_keeper_http, %{http: requests}) do
    Enum.each(requests, fn request ->
      case VialKeeper.HTTP.Router.call(request, state.router_opts) do
        %{status: 200} -> :ok
        response -> Mix.raise("HTTP #{response.status}: #{response.resp_body}")
      end
    end)
  end

  defp invoke!(state, :exqlite_minimal, %{base: base}) do
    minimal_operation!(state.variants.exqlite_minimal, state.scenario, base, state)
  end

  defp check_rows!(_variant, rows, rows), do: :ok

  defp check_rows!(variant, rows, expected),
    do: Mix.raise("#{variant} returned #{rows} rows; the captured run returned #{expected}")

  defp run_exqlite_ops(_conn, [], rows), do: rows

  defp run_exqlite_ops(conn, [{:exec, sql} | rest], rows) do
    :ok = Sqlite3.execute(conn, sql)
    run_exqlite_ops(conn, rest, rows)
  end

  defp run_exqlite_ops(conn, [{:stmt, statement, params, count_rows} | rest], rows) do
    :ok = Sqlite3.bind(statement, params)
    stepped = step_count(conn, statement, 0)
    run_exqlite_ops(conn, rest, if(count_rows, do: rows + stepped, else: rows))
  end

  defp step_count(conn, statement, rows) do
    case Sqlite3.step(conn, statement) do
      {:row, _row} -> step_count(conn, statement, rows + 1)
      :done -> rows
      other -> Mix.raise("exqlite replay step failed: #{inspect(other)}")
    end
  end

  defp run_connection_ops(_conn, [], rows), do: rows

  defp run_connection_ops(conn, [%{kind: :exec, sql: sql} | rest], rows) do
    :ok = Connection.exec(conn, sql)
    run_connection_ops(conn, rest, rows)
  end

  defp run_connection_ops(conn, [%{kind: :execute, sql: sql, params: params} | rest], rows) do
    :ok = Connection.execute(conn, sql, params)
    run_connection_ops(conn, rest, rows)
  end

  defp run_connection_ops(conn, [%{kind: :query, sql: sql, params: params} | rest], rows) do
    {:ok, result} = Connection.query(conn, sql, params)
    run_connection_ops(conn, rest, rows + length(result))
  end

  # The adapter operation for a sample; runs both in the capture worker
  # (traced, untimed) and as the measured adapter variant.
  defp adapter_operation!(adapter, :point_read, ids, _state) do
    Enum.each(ids, fn id ->
      case Adapter.get_document(adapter, %{document_id: id}) do
        {:ok, %{id: ^id, deleted: false, body: body}} when is_map(body) -> :ok
        other -> Mix.raise("point-read adapter result was invalid: #{inspect(other)}")
      end
    end)
  end

  defp adapter_operation!(adapter, :bulk_write, %{operations: operations}, _state) do
    case Adapter.apply_bulk_mutation(adapter, %{operations: operations}) do
      {:ok, results} when length(results) == length(operations) -> :ok
      other -> Mix.raise("bulk-write adapter result was invalid: #{inspect(other)}")
    end
  end

  defp adapter_operation!(adapter, :changes_read, _base, state) do
    limit = changes_limit(state.config)
    expected_has_more = limit < state.config["dataset_size"]

    repeat(state.config["repeat"], fn ->
      case Adapter.read_changes(adapter, %{since: 0, limit: limit}) do
        {:ok, %{results: results, has_more: ^expected_has_more}} when length(results) == limit ->
          :ok

        other ->
          Mix.raise("changes adapter result was invalid: #{inspect(other)}")
      end
    end)
  end

  defp adapter_operation!(adapter, :indexed_query, _base, state) do
    request = indexed_request(state.config)

    repeat(state.config["repeat"], fn ->
      case Adapter.execute_query(adapter, request) do
        {:ok, result} -> check_query_results!(result, state)
        other -> Mix.raise("indexed-query adapter result was invalid: #{inspect(other)}")
      end
    end)
  end

  defp service_operation!(uuid, :point_read, ids, _state) do
    Enum.each(ids, fn id ->
      case VialKeeper.Documents.get(uuid, %{id: id}) do
        {:ok, %{id: ^id, deleted: false}} -> :ok
        other -> Mix.raise("service point read was invalid: #{inspect(other)}")
      end
    end)
  end

  defp service_operation!(uuid, :bulk_write, %{service_operations: operations}, _state) do
    case VialKeeper.Documents.bulk_write(uuid, operations) do
      {:ok, results} when length(results) == length(operations) -> :ok
      other -> Mix.raise("service bulk write was invalid: #{inspect(other)}")
    end
  end

  defp service_operation!(uuid, :changes_read, _base, state) do
    limit = changes_limit(state.config)
    expected_has_more = limit < state.config["dataset_size"]

    repeat(state.config["repeat"], fn ->
      case VialKeeper.Changes.read(uuid, %{since: 0, limit: limit}) do
        {:ok, %{results: results, has_more: ^expected_has_more}} when length(results) == limit ->
          :ok

        other ->
          Mix.raise("service changes read was invalid: #{inspect(other)}")
      end
    end)
  end

  defp service_operation!(uuid, :indexed_query, _base, state) do
    request = indexed_request(state.config)

    repeat(state.config["repeat"], fn ->
      case VialKeeper.Query.execute(uuid, request) do
        {:ok, result} -> check_query_results!(result, state)
        other -> Mix.raise("service query was invalid: #{inspect(other)}")
      end
    end)
  end

  defp indexed_request(config),
    do: %{selector: %{"/category" => "task"}, limit: config["query_limit"]}

  defp check_query_results!(result, state) do
    results = value(result, :documents) || value(result, :results) || []

    if length(results) == min(state.config["query_limit"], state.fixture.category_match_count),
      do: :ok,
      else: Mix.raise("indexed query returned #{length(results)} documents")
  end

  defp changes_limit(config), do: min(config["batch_size"], config["dataset_size"])

  defp repeat(0, _fun), do: :ok

  defp repeat(count, fun) do
    :ok = fun.()
    repeat(count - 1, fun)
  end

  ## Minimal hand-written SQL reference

  defp minimal_statements(:point_read), do: [{:winner_select, @winner_select_sql}]

  defp minimal_statements(:bulk_write) do
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

  defp minimal_statements(:changes_read),
    do: [{:changes_select, @changes_select_sql}, {:changes_exists, @changes_exists_sql}]

  defp minimal_statements(:indexed_query), do: [{:indexed_query, @indexed_query_sql}]

  defp prepare_minimal(%{exqlite_minimal: variant} = variants, scenario) do
    statements = Raw.prepare_many(variant.conn, minimal_statements(scenario))
    variants = Map.put(variants, :exqlite_minimal, %{variant | statements: statements})
    Process.put({__MODULE__, :open_variants}, variants)
    variants
  end

  defp prepare_minimal(variants, _scenario), do: variants

  defp minimal_operation!(variant, scenario, base, state),
    do: run_minimal!(variant.conn, variant.statements, scenario, base, state)

  defp run_minimal!(conn, statements, :point_read, ids, _state) do
    Enum.each(ids, fn id ->
      [[^id, _revision, _json, _term, 0, _sequence, nil, _digest, _size, _type]] =
        Raw.run!(conn, statements.winner_select, [id])
    end)
  end

  defp run_minimal!(conn, statements, :bulk_write, %{batch: batch}, _state) do
    run = fn name, params -> Raw.run!(conn, Map.fetch!(statements, name), params) end
    physical_bulk_write!(conn, batch, run, fn -> Raw.run(conn, statements.rollback, []) end)
  end

  defp run_minimal!(conn, statements, :changes_read, _base, state) do
    limit = changes_limit(state.config)
    expected_has_more = if limit < state.config["dataset_size"], do: 1, else: 0

    repeat(state.config["repeat"], fn ->
      rows = Raw.run!(conn, statements.changes_select, [0, limit])
      last = rows |> List.last([0]) |> List.first()
      [[has_more]] = Raw.run!(conn, statements.changes_exists, [last])

      if length(rows) == limit and has_more == expected_has_more,
        do: :ok,
        else: Mix.raise("minimal changes result was invalid")
    end)
  end

  defp run_minimal!(conn, statements, :indexed_query, _base, state) do
    limit = state.config["query_limit"]
    expected = state.fixture.category_match_count

    repeat(state.config["repeat"], fn ->
      rows = Raw.run!(conn, statements.indexed_query, ["task", "task", limit + 1])
      count = if rows == [], do: 0, else: rows |> List.last() |> List.last()

      if length(rows) == min(limit + 1, expected) and count == expected,
        do: :ok,
        else: Mix.raise("minimal indexed-query result was invalid")
    end)
  end

  # One transaction writing the final document, revision, change, sequence and
  # replication-state rows, one INSERT per row.
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

  ## Report

  defp case_report(state, reference, verification, collected) do
    samples = collected.samples
    operations = operation_count(state.scenario, state.config)
    selected = state.selected
    series = fn kind, key -> Enum.map(samples, &get_in(&1, [kind, key])) end
    ns = Map.new(selected, &{&1, series.(&1, :ns)})

    variants =
      Map.new(selected, fn kind ->
        summary =
          ns[kind]
          |> Stats.summarize(operations)
          |> Map.put("layer", layer_label(kind))
          |> Map.put("samples_ns", ns[kind])
          |> Map.put(
            "median_reductions_per_operation",
            per_op(series.(kind, :reductions), operations)
          )
          |> Map.put("gcs_per_sample_median", Stats.round_float(Stats.median(series.(kind, :gcs))))
          |> Map.merge(native_counters(kind, samples, operations))

        {Atom.to_string(kind), summary}
      end)

    vs_reference =
      selected
      |> Enum.reject(&(&1 == reference))
      |> Map.new(&{Atom.to_string(&1), Stats.compare(ns[reference], ns[&1])})

    ladder = Enum.filter(@ladder, &(&1 in selected))

    steps =
      ladder
      |> Enum.zip(Enum.drop(ladder, 1))
      |> Enum.map(fn {lower, upper} ->
        Stats.compare(ns[lower], ns[upper])
        |> Map.merge(%{
          "from" => Atom.to_string(lower),
          "to" => Atom.to_string(upper),
          "adds" => step_description(lower, upper),
          "median_delta_ns_per_operation" =>
            Stats.round_float(Stats.median(Stats.paired_deltas(ns[lower], ns[upper])) / operations)
        })
      end)

    %{
      "storage_mode" => Atom.to_string(state.mode),
      "scenario" => Atom.to_string(state.scenario),
      "operations_per_sample" => operations,
      "dataset_size" => state.config["dataset_size"],
      "warmup" => state.config["warmup"],
      "iterations" => length(samples),
      "stop_reason" => collected.stop_reason,
      "elapsed_ms" => collected.elapsed_ms,
      "reference_variant" => Atom.to_string(reference),
      "fixture" => fixture_metadata(state.fixture),
      "verification" => verification,
      "captured_statements" => captured_statements(state),
      "variants" => variants,
      "vs_reference" => vs_reference,
      "ladder" => steps,
      "sql_shape" => sql_shape(ns, operations),
      "sample_order" =>
        Enum.map(samples, fn sample -> Enum.map(sample.__order__, &Atom.to_string/1) end)
    }
  end

  @capture_profile {__MODULE__, :capture_profile}

  # Statement counts of every captured sample (warmup included) and the SQL
  # mix of the last one, recorded while inputs are prepared.
  defp record_capture_profile(ops) do
    {counts, _last} = Process.get(@capture_profile, {[], nil})
    Process.put(@capture_profile, {[length(ops) | counts], ops})
  end

  defp captured_statements(%{worker: nil}), do: nil

  defp captured_statements(state) do
    {counts, ops} = Process.delete(@capture_profile) || {[], []}
    median = Stats.median(counts)

    %{
      "per_sample_median" => median,
      "per_operation" => Stats.round_float(median / operation_count(state.scenario, state.config)),
      "distinct_sql" => ops |> Enum.map(& &1.sql) |> Enum.uniq() |> length(),
      "kinds" => Enum.frequencies_by(ops, &Atom.to_string(&1.kind))
    }
  end

  defp native_counters(:native_replay, samples, operations) do
    %{
      "median_vm_steps_per_operation" =>
        per_op(Enum.map(samples, & &1.native_replay.extra.vm_steps), operations),
      "statements_per_sample" =>
        samples |> Enum.map(& &1.native_replay.extra.statements) |> Stats.median()
    }
  end

  defp native_counters(_kind, _samples, _operations), do: %{}

  defp sql_shape(ns, operations) do
    case ns do
      %{exqlite_minimal: minimal, exqlite_replay: replay} ->
        Stats.compare(minimal, replay)
        |> Map.merge(%{
          "from" => "exqlite_minimal",
          "to" => "exqlite_replay",
          "adds" => "SQL the adapter issues versus hand-written minimal SQL, both through ExQLite",
          "median_delta_ns_per_operation" =>
            Stats.round_float(Stats.median(Stats.paired_deltas(minimal, replay)) / operations)
        })

      _ ->
        nil
    end
  end

  defp step_description(lower, upper) do
    lower_index = Enum.find_index(@ladder, &(&1 == lower))
    upper_index = Enum.find_index(@ladder, &(&1 == upper))

    @ladder
    |> Enum.slice(lower_index..upper_index)
    |> then(&Enum.zip(&1, Enum.drop(&1, 1)))
    |> Enum.map_join("; ", &Map.fetch!(@step_descriptions, &1))
  end

  defp layer_label(:native_replay), do: "L0 native SQLite"
  defp layer_label(:exqlite_replay), do: "L1 ExQLite"
  defp layer_label(:connection_replay), do: "L2 Connection"
  defp layer_label(:vial_keeper_adapter), do: "L3 adapter"
  defp layer_label(:vial_keeper_service), do: "L4 service"
  defp layer_label(:vial_keeper_http), do: "L5 HTTP router"
  defp layer_label(:exqlite_minimal), do: "reference: minimal SQL"

  defp per_op(values, operations), do: Stats.round_float(Stats.median(values) / operations)

  defp operation_count(:point_read, config), do: config["read_count"]
  defp operation_count(:bulk_write, config), do: config["batch_size"]
  defp operation_count(:changes_read, config), do: config["repeat"]
  defp operation_count(:indexed_query, config), do: config["repeat"]

  defp fixture(documents) do
    %{
      documents: documents,
      category_match_count: Enum.count(documents, &(&1.body["category"] == "task"))
    }
  end

  defp fixture_metadata(fixture) do
    %{
      "documents" => length(fixture.documents),
      "category_task_documents" => fixture.category_match_count,
      "seed_sequence" => length(fixture.documents),
      "seeded_through" => "Adapter.apply_bulk_mutation (service layers: Documents.bulk_write)",
      "body_shape" => "category, priority, title, tags",
      "attachments" => "none"
    }
  end

  ## Fixture documents

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

  ## Output

  defp sqlite_metadata do
    {:ok, conn} = Sqlite3.open(":memory:")

    try do
      sqlite_metadata(conn)
    after
      Sqlite3.close(conn)
    end
  end

  defp sqlite_metadata(conn) do
    [[version, source_id]] =
      Raw.one_off_query!(conn, "SELECT sqlite_version(), sqlite_source_id()")

    options = conn |> Raw.one_off_query!("PRAGMA compile_options") |> Enum.map(&List.first/1)
    %{"version" => version, "source_id" => source_id, "compile_options" => options}
  end

  defp write_report(report, "-"), do: IO.puts(JSON.encode_to_iodata!(report))

  defp write_report(report, path) do
    File.mkdir_p!(Path.dirname(path))
    File.write!(path, [JSON.encode_to_iodata!(report), "\n"])
  end

  defp print_summary(report, output) do
    IO.puts("VialKeeper layer ladder report: #{output}")

    Enum.each(report["results"], fn result ->
      IO.puts(
        "\n  #{result["storage_mode"]}/#{result["scenario"]} " <>
          "(#{result["iterations"]} samples, #{result["stop_reason"]}, " <>
          "#{result["operations_per_sample"]} ops/sample, reference #{result["reference_variant"]})"
      )

      result["variants"]
      |> Enum.sort_by(fn {name, _} -> variant_rank(name) end)
      |> Enum.each(fn {name, summary} ->
        ratio =
          case result["vs_reference"][name] do
            nil ->
              "reference"

            comparison ->
              "x#{comparison["paired_ratio_median"]} #{inspect(comparison["paired_ratio_ci95"])}"
          end

        IO.puts(
          "    #{pad(summary["layer"], 24)} #{pad(name, 22)} " <>
            "#{pad(format_us(summary["median_ns_per_operation"]) <> "/op", 14)} #{ratio}"
        )
      end)

      Enum.each(result["ladder"], fn step ->
        IO.puts(
          "    step #{step["from"]} -> #{step["to"]}: " <>
            "+#{format_us(step["median_delta_ns_per_operation"])}/op " <>
            "(x#{step["paired_ratio_median"]} #{inspect(step["paired_ratio_ci95"])})"
        )
      end)
    end)
  end

  defp variant_rank(name) do
    Enum.find_index(@all_variants, &(Atom.to_string(&1) == name)) || length(@all_variants)
  end

  defp pad(value, width), do: String.pad_trailing(to_string(value), width)

  defp format_us(ns), do: "#{:erlang.float_to_binary(ns / 1000, decimals: 2)} us"

  defp default_output_path do
    timestamp = DateTime.utc_now() |> Calendar.strftime("%Y%m%dT%H%M%SZ")
    Path.join("output/benchmarks", "layer-ladder-#{timestamp}.json")
  end

  defp cleanup_path(nil), do: :ok
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
      scripts/bench_overhead.sh [options]
      MIX_ENV=prod mix run --no-start bench/sqlite_exqlite_overhead_benchmark.exs -- [options]

    Options:
      --mode memory|disk|both       SQLite mode (default: memory). The service and
                                    HTTP layers need catalog bundles: disk mode only.
      --scenario NAME|all           point_read, bulk_write, changes_read, indexed_query
      --variants NAME,...|all       #{Enum.map_join(@all_variants, ", ", &Atom.to_string/1)}
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
      --work-dir PATH               Databases and runtime root (default: #{@default_work_dir})
      --output PATH                 JSON report path (default: output/benchmarks/...json)

    Environment equivalents:
      VIALKEEPER_OVERHEAD_ITERATIONS, VIALKEEPER_OVERHEAD_WARMUP,
      VIALKEEPER_OVERHEAD_DATASET, VIALKEEPER_OVERHEAD_BATCH,
      VIALKEEPER_OVERHEAD_READS, CC (native control compiler)
    """
  end

  defmodule Raw do
    @moduledoc false

    alias Exqlite.Sqlite3

    def prepare_many(conn, definitions) do
      Map.new(definitions, fn {name, sql} -> {name, prepare!(conn, sql)} end)
    end

    def prepare!(conn, sql) do
      case Sqlite3.prepare(conn, sql) do
        {:ok, statement} -> statement
        {:error, reason} -> Mix.raise("could not prepare benchmark SQL: #{inspect(reason)}")
      end
    end

    def run!(conn, statement, params \\ []) do
      case run(conn, statement, params) do
        {:ok, rows} -> rows
        {:error, reason} -> Mix.raise("ExQLite benchmark SQL failed: #{inspect(reason)}")
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
