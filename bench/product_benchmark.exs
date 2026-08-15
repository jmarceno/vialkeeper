defmodule VialKeeper.Benchmarks.Runner do
  @moduledoc """
  Repeatable storage benchmark runner for VialKeeper.

  The runner intentionally lives outside the application release. It exercises
  the real SQLite adapter while keeping setup, measurement, and cleanup
  separate. Run it with `MIX_ENV=test` so the existing in-memory OTel exporters
  make the instrumentation visible in the result file.
  """

  alias VialKeeper.JSON.StrictDecoder
  alias VialKeeper.Observability.Instrumentation.{Changes, Database}
  alias VialKeeper.Runtime.DatabaseCatalog
  alias VialKeeper.Storage.SQLite.Adapter
  alias VialKeeper.View.Manager

  @scenarios [
    :bulk_write,
    :point_read,
    :changes_read,
    :index_build,
    :indexed_query,
    :fts_query,
    :fts_rebuild
  ]
  @catalog_scenarios [:concurrent_point_read, :multi_writer]
  @allowed_scenarios @scenarios ++ @catalog_scenarios
  @concurrent_reader_counts [1, 2, 4, 8]
  @multi_writer_counts [1, 2, 4, 8]
  @modes [:disk, :memory]
  @fts_index_name "by-text"
  @fts_query_term "lighthouse"
  @fts_filler_sentence "The harbour log recorded wind, tide, and vessel traffic each morning. "
  @fts_filler String.duplicate(@fts_filler_sentence, 29)
  @span_names [
    "vial_keeper.database.command",
    "vial_keeper.changes.read",
    "vial_keeper.query.execute",
    "vial_keeper.index.build",
    "vial_keeper.search.rebuild"
  ]
  @metric_names [
    "vial_keeper.database.command.duration",
    "vial_keeper.changes.read.duration",
    "vial_keeper.query.execute.duration",
    "vial_keeper.index.build.duration",
    "vial_keeper.search.rebuild.duration"
  ]

  @default_iterations 15
  @default_warmup 3
  @default_dataset_size 500
  @default_batch_size 100
  @default_read_count 100
  @default_max_regression_pct 20.0

  @doc false
  @spec main([binary()]) :: :ok
  def main(argv) do
    options = parse_options(argv)
    ensure_test_observability!()

    with_isolated_runtime(fn ->
      config = benchmark_config(options)
      started_at = DateTime.utc_now() |> DateTime.to_iso8601()
      modes = parse_modes(options[:mode])
      scenarios = parse_scenarios(options[:scenario])

      results =
        for mode <- modes,
            scenario <- scenarios,
            result <- List.wrap(run_case(mode, scenario, config)) do
          result
        end

      report = %{
        "schema_version" => 1,
        "started_at" => started_at,
        "git_revision" => git_revision(),
        "runtime" => runtime_metadata(),
        "configuration" => config,
        "results" => results
      }

      output = options[:output] || default_output_path()
      write_report(report, output)
      print_summary(report, output)

      if baseline = options[:baseline] do
        compare_with_baseline!(report, baseline, config["max_regression_pct"])
      end
    end)
  end

  defp with_isolated_runtime(fun) do
    root = Path.join(System.tmp_dir!(), "vialkeeper-product-benchmark-#{unique_suffix()}")
    previous_root = Application.get_env(:vial_keeper, :database_root)
    previous_listener = Application.get_env(:vial_keeper, :listener)
    ensure_application_stopped!()
    File.mkdir_p!(root)
    Application.put_env(:vial_keeper, :database_root, root)
    Application.put_env(:vial_keeper, :listener, ip: {127, 0, 0, 1}, port: 0)

    try do
      {:ok, _started} = Application.ensure_all_started(:vial_keeper)
      fun.()
    after
      _ = Application.stop(:vial_keeper)
      restore_application_env(:database_root, previous_root)
      restore_application_env(:listener, previous_listener)
      _ = File.rm_rf(root)
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
          warmup: :integer,
          dataset: :integer,
          batch: :integer,
          reads: :integer,
          output: :string,
          baseline: :string,
          max_regression: :float,
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
      positive_option(options, :iterations, "VIALKEEPER_BENCH_ITERATIONS", @default_iterations)

    warmup = non_negative_option(options, :warmup, "VIALKEEPER_BENCH_WARMUP", @default_warmup)

    dataset_size =
      positive_option(options, :dataset, "VIALKEEPER_BENCH_DATASET", @default_dataset_size)

    batch_size = positive_option(options, :batch, "VIALKEEPER_BENCH_BATCH", @default_batch_size)
    read_count = positive_option(options, :reads, "VIALKEEPER_BENCH_READS", @default_read_count)

    if batch_size > 500 do
      Mix.raise("--batch must be at most the configured host bulk limit (500)")
    end

    max_regression_pct =
      options[:max_regression] ||
        env_float("VIALKEEPER_BENCH_MAX_REGRESSION_PCT", @default_max_regression_pct)

    if max_regression_pct < 0 do
      Mix.raise("--max-regression must be non-negative")
    end

    %{
      "iterations" => iterations,
      "warmup" => warmup,
      "dataset_size" => dataset_size,
      "batch_size" => batch_size,
      "read_count" => read_count,
      "max_regression_pct" => max_regression_pct
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

  defp env_float(env, default) do
    case System.get_env(env) do
      nil -> default
      value -> String.to_float(value)
    end
  rescue
    ArgumentError -> Mix.raise("#{env} must be a number")
  end

  defp parse_modes(nil), do: @modes
  defp parse_modes("both"), do: @modes
  defp parse_modes(value), do: parse_atoms(value, @modes, "mode")

  defp parse_scenarios(nil), do: @scenarios
  defp parse_scenarios("all"), do: @scenarios
  defp parse_scenarios(value), do: parse_atoms(value, @allowed_scenarios, "scenario")

  defp parse_atoms(value, allowed, label) do
    atoms =
      value
      |> String.split(",", trim: true)
      |> Enum.map(&String.to_existing_atom/1)

    if atoms != [] and Enum.all?(atoms, &(&1 in allowed)) do
      atoms
    else
      Mix.raise(
        "unknown #{label} #{inspect(value)}; allowed: #{Enum.join(Enum.map(allowed, &Atom.to_string/1), ", ")}"
      )
    end
  rescue
    ArgumentError -> Mix.raise("unknown #{label} #{inspect(value)}")
  end

  defp run_case(:memory, :concurrent_point_read, _config), do: []
  defp run_case(:memory, :multi_writer, _config), do: []

  defp run_case(:disk, :concurrent_point_read, config) do
    for reader_count <- @concurrent_reader_counts, writer? <- [false, true] do
      run_concurrent_point_read(reader_count, writer?, config)
    end
  end

  defp run_case(:disk, :multi_writer, config) do
    for writer_count <- @multi_writer_counts, shared? <- [false, true] do
      run_multi_writer(writer_count, shared?, config)
    end
  end

  defp run_case(mode, scenario, config) do
    reset_observability()

    with_adapter(mode, fn adapter ->
      uuid = adapter.identity.database_uuid
      setup_scenario(adapter, uuid, scenario, config)
      reset_observability()

      {operation_count, operation, cleanup, scenario_metadata} =
        scenario_operation(adapter, uuid, scenario, config)

      memory_before = :erlang.memory(:total)

      {{samples, _last_result}, runtime_sample} =
        with_runtime_sample(fn ->
          measure(operation, cleanup, config["warmup"], config["iterations"])
        end)

      memory_after = :erlang.memory(:total)
      flush_metrics()

      result = summarize_samples(samples, operation_count)

      Map.merge(result, %{
        "scenario" => Atom.to_string(scenario),
        "storage_mode" => Atom.to_string(mode),
        "operation_count" => operation_count,
        "dataset_size" => config["dataset_size"],
        "warmup" => config["warmup"],
        "iterations" => config["iterations"],
        "memory_before_bytes" => memory_before,
        "memory_after_bytes" => memory_after,
        "memory_delta_bytes" => memory_after - memory_before,
        "sqlite" => sqlite_metadata(adapter),
        "observability" => observability_metadata(),
        "runtime_sample" => runtime_sample,
        "scenario_metadata" => scenario_metadata
      })
    end)
  end

  defp run_concurrent_point_read(reader_count, writer?, config) do
    reset_observability()

    with_catalog_database(fn uuid ->
      seed_catalog_documents(uuid, config["dataset_size"], config["batch_size"])
      reset_observability()

      dataset_size = config["dataset_size"]
      read_count = config["read_count"]
      ids = 0..(dataset_size - 1) |> Enum.map(&"seed-#{&1}") |> List.to_tuple()
      operation_count = reader_count * read_count

      operation = fn token ->
        run_concurrent_sample(uuid, ids, dataset_size, read_count, reader_count, writer?, token)
      end

      memory_before = :erlang.memory(:total)

      {{samples, _last_result}, runtime_sample} =
        with_runtime_sample(fn ->
          measure(operation, &noop_cleanup/1, config["warmup"], config["iterations"])
        end)

      memory_after = :erlang.memory(:total)
      flush_metrics()

      result = summarize_samples(samples, operation_count)
      scenario_name = concurrent_scenario_name(reader_count, writer?)

      Map.merge(result, %{
        "scenario" => scenario_name,
        "storage_mode" => "disk",
        "operation_count" => operation_count,
        "dataset_size" => dataset_size,
        "warmup" => config["warmup"],
        "iterations" => config["iterations"],
        "memory_before_bytes" => memory_before,
        "memory_after_bytes" => memory_after,
        "memory_delta_bytes" => memory_after - memory_before,
        "sqlite" => %{},
        "observability" => observability_metadata(),
        "runtime_sample" => runtime_sample,
        "scenario_metadata" => %{
          "path" => "catalog",
          "reader_count" => reader_count,
          "steady_writer" => writer?,
          "read_count" => read_count
        }
      })
    end)
  end

  defp run_concurrent_sample(
         uuid,
         ids,
         dataset_size,
         read_count,
         reader_count,
         writer?,
         token
       ) do
    flag = :atomics.new(1, [])
    :atomics.put(flag, 1, 1)

    writer_task =
      if writer? do
        Task.async(fn -> steady_writer_loop(uuid, token, flag) end)
      end

    readers =
      Enum.map(1..reader_count, fn reader_index ->
        Task.async(fn ->
          concurrent_reader_work(uuid, ids, dataset_size, read_count, reader_index)
        end)
      end)

    Enum.each(Task.await_many(readers, 60_000), fn
      {:ok, _} -> :ok
      other -> Mix.raise("concurrent point read failed: #{inspect(other)}")
    end)

    :atomics.put(flag, 1, 0)
    if writer_task, do: Task.await(writer_task, 60_000)

    {:ok, reader_count * read_count}
  end

  defp concurrent_reader_work(uuid, ids, dataset_size, read_count, reader_index) do
    start = rem(reader_index * read_count, dataset_size)

    Enum.each(0..(read_count - 1), fn offset ->
      id = elem(ids, rem(start + offset, dataset_size))
      assert_ok!(VialKeeper.Documents.get(uuid, %{id: id}), :concurrent_point_read)
    end)

    {:ok, read_count}
  end

  defp steady_writer_loop(uuid, token, flag) do
    write_until_stopped(uuid, token, flag, 0)
  end

  defp write_until_stopped(uuid, token, flag, n) do
    if :atomics.get(flag, 1) == 1 do
      id = "writer-#{phase_name(token)}-#{token_number(token)}-#{n}"
      body = %{"category" => "note", "priority" => rem(n, 100), "title" => "Writer #{n}"}
      assert_ok!(VialKeeper.Documents.put(uuid, %{id: id, body: body}), :concurrent_writer)
      write_until_stopped(uuid, token, flag, n + 1)
    else
      :ok
    end
  end

  defp concurrent_scenario_name(reader_count, true),
    do: "concurrent_point_read.r#{reader_count}+writer"

  defp concurrent_scenario_name(reader_count, false),
    do: "concurrent_point_read.r#{reader_count}"

  defp run_multi_writer(writer_count, shared?, config) do
    reset_observability()

    with_catalog_databases(if(shared?, do: 1, else: writer_count), fn uuids ->
      reset_observability()

      write_count = config["read_count"]
      operation_count = writer_count * write_count
      targets = if shared?, do: List.duplicate(hd(uuids), writer_count), else: uuids

      operation = fn token ->
        run_multi_writer_sample(targets, write_count, token)
      end

      memory_before = :erlang.memory(:total)

      {{samples, _last_result}, runtime_sample} =
        with_runtime_sample(fn ->
          measure(operation, &noop_cleanup/1, config["warmup"], config["iterations"])
        end)

      memory_after = :erlang.memory(:total)
      flush_metrics()

      result = summarize_samples(samples, operation_count)

      Map.merge(result, %{
        "scenario" => multi_writer_scenario_name(writer_count, shared?),
        "storage_mode" => "disk",
        "operation_count" => operation_count,
        "dataset_size" => config["dataset_size"],
        "warmup" => config["warmup"],
        "iterations" => config["iterations"],
        "memory_before_bytes" => memory_before,
        "memory_after_bytes" => memory_after,
        "memory_delta_bytes" => memory_after - memory_before,
        "sqlite" => %{},
        "observability" => observability_metadata(),
        "runtime_sample" => runtime_sample,
        "scenario_metadata" => %{
          "path" => "catalog",
          "writer_count" => writer_count,
          "shared_database" => shared?,
          "write_count" => write_count
        }
      })
    end)
  end

  defp run_multi_writer_sample(uuids, write_count, token) do
    writers =
      uuids
      |> Enum.with_index(1)
      |> Enum.map(fn {uuid, writer_index} ->
        Task.async(fn -> multi_writer_work(uuid, write_count, writer_index, token) end)
      end)

    Enum.each(Task.await_many(writers, 60_000), fn
      {:ok, _} -> :ok
      other -> Mix.raise("multi writer sample failed: #{inspect(other)}")
    end)

    {:ok, length(uuids) * write_count}
  end

  defp multi_writer_work(uuid, write_count, writer_index, token) do
    Enum.each(0..(write_count - 1), fn n ->
      id = "mw-#{writer_index}-#{phase_name(token)}-#{token_number(token)}-#{n}"
      body = %{"category" => "note", "priority" => rem(n, 100), "title" => "Writer #{n}"}
      assert_result!(VialKeeper.Documents.put(uuid, %{id: id, body: body}), :multi_writer)
    end)

    {:ok, write_count}
  end

  defp multi_writer_scenario_name(writer_count, true),
    do: "multi_writer.shared.w#{writer_count}"

  defp multi_writer_scenario_name(writer_count, false),
    do: "multi_writer.independent.w#{writer_count}"

  defp with_catalog_databases(count, fun) when is_integer(count) and count > 0 do
    uuids = Enum.map(1..count, fn _ -> open_catalog_benchmark_database() end)

    try do
      fun.(uuids)
    after
      Enum.each(uuids, &close_catalog_benchmark_database/1)
    end
  end

  defp open_catalog_benchmark_database do
    relative = "bench-catalog-#{System.unique_integer([:positive])}.vialkeeper"

    case DatabaseCatalog.create(relative) do
      {:ok, identity} ->
        uuid = identity.database_uuid
        assert_ok!(DatabaseCatalog.open(uuid), :catalog_open)
        :ok = Manager.await_resumed(uuid)
        uuid

      {:error, error} ->
        Mix.raise("could not create catalog benchmark database: #{inspect(error)}")
    end
  end

  defp close_catalog_benchmark_database(uuid) do
    _ = DatabaseCatalog.close(uuid)
    _ = DatabaseCatalog.unregister(uuid)
    :ok
  end

  defp with_catalog_database(fun) do
    with_catalog_databases(1, fn [uuid] -> fun.(uuid) end)
  end

  defp seed_catalog_documents(uuid, count, batch_size) do
    0..(count - 1)
    |> Enum.map(&catalog_put_operation("seed-#{&1}", &1))
    |> Enum.chunk_every(batch_size)
    |> Enum.each(fn operations ->
      assert_ok!(VialKeeper.Documents.bulk_write(uuid, operations), :catalog_seed)
    end)
  end

  defp catalog_put_operation(id, value) do
    %{type: :put, id: id, body: put_operation(id, value).body}
  end

  defp with_adapter(:memory, fun) do
    case Adapter.create(":memory:", %{storage_mode: :memory}) do
      {:ok, adapter} ->
        try do
          fun.(adapter)
        after
          _ = Adapter.close(adapter)
        end

      {:error, error} ->
        Mix.raise("could not create in-memory benchmark database: #{inspect(error)}")
    end
  end

  defp with_adapter(:disk, fun) do
    path =
      Path.join(System.tmp_dir!(), "vialkeeper-benchmark-#{System.unique_integer([:positive])}.db")

    case Adapter.create(path, %{storage_mode: :disk}) do
      {:ok, adapter} ->
        try do
          fun.(adapter)
        after
          _ = Adapter.close(adapter)
          cleanup_database(path)
        end

      {:error, error} ->
        cleanup_database(path)
        Mix.raise("could not create disk benchmark database: #{inspect(error)}")
    end
  end

  defp setup_scenario(adapter, uuid, scenario, config)
       when scenario in [:fts_query, :fts_rebuild] do
    seed_documents(
      adapter,
      uuid,
      config["dataset_size"],
      config["batch_size"],
      &fts_put_operation/2
    )

    create_named_index(adapter, uuid, fts_index_definition(@fts_index_name))
    :ok
  end

  defp setup_scenario(adapter, uuid, scenario, config)
       when scenario in [:bulk_write, :point_read, :changes_read, :index_build, :indexed_query] do
    seed_documents(adapter, uuid, config["dataset_size"], config["batch_size"])

    if scenario == :indexed_query do
      create_named_index(adapter, uuid, category_index_definition("by-category"))
    end

    :ok
  end

  defp seed_documents(adapter, uuid, count, batch_size, operation_fun \\ &put_operation/2) do
    0..(count - 1)
    |> Enum.map(&operation_fun.("seed-#{&1}", &1))
    |> Enum.chunk_every(batch_size)
    |> Enum.each(fn operations ->
      assert_ok!(Adapter.apply_bulk_mutation(adapter, %{operations: operations}), :seed)
    end)

    _ = uuid
    :ok
  end

  defp scenario_operation(adapter, uuid, :bulk_write, config) do
    batch_size = config["batch_size"]

    operation = fn token ->
      operations =
        Enum.map(0..(batch_size - 1), fn offset ->
          put_operation("bench-#{phase_name(token)}-#{token_number(token)}-#{offset}", offset)
        end)

      observable_command(uuid, {:command, :bulk_write, %{operations: operations}}, fn ->
        Adapter.apply_bulk_mutation(adapter, %{operations: operations})
      end)
    end

    {batch_size, operation, &noop_cleanup/1, %{"batch_size" => batch_size}}
  end

  defp scenario_operation(adapter, uuid, :point_read, config) do
    dataset_size = config["dataset_size"]
    read_count = config["read_count"]
    ids = 0..(dataset_size - 1) |> Enum.map(&"seed-#{&1}") |> List.to_tuple()

    operation = fn token ->
      start = rem(token_number(token) * read_count, dataset_size)

      Enum.each(0..(read_count - 1), fn offset ->
        id = elem(ids, rem(start + offset, dataset_size))

        result =
          observable_command(uuid, {:command, :get_document, %{id: id}}, fn ->
            Adapter.get_document(adapter, %{document_id: id})
          end)

        assert_ok!(result, :point_read)
      end)

      {:ok, read_count}
    end

    {read_count, operation, &noop_cleanup/1, %{"read_count" => read_count}}
  end

  defp scenario_operation(adapter, uuid, :changes_read, config) do
    limit = min(config["batch_size"], config["dataset_size"])
    request = %{since: 0, limit: limit}

    operation = fn _token ->
      Changes.read(uuid, 0, fn ->
        observable_command(uuid, {:command, :read_changes, request}, fn ->
          Adapter.read_changes(adapter, request)
        end)
      end)
    end

    {limit, operation, &noop_cleanup/1, %{"limit" => limit}}
  end

  defp scenario_operation(adapter, uuid, :index_build, _config) do
    operation = fn token ->
      definition =
        category_index_definition("by-category-#{phase_name(token)}-#{token_number(token)}")

      observable_command(uuid, {:command, :create_index, definition}, fn ->
        Adapter.create_index(adapter, definition)
      end)
    end

    cleanup = fn
      {:ok, %{"index_id" => index_id}} ->
        assert_ok!(Adapter.delete_index(adapter, index_id), :index_cleanup)

      other ->
        Mix.raise("index benchmark did not return an index id: #{inspect(other)}")
    end

    {1, operation, cleanup, %{"index_type" => "structured", "indexed_field" => "/category"}}
  end

  defp scenario_operation(adapter, uuid, :indexed_query, _config) do
    request = %{
      selector: %{"/category" => "task"},
      index: "by-category",
      limit: 50
    }

    operation = fn _token ->
      observable_command(uuid, {:command, :query, request}, fn ->
        Adapter.execute_query(adapter, request)
      end)
    end

    {1, operation, &noop_cleanup/1, %{"index" => "by-category", "selector" => "/category=task"}}
  end

  defp scenario_operation(adapter, uuid, :fts_query, _config) do
    request = %{
      search: %{index: @fts_index_name, text: @fts_query_term, mode: "all"},
      limit: 50
    }

    operation = fn _token ->
      observable_command(uuid, {:command, :query, request}, fn ->
        Adapter.execute_query(adapter, request)
      end)
    end

    {1, operation, &noop_cleanup/1,
     %{"index" => @fts_index_name, "search" => @fts_query_term, "mode" => "all"}}
  end

  defp scenario_operation(adapter, uuid, :fts_rebuild, _config) do
    index_id = named_index_id!(adapter, @fts_index_name)

    operation = fn _token ->
      observable_command(uuid, {:command, :rebuild_index, index_id}, fn ->
        Adapter.rebuild_index(adapter, index_id)
      end)
    end

    {1, operation, &noop_cleanup/1, %{"index" => @fts_index_name, "operation" => "rebuild"}}
  end

  defp named_index_id!(adapter, name) do
    case Adapter.list_indexes(adapter) do
      {:ok, indexes} ->
        index =
          Enum.find(indexes, fn candidate ->
            Map.get(candidate, "name", Map.get(candidate, :name)) == name
          end)

        id = index && Map.get(index, "index_id", Map.get(index, :index_id))

        if is_binary(id) do
          id
        else
          Mix.raise("fts rebuild benchmark missing index #{name}")
        end

      other ->
        Mix.raise("fts rebuild benchmark could not list indexes: #{inspect(other)}")
    end
  end

  defp create_named_index(adapter, uuid, definition) do
    assert_ok!(
      observable_command(uuid, {:command, :create_index, definition}, fn ->
        Adapter.create_index(adapter, definition)
      end),
      :index_setup
    )
  end

  defp category_index_definition(name) do
    %{
      "name" => name,
      "type" => "structured",
      "fields" => [%{"path" => "/category", "type" => "string", "direction" => "asc"}]
    }
  end

  defp fts_index_definition(name) do
    %{
      "name" => name,
      "type" => "full_text",
      "fields" => ["/text"],
      "tokenization" => %{"strategy" => "unicode_words_v1", "diacritics" => "preserve"}
    }
  end

  defp put_operation(id, value) do
    %{
      operation: :put,
      document_id: id,
      body: %{
        "category" => if(rem(value, 4) == 0, do: "task", else: "note"),
        "priority" => rem(value, 100),
        "title" => "Benchmark document #{value}",
        "tags" => ["benchmark", "v1"]
      }
    }
  end

  defp fts_put_operation(id, value) do
    distinctive = if rem(value, 4) == 0, do: @fts_query_term <> " ", else: ""

    %{
      operation: :put,
      document_id: id,
      body: %{
        "title" => "Benchmark document #{value}",
        "text" => distinctive <> @fts_filler
      }
    }
  end

  defp observable_command(uuid, command, fun),
    do: Database.command(uuid, command, fun)

  defp measure(operation, cleanup, warmup, iterations) do
    Enum.each(sequence(warmup), fn number ->
      result = operation.({:warmup, number})
      cleanup.(assert_result!(result, :warmup))
    end)

    Enum.map_reduce(sequence(iterations), nil, fn number, _last_result ->
      {elapsed_us, result} = :timer.tc(fn -> operation.({:sample, number}) end)
      result = assert_result!(result, :sample)
      cleanup.(result)
      {elapsed_us, result}
    end)
  end

  defp assert_result!({:ok, _} = result, _label), do: result
  defp assert_result!(:ok = result, _label), do: result

  defp assert_result!({:error, error}, label),
    do: Mix.raise("benchmark #{label} operation failed: #{inspect(error)}")

  defp assert_result!(result, label),
    do: Mix.raise("benchmark #{label} returned an unexpected result: #{inspect(result)}")

  defp assert_ok!({:ok, _} = result, _label), do: result
  defp assert_ok!(:ok = result, _label), do: result

  defp assert_ok!({:error, error}, label),
    do: Mix.raise("benchmark #{label} setup failed: #{inspect(error)}")

  defp assert_ok!(result, label),
    do: Mix.raise("benchmark #{label} setup returned an unexpected result: #{inspect(result)}")

  defp summarize_samples(samples, operation_count) do
    values = Enum.sort(samples)
    count = length(values)
    median_us = percentile(values, 0.50)
    p95_us = percentile(values, 0.95)
    p99_us = percentile(values, 0.99)
    mean_us = Enum.sum(values) / count

    %{
      "sample_us" => values,
      "mean_us" => Float.round(mean_us, 2),
      "median_us" => median_us,
      "p95_us" => p95_us,
      "p99_us" => p99_us,
      "median_us_per_operation" => Float.round(median_us / operation_count, 2),
      "median_operations_per_second" => Float.round(operation_count * 1_000_000 / median_us, 2)
    }
  end

  defp percentile(values, fraction) do
    index = max(1, ceil(length(values) * fraction)) - 1
    Enum.at(values, index)
  end

  defp sqlite_metadata(adapter) do
    %{
      "journal_mode" => pragma_value(adapter, "journal_mode"),
      "synchronous" => pragma_value(adapter, "synchronous"),
      "locking_mode" => pragma_value(adapter, "locking_mode"),
      "cache_size" => pragma_value(adapter, "cache_size"),
      "temp_store" => pragma_value(adapter, "temp_store")
    }
  end

  defp pragma_value(adapter, pragma) do
    case VialKeeper.Storage.SQLite.Connection.pragma(adapter.conn, pragma) do
      {:ok, [[value]]} -> to_string(value)
      other -> inspect(other)
    end
  end

  defp reset_observability do
    VialKeeper.Observability.TestExporter.reset()
    VialKeeper.Observability.TestMetricExporter.reset()
  end

  defp flush_metrics do
    deadline = System.monotonic_time(:millisecond) + 1_000

    wait_for_metrics(deadline)
  end

  defp wait_for_metrics(deadline) do
    if System.monotonic_time(:millisecond) >= deadline or metrics_exported?() do
      :ok
    else
      Process.sleep(10)
      wait_for_metrics(deadline)
    end
  end

  defp metrics_exported? do
    Enum.any?(@metric_names, fn name ->
      VialKeeper.Observability.TestMetricExporter.datapoints(name) != []
    end)
  end

  defp observability_metadata do
    %{
      "span_counts" =>
        Map.new(@span_names, fn name ->
          {name, length(VialKeeper.Observability.TestExporter.spans_named(name))}
        end),
      "metric_datapoint_counts" =>
        Map.new(@metric_names, fn name ->
          {name, length(VialKeeper.Observability.TestMetricExporter.datapoints(name))}
        end)
    }
  end

  defp ensure_test_observability! do
    unless Code.ensure_loaded?(VialKeeper.Observability.TestExporter) and
             Code.ensure_loaded?(VialKeeper.Observability.TestMetricExporter) do
      Mix.raise("run benchmarks with MIX_ENV=test to enable the in-memory observability exporters")
    end
  end

  defp write_report(report, "-") do
    IO.puts(JSON.encode_to_iodata!(report))
  end

  defp write_report(report, path) do
    File.mkdir_p!(Path.dirname(path))
    json = JSON.encode_to_iodata!(report) |> IO.iodata_to_binary()
    File.write!(path, json <> "\n")
  end

  defp print_summary(report, output) do
    IO.puts("VialKeeper benchmark report: #{output}")

    Enum.each(report["results"], fn result ->
      IO.puts(
        "  #{result["storage_mode"]}/#{result["scenario"]}: " <>
          "median #{result["median_us"]} us, " <>
          "p95 #{result["p95_us"]} us, " <>
          "#{result["median_operations_per_second"]} ops/s"
      )
    end)
  end

  defp compare_with_baseline!(report, baseline_path, threshold_pct) do
    baseline = decode_baseline!(baseline_path)

    validate_baseline_configuration!(
      report["configuration"],
      baseline["configuration"],
      baseline_path
    )

    failures =
      Enum.flat_map(report["results"], fn current ->
        case find_result(baseline["results"], current["storage_mode"], current["scenario"]) do
          nil ->
            [{current, :missing_baseline}]

          previous ->
            threshold = previous["median_us"] * (1 + threshold_pct / 100)

            if current["median_us"] > threshold do
              [{current, {:regressed, previous["median_us"], threshold_pct}}]
            else
              []
            end
        end
      end)

    if failures != [] do
      IO.puts("Benchmark regression detected against #{baseline_path}:")

      Enum.each(failures, fn {current, reason} ->
        IO.puts("  #{current["storage_mode"]}/#{current["scenario"]}: #{inspect(reason)}")
      end)

      System.halt(1)
    end

    IO.puts(
      "Benchmark comparison passed against #{baseline_path} (median threshold #{threshold_pct}%)."
    )
  end

  defp decode_baseline!(baseline_path) do
    case baseline_path |> File.read!() |> StrictDecoder.decode() do
      {:ok, baseline} -> baseline
      {:error, error} -> Mix.raise("invalid benchmark baseline #{baseline_path}: #{inspect(error)}")
    end
  end

  defp validate_baseline_configuration!(current, baseline, baseline_path)
       when is_map(current) and is_map(baseline) do
    current = Map.delete(current, "max_regression_pct")
    baseline = Map.delete(baseline, "max_regression_pct")

    if current != baseline do
      Mix.raise(
        "baseline configuration does not match #{baseline_path}; " <>
          "use the same iterations, warmup, dataset, batch, and read count"
      )
    end
  end

  defp validate_baseline_configuration!(_current, _baseline, baseline_path) do
    Mix.raise("baseline report #{baseline_path} is missing benchmark configuration")
  end

  defp find_result(results, mode, scenario) do
    Enum.find(results, fn result ->
      result["storage_mode"] == mode and result["scenario"] == scenario
    end)
  end

  defp runtime_metadata do
    %{
      "elixir" => System.version(),
      "otp" => :erlang.system_info(:otp_release) |> to_string(),
      "sqlite" => VialKeeper.Diagnostics.runtime().backend,
      "schedulers_online" => :erlang.system_info(:schedulers_online),
      "dirty_cpu_schedulers_online" => :erlang.system_info(:dirty_cpu_schedulers_online),
      "dirty_io_schedulers" => :erlang.system_info(:dirty_io_schedulers),
      "os" => :os.type() |> inspect()
    }
  end

  defp with_runtime_sample(fun) do
    wall_before = scheduler_wall_snapshot()
    start_msacc()
    result = fun.()
    msacc = take_msacc()
    wall_after = scheduler_wall_snapshot()

    {result,
     %{
       "msacc" => msacc,
       "scheduler_wall_time" => scheduler_wall_delta(wall_before, wall_after)
     }}
  end

  defp start_msacc do
    _ = Application.ensure_all_started(:runtime_tools)

    case Code.ensure_loaded(:msacc) do
      {:module, _} ->
        _ = apply(:msacc, :stop, [])
        _ = apply(:msacc, :reset, [])
        _ = apply(:msacc, :start, [])
        :ok

      _ ->
        :ok
    end
  rescue
    _ -> :ok
  end

  defp take_msacc do
    case Code.ensure_loaded(:msacc) do
      {:module, _} ->
        stats = apply(:msacc, :stats, [])
        _ = apply(:msacc, :stop, [])
        summarize_msacc(stats)

      _ ->
        %{}
    end
  rescue
    _ -> %{}
  end

  defp summarize_msacc(stats) when is_list(stats) do
    totals =
      Enum.reduce(stats, %{}, fn sample, acc ->
        type = sample[:type] || sample.type
        system = sample[:system] || 0
        realtime = sample[:realtime] || 0
        key = type |> to_string()

        acc
        |> Map.update(key <> "_system", system, &(&1 + system))
        |> Map.update(key <> "_realtime", realtime, &(&1 + realtime))
      end)

    Map.merge(totals, %{
      "dirty_cpu_system" => Map.get(totals, "dirty_cpu_system", 0),
      "normal_system" => Map.get(totals, "normal_system", 0),
      "gc_system" => Map.get(totals, "gc_system", 0)
    })
  end

  defp summarize_msacc(_), do: %{}

  defp scheduler_wall_snapshot do
    _ = :erlang.system_flag(:scheduler_wall_time, true)
    :erlang.statistics(:scheduler_wall_time)
  rescue
    _ -> []
  end

  defp scheduler_wall_delta(before, after_sample)
       when is_list(before) and is_list(after_sample) do
    before_map = Map.new(before, fn {id, active, total} -> {id, {active, total}} end)

    {active, total} =
      Enum.reduce(after_sample, {0, 0}, fn {id, active_after, total_after},
                                           {active_acc, total_acc} ->
        {active_before, total_before} = Map.get(before_map, id, {0, 0})
        {active_acc + (active_after - active_before), total_acc + (total_after - total_before)}
      end)

    percent =
      if total > 0, do: Float.round(active * 100 / total, 2), else: 0.0

    %{
      "active_percent" => percent,
      "schedulers_sampled" => length(after_sample)
    }
  end

  defp scheduler_wall_delta(_before, _after), do: %{}

  defp git_revision do
    case System.cmd("git", ["rev-parse", "HEAD"], stderr_to_stdout: true) do
      {revision, 0} -> String.trim(revision)
      _ -> "unknown"
    end
  end

  defp default_output_path do
    timestamp = DateTime.utc_now() |> Calendar.strftime("%Y%m%dT%H%M%SZ")
    Path.join("output/benchmarks", "vialkeeper-#{timestamp}.json")
  end

  defp cleanup_database(path) do
    for suffix <- ["", "-journal", "-wal", "-shm"] do
      _ = File.rm(path <> suffix)
    end

    :ok
  end

  defp unique_suffix,
    do: "#{System.system_time(:microsecond)}-#{System.unique_integer([:positive])}"

  defp phase_name({phase, _number}), do: Atom.to_string(phase)
  defp token_number({_phase, number}), do: number

  defp sequence(0), do: []
  defp sequence(count), do: 1..count

  defp noop_cleanup(_result), do: :ok

  defp usage do
    """
    Usage:
      MIX_ENV=test mix run --no-start bench/product_benchmark.exs -- [options]

    Options:
      --mode disk|memory|both        Storage mode (default: both)
      --scenario NAME|all             Comma-separated sequential scenarios (default: all)
                                      concurrent_point_read and multi_writer are opt-in
                                      catalog-path scenarios. Sequential names:
                                      bulk_write, point_read, changes_read,
                                      index_build, indexed_query, fts_query,
                                      fts_rebuild
      --iterations N                  Measured samples (default: 15)
      --warmup N                      Warmup samples excluded from results (default: 3)
      --dataset N                     Seeded documents (default: 500)
      --batch N                       Bulk/changes batch size (default: 100, max: 500)
      --reads N                       Point reads per measured sample (default: 100)
      --output PATH                   JSON report path (default: output/benchmarks/...json)
      --baseline PATH                 Compare median latency against a prior report
      --max-regression PCT            Allowed median regression (default: 20)

    Environment equivalents:
      VIALKEEPER_BENCH_ITERATIONS, VIALKEEPER_BENCH_WARMUP,
      VIALKEEPER_BENCH_DATASET, VIALKEEPER_BENCH_BATCH, VIALKEEPER_BENCH_READS,
      VIALKEEPER_BENCH_MAX_REGRESSION_PCT
    """
  end
end

VialKeeper.Benchmarks.Runner.main(System.argv())
