defmodule VialKeeper.Benchmarks.ConcurrentWrites do
  @moduledoc """
  Concurrent document writes per storage engine.

  Drives `VialKeeper.Documents.put/2` through the catalog, as the HTTP path
  does, from 8 and 16 concurrent clients and reports throughput and put
  latency (p50, p99) for each engine. Every put creates a new document, so
  clients never contend on one document.

  Run with `mix bench.concurrent_writes`. Options:

    * `--engine turso|sqlite` (repeatable; default both)
    * `--clients N` (repeatable; default 8 and 16)
    * `--puts N` total puts per run (default 20000)
  """

  alias VialKeeper.Documents
  alias VialKeeper.Runtime.DatabaseCatalog

  @engines %{
    "sqlite" => VialKeeper.Storage.SQLite.Adapter,
    "turso" => VialKeeper.Storage.Turso.Adapter
  }

  @doc false
  @spec main([binary()]) :: :ok
  def main(argv) do
    {options, _rest, _invalid} =
      OptionParser.parse(argv, strict: [engine: :keep, clients: :keep, puts: :integer])

    engines = Keyword.get_values(options, :engine) |> default_to(["sqlite", "turso"])

    clients =
      Keyword.get_values(options, :clients)
      |> Enum.map(&String.to_integer/1)
      |> default_to([8, 16])

    puts = Keyword.get(options, :puts, 20_000)
    Logger.configure(level: :warning)

    IO.puts("engine  clients  puts    tx/s      p50_ms   p99_ms")

    for engine <- engines, client_count <- clients do
      result = run(engine, client_count, puts)

      IO.puts(
        :io_lib.format("~-7s ~7B  ~6B  ~8.1f  ~7.3f  ~7.3f", [
          engine,
          client_count,
          puts,
          result.tx_per_s,
          result.p50_ms,
          result.p99_ms
        ])
      )
    end

    :ok
  end

  defp default_to([], default), do: default
  defp default_to(values, _default), do: values

  defp run(engine, client_count, puts) do
    with_runtime(Map.fetch!(@engines, engine), client_count, fn uuid ->
      per_client = div(puts, client_count)
      started = System.monotonic_time()

      latencies =
        1..client_count
        |> Enum.map(fn client -> Task.async(fn -> put_many(uuid, client, per_client) end) end)
        |> Enum.flat_map(&Task.await(&1, :infinity))

      elapsed_s =
        System.convert_time_unit(System.monotonic_time() - started, :native, :microsecond) / 1.0e6

      sorted = Enum.sort(latencies)

      %{
        tx_per_s: length(sorted) / elapsed_s,
        p50_ms: percentile(sorted, 0.50),
        p99_ms: percentile(sorted, 0.99)
      }
    end)
  end

  defp put_many(uuid, client, count) do
    for n <- 1..count do
      started = System.monotonic_time()

      {:ok, _} =
        Documents.put(uuid, %{id: "c#{client}-#{n}", body: %{"client" => client, "n" => n}})

      System.convert_time_unit(System.monotonic_time() - started, :native, :microsecond) / 1000
    end
  end

  defp percentile(sorted, fraction) do
    index = min(length(sorted) - 1, trunc(fraction * length(sorted)))
    Enum.at(sorted, index)
  end

  defp with_runtime(backend, client_count, fun) do
    root =
      Path.join(System.tmp_dir!(), "vialkeeper-concurrent-#{System.unique_integer([:positive])}")

    File.mkdir_p!(root)
    limits = Application.get_env(:vial_keeper, :host_limits, [])

    Application.put_env(:vial_keeper, :storage_backend, backend)
    Application.put_env(:vial_keeper, :database_root, root)
    Application.put_env(:vial_keeper, :registration_manifest, Path.join(root, "registrations.json"))
    Application.put_env(:vial_keeper, :listener, ip: {127, 0, 0, 1}, port: 0)

    Application.put_env(
      :vial_keeper,
      :host_limits,
      limits
      |> Keyword.put(:writer_pool_size, min(client_count, 16))
      |> Keyword.put(:write_queue_limit, max(128, client_count * 4))
    )

    {:ok, _started} = Application.ensure_all_started(:vial_keeper)

    try do
      {:ok, identity} = DatabaseCatalog.create("bench.vialkeeper")
      {:ok, _} = DatabaseCatalog.open(identity.database_uuid)
      fun.(identity.database_uuid)
    after
      :ok = Application.stop(:vial_keeper)
      Application.put_env(:vial_keeper, :host_limits, limits)
      File.rm_rf(root)
    end
  end
end

VialKeeper.Benchmarks.ConcurrentWrites.main(System.argv())
