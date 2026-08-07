import Config

test_registration_manifest =
  Path.join(
    System.tmp_dir!(),
    "elixirdb-test-registrations-#{System.system_time(:microsecond)}-#{System.unique_integer([:positive])}.json"
  )

config :elixir_db,
  database_root: Path.expand("tmp/test-databases", File.cwd!()),
  registration_manifest: test_registration_manifest,
  listener: [ip: {127, 0, 0, 1}, port: 0]

# Observability: never export over the network in tests. Wire the SDK to use
# the simple (synchronous) span processor with the in-memory TestExporter so
# spans are recorded immediately and assertable. traces_exporter is read by
# the simple processor config merge and mapped to its `exporter` key.
config :opentelemetry,
  traces_exporter: {:"Elixir.ElixirDB.Observability.TestExporter", []},
  processors: [
    {:otel_simple_processor, %{}}
  ]

# Metrics: a periodic reader exporting every 50ms into the in-memory
# TestMetricExporter so metric assertions can poll for datapoints. Never any
# network.
config :opentelemetry_experimental,
  readers: [
    %{
      id: :elixir_db_test_metric_reader,
      module: :otel_metric_reader,
      config: %{
        exporter: {:"Elixir.ElixirDB.Observability.TestMetricExporter", []},
        export_interval_ms: 50
      }
    }
  ]
