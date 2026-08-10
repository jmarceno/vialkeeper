import Config

test_registration_manifest =
  Path.join(
    System.tmp_dir!(),
    "elixirdb-test-registrations-#{System.system_time(:microsecond)}-#{System.unique_integer([:positive])}.json"
  )

# In the test environment runtime.exs is a no-op (HostConfig.load/0 returns
# {:ok, []}), so test.exs owns the host configuration keys directly. These
# mirror the floor defaults shipped in priv/host.toml.
config :elixir_db,
  database_root: Path.expand("tmp/test-databases", File.cwd!()),
  registration_manifest: test_registration_manifest,
  shutdown_timeout: 30_000,
  host_limits: [
    max_document_bytes: 1_048_576,
    max_request_bytes: 2_097_152,
    max_document_id_bytes: 512,
    max_bulk_operations: 500,
    max_query_results: 500,
    max_changes_batch: 500,
    max_replication_batch_documents: 500,
    max_replication_batch_bytes: 16_777_216,
    max_replication_attempts: 32,
    max_replication_delay_ms: 300_000,
    max_full_scan_documents: 1_000,
    max_query_execution_ms: 5_000,
    max_wait_ms: 30_000,
    max_open_databases: 64,
    max_replication_workers: 32,
    max_replication_concurrent_chain_fetches: 32,
    max_replication_concurrent_blob_transfers: 32,
    max_replication_transfer_bytes_in_flight: 4_294_967_296,
    admission_limit: 128,
    max_json_nesting_depth: 100,
    max_attachment_bytes: 4_294_967_296,
    max_concurrent_attachment_reads: 1024,
    max_concurrent_attachment_writes: 256,
    max_query_subscriptions: 4_096,
    max_query_subscription_members: 10_000,
    max_query_subscription_buffered_events: 4_096,
    max_query_subscription_heartbeat_ms: 300_000,
    max_peer_expiry_ms: 604_800_000,
    max_retention_schedule_ms: 604_800_000
  ],
  listener: [ip: {127, 0, 0, 1}, port: 0],
  admission_policy: [
    foreground_weight: 8,
    subscription_weight: 4,
    replication_weight: 2,
    maintenance_weight: 1,
    foreground_reserved_slots: 1,
    subscription_reserved_slots: 1,
    replication_reserved_slots: 1,
    maintenance_reserved_slots: 1
  ],
  auth: [enabled: false, token_digests: []],
  tls: [enabled: false],
  security: [allow_insecure_remote: false],
  otlp_endpoint: ""

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
