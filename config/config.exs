import Config

config :elixir_db,
  database_root: Path.expand("data", File.cwd!()),
  registration_manifest: nil,
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
    admission_limit: 128,
    max_json_nesting_depth: 100
  ],
  listener: [ip: {127, 0, 0, 1}, port: 4000]

config :logger, :console, format: "[$level] $message\n"

# OpenTelemetry resource identity and span batch processor tuning only (plan
# §2.2). The OTLP endpoint is intentionally NOT set here — a hardcoded endpoint
# would risk a network attempt on misconfiguration and break the "no network
# when unconfigured" guarantee (OBSV-004). The exporter is wired exclusively by
# config/runtime.exs when ELIXIRDB_OTLP_ENDPOINT is present.
config :opentelemetry, :resource, service: %{name: "elixir_db", version: "0.1.0"}

# Batch processor tuning (plan §2.2; the key names are the SDK's actual ones:
# scheduled_delay_ms / max_queue_size / exporting_timeout_ms). The values equal
# the SDK defaults — declared explicitly so the operational contract is visible.
# test.exs overrides :processors with the synchronous simple processor.
config :opentelemetry, :processors,
  otel_batch_processor: %{
    scheduled_delay_ms: 5_000,
    max_queue_size: 2_048,
    exporting_timeout_ms: 30_000
  }

# Default: no exporter wired. runtime.exs enables OTLP export only when
# ELIXIRDB_OTLP_ENDPOINT is set. Per-env config (test.exs) may override to wire
# a test exporter.
config :opentelemetry_experimental, readers: []

import_config "#{config_env()}.exs"
