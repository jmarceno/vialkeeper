import Config

config :elixir_db,
  database_root: Path.expand("data", File.cwd!()),
  registration_manifest: nil,
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
    admission_limit: 128
  ],
  listener: [ip: {127, 0, 0, 1}, port: 4000]

config :logger, :console, format: "[$level] $message\n"

import_config "#{config_env()}.exs"
