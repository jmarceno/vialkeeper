import Config

# Host configuration defaults (database root, listener, limits, admission
# policy, auth, tls, observability) live in `ElixirDB.HostConfig` and are
# loaded from `<database_root>/host.toml` at runtime by config/runtime.exs.

config :logger, :console,
  format: "[$level] $message\n",
  metadata: [:database_uuid, :kind, :reason]

# OpenTelemetry resource identity and span batch processor tuning only. The OTLP
# endpoint is intentionally NOT set here — a hardcoded endpoint
# would risk a network attempt on misconfiguration and break the "no network
# when unconfigured" guarantee (OBSV-004). The exporter is wired exclusively by
# config/runtime.exs when an otlp_endpoint is present in host.toml.
config :opentelemetry, :resource, service: %{name: "elixir_db", version: "0.1.0"}

# Batch processor tuning; the key names are the SDK's actual ones:
# scheduled_delay_ms / max_queue_size / exporting_timeout_ms). The values equal
# the SDK defaults — declared explicitly so the operational contract is visible.
# test.exs overrides :processors with the synchronous simple processor.
config :opentelemetry, :processors,
  otel_batch_processor: %{
    scheduled_delay_ms: 5_000,
    max_queue_size: 2_048,
    exporting_timeout_ms: 30_000
  }

# Default: no exporter wired. runtime.exs enables OTLP export only when an
# otlp_endpoint is set in host.toml. Per-env config (test.exs) may override to
# wire a test exporter.
config :opentelemetry_experimental, readers: []

# Post-compact attachment GC is invoked from the runtime owner via configured MFA
# so Reach layers stay acyclic (runtime must not depend on the application facade).
config :elixir_db, :attachment_gc_module, ElixirDB.Attachments

import_config "#{config_env()}.exs"
