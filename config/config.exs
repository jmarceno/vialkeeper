import Config

# Host configuration defaults (database root, listener, limits, admission
# policy, auth, tls, observability) live in `VialKeeper.HostConfig` and are
# loaded from `<database_root>/host.toml` at runtime by config/runtime.exs.

config :logger, :console,
  format: "[$level] $message\n",
  metadata: [:database_uuid, :kind, :reason]

# OpenTelemetry resource identity only. The OTLP endpoint is intentionally NOT
# set here — a hardcoded endpoint would risk a network attempt on
# misconfiguration and break the "no network when unconfigured" guarantee
# (OBSV-004). The exporter and span processors are wired by config/runtime.exs
# (batch processor in Mix/release environments) and config/test.exs (synchronous
# simple processor). Mix deep-merges keyword-list `:processors` values, so this
# file must not declare a batch processor or tests would run both processors.
config :opentelemetry, :resource, service: %{name: "vial_keeper", version: "0.1.0"}

# Default: no exporter wired. runtime.exs enables OTLP export only when an
# otlp_endpoint is set in host.toml. Per-env config (test.exs) may override to
# wire a test exporter.
config :opentelemetry_experimental, readers: []

# Post-compact attachment GC is invoked from the runtime owner via configured MFA
# so Reach layers stay acyclic (runtime must not depend on the application facade).
config :vial_keeper, :attachment_gc_module, VialKeeper.Attachments

# Default physical backend. Runtime selects through VialKeeper.Storage.Registry;
# tests may swap this for VialKeeper.Storage.Sentinel.Adapter.
config :vial_keeper, :storage_backend, VialKeeper.Storage.SQLite.Adapter

import_config "#{config_env()}.exs"
