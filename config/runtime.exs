import Config

# Runtime configuration for Mix environments and assembled OTP releases.
#
# Host configuration is loaded from `<database_root>/host.toml` — a single
# TOML file co-located with the database files (CONFIG-001). The
# `ELIXIR_DB_ROOT` environment variable locates the database root; on first run
# in an empty root the file is created from a shipped template and then never
# overwritten. Production hosts run the release binary; Mix is for development
# and CI only.
#
# In the test environment this is a no-op: configuration is owned by
# config/test.exs and the HostConfig loader returns an empty keyword list.

# In the test environment, configuration is owned by config/test.exs; do not
# read or create host.toml (mirrors the existing OTLP test guard below).
host_config =
  if config_env() == :test do
    []
  else
    case ElixirDB.HostConfig.load() do
      {:ok, kw} ->
        kw

      {:error, reason} ->
        raise "host configuration error: #{reason}"
    end
  end

config :elixir_db, host_config

# OpenTelemetry opt-in gate (OBSV-004). The OTLP exporter and metric reader
# are wired ONLY when an otlp_endpoint is configured in host.toml. Otherwise no
# exporter is configured and no network connection to any collector is
# attempted. The Observability.Supervisor still starts the (no-op) tracer/meter
# providers so instrumentation calls are safe no-ops.
#
# This gate is skipped in the test environment so test.exs can wire the
# in-memory TestExporter without runtime.exs clobbering it.
if config_env() != :test do
  otlp_endpoint = Keyword.get(host_config, :otlp_endpoint)

  if otlp_endpoint not in [nil, ""] do
    config :opentelemetry_exporter,
      otlp_protocol: :http_protobuf,
      otlp_endpoint: otlp_endpoint

    config :opentelemetry,
      traces_exporter: {:opentelemetry_exporter, %{}}

    config :opentelemetry_experimental,
      readers: [
        %{
          id: :elixir_db_otlp_metric_reader,
          module: :otel_metric_reader,
          config: %{
            exporter: {:opentelemetry_exporter, %{}},
            export_interval_ms: 30_000
          }
        }
      ]
  else
    config :opentelemetry, traces_exporter: :none
    config :opentelemetry_experimental, readers: []
  end
end
