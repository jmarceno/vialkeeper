import Config

# Runtime configuration for Mix environments and assembled OTP releases.
# Production hosts run the release binary; Mix is only for development and CI.

database_root =
  case System.get_env("ELIXIR_DB_ROOT") do
    value when is_binary(value) and value != "" ->
      Path.expand(value)

    _ ->
      if config_env() == :prod do
        raise """
        environment variable ELIXIR_DB_ROOT is missing.

        Set it to the absolute directory that holds database files and the
        registration manifest, for example:

            export ELIXIR_DB_ROOT=/var/lib/elixirdb
        """
      else
        nil
      end
  end

if database_root do
  config :elixir_db, database_root: database_root
end

if manifest = System.get_env("ELIXIR_DB_REGISTRATION_MANIFEST") do
  config :elixir_db, registration_manifest: Path.expand(manifest)
end

if timeout = System.get_env("ELIXIR_DB_SHUTDOWN_TIMEOUT_MS") do
  config :elixir_db, shutdown_timeout: String.to_integer(timeout)
end

listener_ip =
  case System.get_env("ELIXIR_DB_IP") do
    nil ->
      {127, 0, 0, 1}

    value ->
      case :inet.parse_address(String.to_charlist(value)) do
        {:ok, ip} ->
          ip

        {:error, _} ->
          raise "ELIXIR_DB_IP must be an IPv4 or IPv6 address, got: #{inspect(value)}"
      end
  end

listener_port =
  case System.get_env("ELIXIR_DB_PORT") do
    nil -> 4000
    value -> String.to_integer(value)
  end

if System.get_env("ELIXIR_DB_IP") || System.get_env("ELIXIR_DB_PORT") do
  config :elixir_db, listener: [ip: listener_ip, port: listener_port]
end

# OpenTelemetry opt-in gate. The OTLP exporter and metric reader are wired ONLY
# when ELIXIRDB_OTLP_ENDPOINT is present. Otherwise no exporter is configured
# and no network connection to any collector is attempted (OBSV-004). The
# Observability.Supervisor still starts the (no-op) tracer/meter providers so
# instrumentation calls are safe no-ops.
#
# This gate is skipped in the test environment so test.exs can wire the
# in-memory TestExporter without runtime.exs clobbering it.
if config_env() != :test do
  if System.get_env("ELIXIRDB_OTLP_ENDPOINT") do
    config :opentelemetry_exporter,
      otlp_protocol: :http_protobuf,
      otlp_endpoint: System.fetch_env!("ELIXIRDB_OTLP_ENDPOINT")

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
