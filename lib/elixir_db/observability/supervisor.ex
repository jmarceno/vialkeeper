defmodule ElixirDB.Observability.Supervisor do
  @moduledoc """
  Owns the OpenTelemetry SDK lifecycle.

  The OpenTelemetry deps are declared `runtime: false`, so they do NOT
  auto-start with the VM. This supervisor starts them explicitly:

    * the `:opentelemetry` SDK app (tracer provider + span processors)
    * the `:opentelemetry_experimental` app (meter provider + metric reader)
    * the `:telemetry` bridge (Bandit/Finch → OTel child spans)

  When no OTLP endpoint is configured (`traces_exporter: :none`, empty
  `readers`), the SDK still starts but wires no exporter: spans are no-ops over
  a no-op tracer provider and no network connection is attempted (OBSV-004,
  OBSV-007). In tests, the tracer provider is configured with the in-memory
  `ElixirDB.Observability.TestExporter` so spans are real and assertable.
  """

  use Supervisor

  require Logger

  def start_link(_args), do: Supervisor.start_link(__MODULE__, [], name: __MODULE__)

  @impl true
  def init(_) do
    ensure_started(:opentelemetry_api)
    ensure_started(:opentelemetry)
    ensure_started(:opentelemetry_experimental)

    children = [
      # Attaches the Bandit/Finch :telemetry bridge handlers; detaches on shutdown.
      {ElixirDB.Observability.TelemetryBridge, []}
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end

  defp ensure_started(app) do
    case Application.ensure_all_started(app) do
      {:ok, _} ->
        :ok

      {:error, reason} ->
        # Deps are runtime: false; if an app can't start we must not crash the
        # whole node. Log loudly and degrade to no-op instrumentation.
        Logger.warning(
          "OpenTelemetry app #{app} failed to start: #{inspect(reason)}; " <>
            "instrumentation will be no-ops"
        )

        :ok
    end
  end
end
