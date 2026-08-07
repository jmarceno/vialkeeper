defmodule ElixirDB.Observability.OtelCase do
  @moduledoc """
  ExUnit case for observability tests.

  The OpenTelemetry SDK is started by `Observability.Supervisor` with the simple
  (synchronous) span processor wired to the in-memory `TestExporter` via test
  config. Spans are therefore recorded synchronously on end — no flush needed.
  This case clears the recorder between tests so assertions are isolated.
  """

  use ExUnit.CaseTemplate

  using do
    quote do
      alias ElixirDB.Observability.{OtelCase, TestExporter}
    end
  end

  setup _tags do
    ElixirDB.Observability.TestExporter.start()
    ElixirDB.Observability.TestExporter.reset()
    ElixirDB.Observability.TestMetricExporter.start()
    ElixirDB.Observability.TestMetricExporter.reset()

    on_exit(fn ->
      ElixirDB.Observability.TestExporter.reset()
      ElixirDB.Observability.TestMetricExporter.reset()
    end)

    :ok
  end

  @doc "No-op kept for callers; the simple processor exports synchronously."
  def flush(_timeout_ms \\ 0), do: :ok
end
