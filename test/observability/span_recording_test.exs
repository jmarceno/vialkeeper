defmodule ElixirDB.Observability.SpanRecordingTest do
  @moduledoc false
  use ElixirDB.Observability.OtelCase, async: false

  alias ElixirDB.Observability.{OtelCase, TestExporter}

  test "a span created in the test process is recorded after flush" do
    require OpenTelemetry.Tracer

    OpenTelemetry.Tracer.with_span "test-direct-span" do
      :ok
    end

    OtelCase.flush()

    spans = TestExporter.spans_named("test-direct-span")

    assert length(spans) == 1,
           "expected the direct span to be recorded, got: #{inspect(TestExporter.spans() |> Enum.map(& &1[:name]))}"
  end
end
