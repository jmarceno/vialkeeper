defmodule VialKeeper.Observability.SpanRecordingTest do
  @moduledoc "Covers direct span recording and exporter flushing in tests."

  use VialKeeper.Observability.OtelCase, async: false

  @moduletag :integration

  alias VialKeeper.Observability.{OtelCase, TestExporter}

  test "a span created in the test process is recorded after flush" do
    require OpenTelemetry.Tracer

    OpenTelemetry.Tracer.with_span "test-direct-span" do
      :ok
    end

    OtelCase.flush()

    spans = TestExporter.spans_named("test-direct-span")

    assert [_] = spans,
           "expected the direct span to be recorded, got: #{inspect(TestExporter.spans() |> Enum.map(& &1[:name]))}"
  end

  test "test env runs only the synchronous simple span processor" do
    names = Enum.map(Process.registered(), &Atom.to_string/1)

    refute Enum.any?(names, &String.starts_with?(&1, "otel_batch_processor")),
           "batch processor must not run in tests: #{inspect(names)}"

    assert Enum.any?(names, &String.starts_with?(&1, "otel_simple_processor"))
  end
end
