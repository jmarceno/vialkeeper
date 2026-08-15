defmodule VialKeeper.Observability.TelemetryBridgeTest do
  @moduledoc """
  Trigger a Req call; assert the Finch child span appears under the
  current span. Also asserts the bridge handlers are defensive: malformed
  telemetry metadata must never crash the emitting process (`:telemetry`
  propagates handler exceptions into the emitter).
  """

  use VialKeeper.Observability.OtelCase, async: false

  @moduletag :integration

  require OpenTelemetry.Tracer

  alias VialKeeper.Eventual
  alias VialKeeper.Observability.TestExporter
  alias VialKeeper.TestServer

  test "a Req call emits a finch.request child span under the current span" do
    server = TestServer.start_supervised!()

    OpenTelemetry.Tracer.with_span "bridge-parent" do
      assert {:ok, %{status: 200}} = Req.get(server.base_url <> "/v1/databases")
    end

    {parent, finch_spans} =
      Eventual.eventually(
        fn ->
          case {TestExporter.spans_named("bridge-parent"),
                TestExporter.spans_named("finch.request")} do
            {[parent], [_ | _] = finch_spans} -> {:ok, {parent, finch_spans}}
            _ -> false
          end
        end,
        message: "bridge parent and child spans were not exported"
      )

    for span <- finch_spans do
      assert span[:trace_id] == parent[:trace_id],
             "finch span must share the parent trace"

      assert span[:parent_span_id] == parent[:span_id],
             "finch span must be a child of the current span"

      # The duration attribute goes through the central allow-list (§3.1).
      assert TestExporter.span_attr(span, :"finch.duration") != nil
    end
  end

  test "bridge handlers never crash the emitting process on malformed events" do
    # If any handler raised, :telemetry would propagate the exception into
    # THIS process and the test would fail. All of these shapes must be
    # absorbed silently.
    :telemetry.execute([:finch, :request, :start], %{}, %{request: :bogus})
    :telemetry.execute([:finch, :request, :stop], %{}, %{})
    :telemetry.execute([:finch, :request, :start], %{}, %{})
    :telemetry.execute([:finch, :request, :stop], %{duration: "not-a-number"}, %{})
    :telemetry.execute([:finch, :request, :exception], %{}, %{kind: :error, reason: :boom})
    :telemetry.execute([:finch, :request, :exception], %{}, %{})

    # The one well-formed start/stop pair above still produced a span.
    Eventual.eventually(
      fn -> TestExporter.spans_named("finch.request") != [] end,
      message: "well-formed finch span was not exported"
    )
  end
end
