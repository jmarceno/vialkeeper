defmodule VialKeeper.Observability.HTTPUntracedTest do
  @moduledoc """
  With no span exporter configured, an HTTP request starts no span but still
  carries an inbound caller's trace context while it runs, and restores the
  prior context afterwards.
  """

  # Changes the global span exporter setting, so this module does not run async.
  use VialKeeper.Observability.OtelCase, async: false

  alias VialKeeper.Observability.Instrumentation.HTTP
  alias VialKeeper.Observability.TestExporter

  @trace_id_hex "0af7651916cd43dd8448eb211c80319c"
  @parent_span_hex "b7ad6b7169203331"

  setup do
    previous = Application.fetch_env(:opentelemetry, :traces_exporter)
    Application.put_env(:opentelemetry, :traces_exporter, :none)

    on_exit(fn ->
      case previous do
        {:ok, value} -> Application.put_env(:opentelemetry, :traces_exporter, value)
        :error -> Application.delete_env(:opentelemetry, :traces_exporter)
      end
    end)
  end

  test "no request span is started and the caller's context is carried" do
    conn =
      Plug.Test.conn(:get, "/v1/databases")
      |> Plug.Conn.put_req_header("traceparent", "00-#{@trace_id_hex}-#{@parent_span_hex}-01")

    parent = self()

    conn =
      HTTP.wrap(conn, fn conn ->
        send(parent, {:span_during_request, OpenTelemetry.Tracer.current_span_ctx()})
        Plug.Conn.send_resp(conn, 200, "{}")
      end)

    assert conn.status == 200
    assert_receive {:span_during_request, span_ctx}
    assert OpenTelemetry.Span.trace_id(span_ctx) == String.to_integer(@trace_id_hex, 16)
    assert OpenTelemetry.Span.span_id(span_ctx) == String.to_integer(@parent_span_hex, 16)
    refute OpenTelemetry.Span.is_recording(span_ctx)

    assert TestExporter.spans_named("vial_keeper.http.request") == []
    assert OpenTelemetry.Tracer.current_span_ctx() == :undefined
  end
end
