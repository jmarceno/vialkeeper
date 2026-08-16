defmodule VialKeeper.Observability.TelemetryBridge do
  @moduledoc """
  Bridges dependency `:telemetry` events into OpenTelemetry spans.

  Finch (`[:finch, :request, *]`) emits `:telemetry` events. Rather than
  re-instrument the library, this GenServer attaches a handler that opens and
  closes an OTel client span under the current trace context, giving outbound
  HTTP client timing (replication requests) "for free" as child spans under the
  replication spans.

  Bandit inbound requests are intentionally NOT bridged:
  `VialKeeper.Observability.Instrumentation.HTTP` already starts one server span
  per request, and bridging Bandit too would double-span every request.

  Attach on `init`, detach on `terminate`. Finch emits start/stop/exception in
  the calling process, so the started span context is stashed in the process
  dictionary and ended by its own context on stop/exception (`start_span` does
  not make the new span current; ending "the current span" could end the wrong
  span). Handlers are fully defensive: `:telemetry` propagates handler
  exceptions into the emitting process, so nothing here may raise.
  """

  use GenServer

  require OpenTelemetry.Tracer

  alias OpenTelemetry.Span, as: OtelSpan
  alias OpenTelemetry.Tracer, as: OtelTracer
  alias VialKeeper.Observability.Attributes

  @handler_id __MODULE__.Finch

  @finch_events [
    [:finch, :request, :start],
    [:finch, :request, :stop],
    [:finch, :request, :exception]
  ]

  # Process-dictionary slot holding the span ctx started by the :start event.
  # Finch requests are synchronous per process, so one slot suffices.
  @span_key {__MODULE__, :finch_span}

  @spec start_link(term()) :: GenServer.on_start()
  def start_link(_), do: GenServer.start_link(__MODULE__, [], name: __MODULE__)

  @impl true
  def init(_) do
    _ = :telemetry.attach_many(@handler_id, @finch_events, &__MODULE__.handle_event/4, nil)
    {:ok, %{handler_id: @handler_id}}
  end

  @impl true
  def terminate(_reason, %{handler_id: id}) do
    _ = :telemetry.detach(id)
    :ok
  end

  @doc false
  def handle_event(event, measurements, metadata, _config) do
    dispatch(event, measurements, metadata)
  catch
    # Never leak an observability fault into the emitting (request) process.
    _, _ -> :ok
  end

  defp dispatch([:finch, :request, :start], _measurements, _metadata) do
    # Constant span name: the remote host/scheme/port stay OUT of the name
    # (§3.1 forbids full remote URLs in telemetry; bounded cardinality).
    span_ctx = OtelTracer.start_span("finch.request", %{kind: :client})
    Process.put(@span_key, span_ctx)
    :ok
  end

  defp dispatch([:finch, :request, :stop], measurements, _metadata) do
    case Process.delete(@span_key) do
      nil -> :ok
      span_ctx -> finish_span(span_ctx, measurements)
    end
  end

  defp dispatch([:finch, :request, :exception], _measurements, _metadata) do
    case Process.delete(@span_key) do
      nil -> :ok
      span_ctx -> finish_span_exception(span_ctx)
    end
  end

  defp dispatch(_event, _measurements, _metadata), do: :ok

  defp finish_span(span_ctx, measurements) do
    if OtelSpan.is_recording(span_ctx) do
      attrs =
        case measurements[:duration] do
          nil -> %{}
          # Routed through the central allow-list (§3.1): a bounded numeric
          # measurement, never customer data.
          duration -> Attributes.build(finch_duration: duration)
        end

      _ = OtelSpan.set_attributes(span_ctx, attrs)
      _ = OtelSpan.end_span(span_ctx)
      :ok
    end

    :ok
  end

  defp finish_span_exception(span_ctx) do
    if OtelSpan.is_recording(span_ctx) do
      try do
        # Generic message only: exception reasons can carry request detail.
        _ =
          OtelSpan.record_exception(
            span_ctx,
            %RuntimeError{message: "finch request failed"},
            []
          )

        _ = OtelSpan.set_status(span_ctx, :opentelemetry.status(:error))
      catch
        _, _ -> :ok
      end

      _ = OtelSpan.end_span(span_ctx)
      :ok
    end

    :ok
  end
end
