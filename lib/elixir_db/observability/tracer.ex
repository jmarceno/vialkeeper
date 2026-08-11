defmodule ElixirDB.Observability.Tracer do
  @moduledoc """
  Thin tracing wrappers around the OpenTelemetry API.

  Centralizes span creation so instrumentation sites stay small and the span
  names match the observability catalog. All span
  attributes pass through `ElixirDB.Observability.Attributes.build/1` so the
  allow-list is enforced at every emission site.
  """

  require OpenTelemetry.Tracer

  alias ElixirDB.Observability.Attributes

  @doc """
  Runs `fun` within a span named `name` with allow-listed `attrs` as span
  attributes. The span kind defaults to `:internal`; pass `kind:` to override.

  Returns the result of `fun`. The span is ended when `fun` returns or raises.
  """
  @spec with_span(binary(), keyword(), (-> term())) :: term()
  def with_span(name, attrs, fun) when is_binary(name) and is_list(attrs) and is_function(fun, 0) do
    if tracing_enabled?() do
      kind = Keyword.get(attrs, :kind, :internal)
      start_attrs = Attributes.build(Keyword.delete(attrs, :kind))

      OpenTelemetry.Tracer.with_span name, %{kind: kind, attributes: start_attrs} do
        fun.()
      end
    else
      fun.()
    end
  end

  @doc "Returns whether OpenTelemetry span export is configured."
  @spec tracing_enabled?() :: boolean()
  def tracing_enabled?,
    do: Application.get_env(:opentelemetry, :traces_exporter, :none) not in [:none, nil]

  @doc "Sets an allow-listed attribute on the current span."
  @spec set_attributes(keyword()) :: boolean()
  def set_attributes(attrs) when is_list(attrs) do
    if api_available?() do
      OpenTelemetry.Tracer.set_attributes(Attributes.build(attrs))
    else
      false
    end
  end

  @doc """
  Applies the error-to-span status policy:

    * `:internal_error` and the adapter `normalize_error` fallback → status ERROR
    * all other registered domain errors → status UNSET (rely on `error.code`)

  Returns the input unchanged for pipelining.
  """
  @spec apply_error_status(ElixirDB.Error.t() | nil) :: ElixirDB.Error.t() | nil
  def apply_error_status(nil), do: nil

  def apply_error_status(%ElixirDB.Error{code: :internal_error} = error) do
    OpenTelemetry.Span.set_status(
      OpenTelemetry.Tracer.current_span_ctx(),
      :opentelemetry.status(:error)
    )

    error
  end

  def apply_error_status(%ElixirDB.Error{} = error), do: error

  @doc "Records the stable error code atom on the current span (never the message)."
  @spec record_error(ElixirDB.Error.t() | nil) :: :ok
  def record_error(nil), do: :ok

  def record_error(%ElixirDB.Error{code: code}) do
    _ = set_attributes(error_code: code)
    :ok
  end

  @doc """
  Returns the current span context for propagation across process boundaries
  (e.g. into a `Task.Supervisor` phase task). Returns `:undefined` when no SDK.
  """
  @spec current_span_ctx() :: term()
  def current_span_ctx, do: OpenTelemetry.Tracer.current_span_ctx()

  @doc "Injects the current trace context into a carrier (list of `{name, value}`)."
  @spec inject(term()) :: term()
  def inject(carrier) do
    if api_available?() do
      :otel_propagator_text_map.inject(carrier)
    else
      carrier
    end
  end

  @doc "Extracts trace context from a carrier (list of `{name, value}`)."
  @spec extract(term()) :: term() | nil
  def extract(carrier) do
    if api_available?() do
      :otel_propagator_text_map.extract(carrier)
    else
      nil
    end
  end

  defp api_available? do
    Code.ensure_loaded?(:otel_propagator_text_map) and
      function_exported?(:otel_propagator_text_map, :extract, 1)
  end
end
