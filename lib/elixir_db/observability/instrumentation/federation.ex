defmodule ElixirDB.Observability.Instrumentation.Federation do
  @moduledoc "Bounded tracing and metrics for federation queries."

  alias ElixirDB.Error
  alias ElixirDB.Observability.{Meters, Tracer}

  @span "elixir_db.federation.query"
  @duration_metric :"elixir_db.federation.query.duration"
  @max_source_count 256

  @doc "Runs a federation query with bounded source-count telemetry."
  @spec query(non_neg_integer() | nil, (-> term())) :: term()
  def query(source_count, fun)
      when ((is_integer(source_count) and source_count >= 0) or is_nil(source_count)) and
             is_function(fun, 0) do
    started = System.monotonic_time()
    base_attrs = source_attrs(source_count)

    Tracer.with_span(@span, base_attrs, fn ->
      result = fun.()
      result_attrs = result_attrs(result)

      _ = Tracer.set_attributes(result_attrs)
      Meters.record(@duration_metric, System.monotonic_time() - started, base_attrs ++ result_attrs)
      _ = record_error(result)

      result
    end)
  end

  defp source_attrs(nil), do: []

  defp source_attrs(source_count),
    do: [federation_source_count: min(source_count, @max_source_count)]

  defp result_attrs({:error, %Error{code: code}}),
    do: [outcome: :rejected, error_code: code]

  defp result_attrs(_result), do: [outcome: :ok]

  defp record_error({:error, %Error{} = error}) do
    _ = Tracer.record_error(error)
    _ = Tracer.apply_error_status(error)
    :ok
  end

  defp record_error(_result), do: :ok
end
