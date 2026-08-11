defmodule ElixirDB.Observability.Instrumentation.DerivedView do
  @moduledoc "Bounded tracing and metrics for materialized-view batches."

  alias ElixirDB.Error
  alias ElixirDB.Observability.{Meters, Tracer}

  @span "elixir_db.derived_view.batch"
  @duration_metric :"elixir_db.derived_view.batch.duration"
  @max_count 10_000
  @max_source_count 256

  @doc "Runs one derived materializer batch with bounded outcome telemetry."
  @spec batch(binary(), non_neg_integer(), (-> term())) :: term()
  def batch(uuid, source_count, fun)
      when is_binary(uuid) and is_integer(source_count) and source_count >= 0 and
             is_function(fun, 0) do
    started = System.monotonic_time()
    base_attrs = [db_uuid: uuid, derived_source_count: min(source_count, @max_source_count)]

    Tracer.with_span(@span, base_attrs, fn ->
      result = fun.()
      result_attrs = result_attrs(result)
      attrs = base_attrs ++ result_attrs

      _ = Tracer.set_attributes(result_attrs)
      Meters.record(@duration_metric, System.monotonic_time() - started, attrs)
      record_error(result)
      result
    end)
  end

  defp result_attrs({:ok, result}) when is_map(result) do
    [
      derived_document_count: bounded_count(result, [:changed_rows, :changed_documents]),
      derived_group_count: bounded_count(result, [:changed_groups])
    ]
  end

  defp result_attrs({:error, %Error{code: code}}), do: [outcome: :rejected, error_code: code]
  defp result_attrs(_result), do: [outcome: :ok]

  defp bounded_count(result, keys) do
    Enum.find_value(keys, 0, fn key ->
      case Map.get(result, key, Map.get(result, Atom.to_string(key))) do
        value when is_integer(value) and value >= 0 -> min(value, @max_count)
        _ -> nil
      end
    end)
  end

  defp record_error({:error, %Error{} = error}) do
    _ = Tracer.record_error(error)
    _ = Tracer.apply_error_status(error)
    :ok
  end

  defp record_error(_result), do: :ok
end
