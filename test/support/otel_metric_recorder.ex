defmodule ElixirDB.Observability.TestMetricExporter do
  @moduledoc """
  In-memory metric exporter for tests. Implements the exporter callbacks the
  `otel_metric_reader` invokes (`init/1`, `export/4`, `force_flush/0`,
  `shutdown/1`) and stores every exported metric in an ETS table.

  The test metric reader (see `config/test.exs`) exports periodically, so
  tests poll with `ElixirDB.Eventual`. Compiled only under `:test` (it lives
  under `test/support`).
  """

  @table __MODULE__

  @doc "Ensures the backing ETS table exists. Idempotent."
  @spec start() :: :ok
  def start do
    ensure_table()
    :ok
  end

  @doc "Clears recorded metrics between tests."
  @spec reset() :: :ok
  def reset do
    if :ets.whereis(@table) != :undefined, do: :ets.delete_all_objects(@table)
    :ok
  end

  @doc """
  Returns all exported metric views:
  `%{name: String.t(), datapoints: [%{attributes: term(), value: number()} | %{attributes: term(), count: number(), sum: number()}]}`.
  """
  @spec metrics() :: [map()]
  def metrics do
    if :ets.whereis(@table) == :undefined do
      []
    else
      @table |> :ets.tab2list() |> Enum.map(fn {_ref, view} -> view end)
    end
  end

  @doc "Returns the datapoints recorded for metric `name`."
  @spec datapoints(binary()) :: [map()]
  def datapoints(name) do
    metrics()
    |> Enum.filter(&(&1.name == name))
    |> Enum.flat_map(& &1.datapoints)
  end

  @doc """
  Returns the sum of the counter values for metric `name` restricted to
  datapoints whose attributes contain every pair in `attr_subset`.
  """
  @spec counter_sum(binary(), map()) :: number()
  def counter_sum(name, attr_subset \\ %{}) do
    name
    |> datapoints()
    |> Enum.filter(fn dp -> attrs_match?(dp[:attributes], attr_subset) end)
    |> Enum.reduce(0, fn dp, acc -> acc + (dp[:value] || 0) end)
  end

  @doc "Returns the value of attribute `key` on a datapoint, or nil."
  @spec datapoint_attr(map(), atom() | binary()) :: term()
  def datapoint_attr(datapoint, key) do
    map = attr_map(datapoint[:attributes])
    Map.get(map, key) || Map.get(map, to_string(key))
  end

  defp attrs_match?(attributes, subset) do
    map = attr_map(attributes)
    Enum.all?(subset, fn {k, v} -> Map.get(map, k) == v || Map.get(map, to_string(k)) == v end)
  end

  defp attr_map({:attributes, _count_limit, _value_len_limit, _dropped, map}) when is_map(map),
    do: map

  defp attr_map(map) when is_map(map), do: map
  defp attr_map(_), do: %{}

  # --- exporter callbacks (invoked by otel_metric_reader via otel_exporter) ---

  @doc false
  def init(_opts), do: {:ok, []}

  @doc false
  def export(:metrics, metrics, _resource, _config) do
    ensure_table()

    Enum.each(List.wrap(metrics), fn metric ->
      :ets.insert(@table, {make_ref(), to_view(metric)})
    end)

    :ok
  end

  def export(_signal, _data, _resource, _config), do: :ok

  @doc false
  def force_flush, do: :ok

  @doc false
  def shutdown(_config), do: :ok

  # #metric{name, scope, description, unit, data}
  defp to_view({:metric, name, _scope, _description, _unit, data}) do
    %{name: to_string(name), datapoints: datapoints_for(data)}
  end

  defp to_view(_other), do: %{name: "unknown", datapoints: []}

  # #sum{datapoints, aggregation_temporality, is_monotonic}
  defp datapoints_for({:sum, dps, _temporality, _monotonic}),
    do: Enum.map(List.wrap(dps), &number_dp/1)

  # #gauge{datapoints}
  defp datapoints_for({:gauge, dps}), do: Enum.map(List.wrap(dps), &number_dp/1)

  # #histogram{datapoints, aggregation_temporality}
  defp datapoints_for({:histogram, dps, _temporality}),
    do: Enum.map(List.wrap(dps), &histogram_dp/1)

  defp datapoints_for(_), do: []

  # #datapoint{attributes, start_time, time, value, exemplars, flags}
  defp number_dp({:datapoint, attributes, _start_time, _time, value, _exemplars, _flags}),
    do: %{attributes: attributes, value: value}

  defp number_dp(_), do: %{attributes: nil, value: 0}

  # #histogram_datapoint{attributes, start_time, time, count, sum, bucket_counts,
  #                      explicit_bounds, exemplars, flags, min, max}
  defp histogram_dp(
         {:histogram_datapoint, attributes, _start_time, _time, count, sum, _bucket_counts,
          _explicit_bounds, _exemplars, _flags, _min, _max}
       ),
       do: %{attributes: attributes, count: count, sum: sum}

  defp histogram_dp(_), do: %{attributes: nil, count: 0, sum: 0}

  defp ensure_table do
    if :ets.whereis(@table) == :undefined do
      try do
        :ets.new(@table, [:named_table, :public, :duplicate_bag])
      rescue
        # Already exists (named table): fine.
        ArgumentError -> :ok
      catch
        :error, :badarg -> :ok
      end
    end

    :ok
  end
end
