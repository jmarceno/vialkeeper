defmodule VialKeeper.Observability.Dashboard do
  alias VialKeeper.Runtime.{DatabaseAdmission, DatabaseCatalog}

  @moduledoc """
  Local OpenTelemetry metric exporter and compact runtime snapshot.

  The replication harness installs this module as an additional OTel metric
  reader. It keeps the latest cumulative metric view in a persistent term so a
  local HTTP request can read the SDK's exported data without introducing a
  second set of application counters. No customer data is retained here: only
  metric aggregates and bounded runtime pressure signals are returned.

  The exporter is intentionally useful outside the harness as well. It is not
  enabled by default, and the HTTP route that serves `snapshot/0` is gated by
  `:observability_dashboard` in the application environment.
  """

  @state_key {__MODULE__, :state}
  @default_state %{metrics: %{}, exported_at_ms: nil}
  @milliseconds_in_native_unit System.convert_time_unit(1, :millisecond, :native)

  @http_metric "vial_keeper.http.request.duration"
  @command_metric "vial_keeper.database.command.duration"
  @changes_metric "vial_keeper.changes.read.duration"
  @replication_metric "vial_keeper.replication.batch.duration"
  @checkpoint_metric "vial_keeper.replication.checkpoint.count"
  @database_open_metric "vial_keeper.database.open.count"
  @admission_metric "vial_keeper.database.admission.wait"

  @doc false
  @spec init(term()) :: {:ok, []}
  def init(_opts) do
    reset()
    {:ok, []}
  end

  @doc false
  @spec export(:metrics, list(), term(), term()) :: :ok
  def export(:metrics, metrics, _resource, _config) do
    current = state()

    next_metrics =
      metrics
      |> List.wrap()
      |> Enum.reduce(current.metrics, fn metric, acc ->
        case metric_view(metric) do
          nil -> acc
          view -> Map.update(acc, view.name, view, &merge_metric_view(&1, view))
        end
      end)

    :persistent_term.put(
      @state_key,
      %{current | metrics: next_metrics, exported_at_ms: System.system_time(:millisecond)}
    )

    :ok
  rescue
    # Exporters must not take down the SDK reader because a future SDK version
    # changed a private metric tuple shape.
    _error in [
      ArgumentError,
      ArithmeticError,
      BadMapError,
      CaseClauseError,
      ErlangError,
      FunctionClauseError,
      KeyError,
      MatchError,
      Protocol.UndefinedError,
      RuntimeError,
      UndefinedFunctionError,
      WithClauseError
    ] ->
      :ok
  end

  @doc false
  def export(_signal, _metrics, _resource, _config), do: :ok

  @doc false
  def force_flush, do: :ok

  @doc false
  def shutdown(_config), do: :ok

  @doc "Clears the local dashboard view. Primarily useful for isolated tests."
  @spec reset() :: :ok
  def reset do
    :persistent_term.put(@state_key, @default_state)
    :ok
  end

  @doc "Returns the current aggregated system snapshot for the local dashboard."
  @spec snapshot() :: map()
  def snapshot do
    current = state()
    http = histogram_summary(current.metrics, @http_metric)
    commands = histogram_summary(current.metrics, @command_metric)
    changes = histogram_summary(current.metrics, @changes_metric)
    replication = histogram_summary(current.metrics, @replication_metric)
    checkpoints = counter_summary(current.metrics, @checkpoint_metric)
    database_opens = counter_summary(current.metrics, @database_open_metric)
    admission = histogram_summary(current.metrics, @admission_metric)
    runtime = runtime_snapshot()

    %{
      "status" => runtime["status"],
      "sampled_at" => sampled_at(current.exported_at_ms),
      "otel" => %{
        "available" => not is_nil(current.exported_at_ms),
        "http" => http,
        "commands" => commands,
        "changes" => changes,
        "replication" => replication,
        "checkpoints" => checkpoints,
        "database_opens" => database_opens,
        "admission" => admission,
        "errors" => %{
          "http" => http["error_count"],
          "commands" => commands["error_count"],
          "changes" => changes["error_count"],
          "replication" => replication["error_count"] + checkpoints["error_count"]
        }
      },
      "runtime" => runtime
    }
  end

  @doc false
  @spec metrics() :: map()
  def metrics, do: state().metrics

  defp state do
    :persistent_term.get(@state_key, @default_state)
  end

  # OTel metric records are Erlang records represented as tuples at the Elixir
  # boundary. Keep the decoder defensive because this module is a local demo
  # view, not part of the SDK's private record contract.
  defp metric_view({:metric, name, _scope, _description, _unit, data}) do
    {kind, temporality} = metric_kind(data)

    %{
      name: to_string(name),
      kind: kind,
      temporality: temporality,
      datapoints: datapoints_for(data)
    }
  rescue
    _error in [
      ArgumentError,
      ArithmeticError,
      BadMapError,
      CaseClauseError,
      ErlangError,
      FunctionClauseError,
      KeyError,
      MatchError,
      Protocol.UndefinedError,
      RuntimeError,
      UndefinedFunctionError,
      WithClauseError
    ] ->
      nil
  end

  defp metric_view(_), do: nil

  defp metric_kind({:sum, _datapoints, temporality, _monotonic}), do: {:sum, temporality}
  defp metric_kind({:gauge, _datapoints}), do: {:gauge, nil}
  defp metric_kind({:histogram, _datapoints, temporality}), do: {:histogram, temporality}
  defp metric_kind(_), do: {:unknown, nil}

  defp datapoints_for({:sum, datapoints, _temporality, _monotonic}),
    do: Enum.map(List.wrap(datapoints), &number_datapoint/1)

  defp datapoints_for({:gauge, datapoints}),
    do: Enum.map(List.wrap(datapoints), &number_datapoint/1)

  defp datapoints_for({:histogram, datapoints, _temporality}),
    do: Enum.map(List.wrap(datapoints), &histogram_datapoint/1)

  defp datapoints_for(_), do: []

  defp number_datapoint({:datapoint, attributes, _start_time, _time, value, _exemplars, _flags})
       when is_number(value),
       do: %{attributes: attributes, value: value}

  defp number_datapoint(_), do: %{attributes: nil, value: 0}

  defp histogram_datapoint(
         {:histogram_datapoint, attributes, _start_time, _time, count, sum, bucket_counts,
          explicit_bounds, _exemplars, _flags, min, max}
       )
       when is_number(count) and is_number(sum),
       do: %{
         attributes: attributes,
         count: count,
         sum: sum,
         bucket_counts: List.wrap(bucket_counts),
         explicit_bounds: List.wrap(explicit_bounds),
         min: min,
         max: max
       }

  defp histogram_datapoint(_),
    do: %{attributes: nil, count: 0, sum: 0, bucket_counts: [], explicit_bounds: []}

  defp merge_metric_view(existing, %{datapoints: []}), do: existing

  defp merge_metric_view(existing, %{temporality: :temporality_delta} = incoming) do
    if existing.kind == incoming.kind do
      merge_delta(existing, incoming)
    else
      incoming
    end
  end

  defp merge_metric_view(_existing, incoming), do: incoming

  defp merge_delta(%{kind: :sum} = existing, incoming) do
    %{
      existing
      | datapoints: merge_datapoints(existing.datapoints, incoming.datapoints, &merge_number/2)
    }
  end

  defp merge_delta(%{kind: :histogram} = existing, incoming) do
    %{
      existing
      | datapoints: merge_datapoints(existing.datapoints, incoming.datapoints, &merge_histogram/2)
    }
  end

  defp merge_delta(_existing, incoming), do: incoming

  defp merge_datapoints(existing, incoming, merge_fun) do
    existing_keys = Enum.map(existing, & &1[:attributes])
    existing_values = Map.new(existing, fn datapoint -> {datapoint[:attributes], datapoint} end)

    {new_keys, values} =
      Enum.reduce(incoming, {[], existing_values}, fn datapoint, {new_keys, values} ->
        key = datapoint[:attributes]

        case Map.fetch(values, key) do
          {:ok, current} ->
            {new_keys, Map.put(values, key, merge_fun.(current, datapoint))}

          :error ->
            {[key | new_keys], Map.put(values, key, datapoint)}
        end
      end)

    (existing_keys ++ Enum.reverse(new_keys))
    |> Enum.map(&Map.fetch!(values, &1))
  end

  defp merge_number(left, right),
    do: %{left | value: numeric(left[:value]) + numeric(right[:value])}

  defp merge_histogram(left, right) do
    %{
      left
      | count: non_negative(left[:count]) + non_negative(right[:count]),
        sum: numeric(left[:sum]) + numeric(right[:sum]),
        bucket_counts: merge_buckets(left[:bucket_counts], right[:bucket_counts]),
        min: merge_min(left[:min], right[:min]),
        max: merge_max(left[:max], right[:max])
    }
  end

  defp merge_buckets(left, right) do
    length = max(length(left), length(right))
    left = List.to_tuple(left)
    right = List.to_tuple(right)

    if length == 0 do
      []
    else
      for index <- 0..(length - 1) do
        non_negative(tuple_value(left, index)) + non_negative(tuple_value(right, index))
      end
    end
  end

  defp tuple_value(tuple, index) when index < tuple_size(tuple), do: elem(tuple, index)
  defp tuple_value(_tuple, _index), do: nil

  defp merge_min(left, right) do
    [left, right]
    |> Enum.map(&numeric_or_nil/1)
    |> Enum.reject(&is_nil/1)
    |> case do
      [] -> nil
      values -> Enum.min(values)
    end
  end

  defp merge_max(left, right) do
    [left, right]
    |> Enum.map(&numeric_or_nil/1)
    |> Enum.reject(&is_nil/1)
    |> case do
      [] -> nil
      values -> Enum.max(values)
    end
  end

  defp histogram_summary(metrics, name) do
    datapoints = datapoints(metrics, name)
    count = Enum.reduce(datapoints, 0, &(&2 + non_negative(&1[:count])))
    sum = Enum.reduce(datapoints, 0, &(&2 + numeric(&1[:sum])))
    observed_max = observed_max(datapoints)

    %{
      "count" => count,
      "avg_ms" => if(count > 0, do: round_ms(sum / count), else: nil),
      "p95_ms" => percentile_ms(datapoints, count, 0.95, observed_max),
      "error_count" =>
        Enum.reduce(datapoints, 0, fn datapoint, total ->
          if error_datapoint?(datapoint), do: total + non_negative(datapoint[:count]), else: total
        end)
    }
  end

  defp counter_summary(metrics, name) do
    datapoints = datapoints(metrics, name)
    value = Enum.reduce(datapoints, 0, &(&2 + numeric(&1[:value])))

    %{
      "count" => value,
      "error_count" =>
        Enum.reduce(datapoints, 0, fn datapoint, total ->
          if error_datapoint?(datapoint), do: total + numeric(datapoint[:value]), else: total
        end)
    }
  end

  defp datapoints(metrics, name) do
    case Map.get(metrics, name) do
      %{datapoints: datapoints} when is_list(datapoints) -> datapoints
      _ -> []
    end
  end

  defp error_datapoint?(datapoint) do
    not is_nil(attribute(datapoint[:attributes], :"error.code")) or
      attribute(datapoint[:attributes], :outcome) in [:rejected, "rejected"]
  end

  defp attribute(attributes, key) do
    attributes
    |> attribute_map()
    |> then(fn map -> Map.get(map, key) || Map.get(map, Atom.to_string(key)) end)
  end

  defp attribute_map({:attributes, _count_limit, _value_len_limit, _dropped, map})
       when is_map(map),
       do: map

  defp attribute_map(map) when is_map(map), do: map
  defp attribute_map(_), do: %{}

  defp observed_max(datapoints) do
    datapoints
    |> Enum.map(&numeric_or_nil(&1[:max]))
    |> Enum.reject(&is_nil/1)
    |> case do
      [] -> nil
      values -> Enum.max(values)
    end
  end

  defp percentile_ms(_datapoints, 0, _quantile, _observed_max), do: nil

  defp percentile_ms(datapoints, count, quantile, observed_max) do
    target = max(1, trunc(Float.ceil(count * quantile)))

    buckets =
      Enum.reduce(datapoints, %{}, &accumulate_histogram_buckets/2)

    case Enum.reduce_while(sorted_buckets(buckets), 0, &percentile_step(&1, &2, target)) do
      {:found, :infinity} -> if(observed_max, do: round_ms(observed_max), else: nil)
      {:found, bound} when is_number(bound) -> round_ms(bound)
      _ -> if(observed_max, do: round_ms(observed_max), else: nil)
    end
  end

  defp percentile_step({bound, bucket_count}, running, target) do
    next = running + bucket_count
    if next >= target, do: {:halt, {:found, bound}}, else: {:cont, next}
  end

  defp accumulate_histogram_buckets(datapoint, acc) do
    bounds = datapoint[:explicit_bounds]
    counts = datapoint[:bucket_counts]

    if is_list(bounds) and is_list(counts) do
      bounded =
        Enum.zip(bounds, counts)
        |> Enum.reduce(acc, fn {bound, bucket_count}, bucket_acc ->
          Map.update(
            bucket_acc,
            bound,
            non_negative(bucket_count),
            &(&1 + non_negative(bucket_count))
          )
        end)

      tail_count =
        counts
        |> Enum.drop(length(bounds))
        |> Enum.reduce(0, &(&1 + non_negative(&2)))

      Map.update(bounded, :infinity, tail_count, &(&1 + tail_count))
    else
      acc
    end
  end

  defp sorted_buckets(buckets) do
    Enum.sort_by(buckets, fn
      {:infinity, _count} -> {1, 0}
      {bound, _count} when is_number(bound) -> {0, bound}
      {_bound, _count} -> {1, 0}
    end)
  end

  defp runtime_snapshot do
    memory = :erlang.memory()
    run_queue = :erlang.statistics(:run_queue)
    schedulers = System.schedulers_online()
    databases = database_counts()

    %{
      "status" => if(run_queue > max(schedulers * 2, 4), do: "busy", else: "ok"),
      "memory_bytes" => Keyword.get(memory, :total, 0),
      "run_queue" => run_queue,
      "schedulers_online" => schedulers,
      "dirty_cpu_schedulers_online" => :erlang.system_info(:dirty_cpu_schedulers_online),
      "dirty_io_schedulers" => :erlang.system_info(:dirty_io_schedulers),
      "process_count" => :erlang.system_info(:process_count),
      "replication_workers" => safe_registry_count(VialKeeper.Replication.WorkerRegistry),
      "registered_databases" => databases.registered,
      "open_databases" => databases.open,
      "admission_queues" => admission_queue_snapshot()
    }
  end

  defp admission_queue_snapshot do
    case DatabaseCatalog.list() do
      {:ok, entries} when is_list(entries) ->
        entries
        |> Enum.filter(&(&1[:state] == :open))
        |> Enum.map(&admission_stats_for/1)
        |> Enum.reject(&is_nil/1)

      _ ->
        []
    end
  catch
    _kind, _reason -> []
  end

  defp admission_stats_for(%{database_uuid: uuid}) when is_binary(uuid) do
    case DatabaseAdmission.stats(uuid) do
      {:ok, stats} ->
        %{
          "database_uuid" => uuid,
          "active_class" => stats.active_class && to_string(stats.active_class),
          "queued_foreground" => stats.queued_foreground,
          "queued_subscription" => stats.queued_subscription,
          "queued_replication" => stats.queued_replication,
          "queued_maintenance" => stats.queued_maintenance,
          "total_occupancy" => stats.total_occupancy,
          "closing" => stats.closing?
        }

      _ ->
        nil
    end
  end

  defp admission_stats_for(_), do: nil

  defp database_counts do
    case DatabaseCatalog.list() do
      {:ok, entries} when is_list(entries) ->
        %{registered: length(entries), open: Enum.count(entries, &(&1[:state] == :open))}

      _ ->
        %{registered: 0, open: 0}
    end
  catch
    _kind, _reason -> %{registered: 0, open: 0}
  end

  defp safe_registry_count(registry) do
    Registry.count(registry)
  catch
    :error, _reason -> 0
  end

  defp sampled_at(nil), do: nil

  defp sampled_at(milliseconds) do
    milliseconds
    |> DateTime.from_unix!(:millisecond)
    |> DateTime.to_iso8601()
  end

  defp numeric(value) when is_number(value), do: value
  defp numeric(_), do: 0

  defp numeric_or_nil(value) when is_number(value), do: value
  defp numeric_or_nil(_), do: nil

  defp non_negative(value) when is_number(value) and value >= 0, do: value
  defp non_negative(_), do: 0

  defp round_ms(value) when is_number(value),
    do: Float.round(value / @milliseconds_in_native_unit, 2)
end
