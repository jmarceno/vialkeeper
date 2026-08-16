defmodule VialKeeper.Observability.TestExporter.Owner do
  @moduledoc """
  Keeps the observability test span table alive independently of test processes.

  Export callbacks run in short-lived OpenTelemetry worker processes. The table
  must therefore be owned by a process whose lifetime is not tied to one test
  or one export operation.
  """

  use GenServer

  @impl true
  def init(table) do
    if :ets.whereis(table) == :undefined do
      _ = :ets.new(table, [:named_table, :public, :bag])
      :ok
    end

    {:ok, table}
  end
end

defmodule VialKeeper.Observability.TestExporter do
  @moduledoc """
  In-memory span exporter for tests. Implements the `otel_exporter_traces`
  behaviour (`init/1`, `export/3`, `shutdown/1`) and stores every ended span
  in an ETS table keyed by span id.

  This lets tests assert on the span catalog without any network. It is compiled
  only under `:test` (it lives under `test/support`).
  """

  @behaviour :otel_exporter_traces
  @owner VialKeeper.Observability.TestExporter.Owner
  @table __MODULE__

  # #span record tuple indices (0-based; tag at 0). From
  # deps/opentelemetry/include/otel_span.hrl:
  # 0=span(tag), 1=trace_id, 2=span_id, 3=tracestate, 4=parent_span_id,
  # 5=parent_span_is_remote, 6=name, 7=kind, 8=start_time, 9=end_time,
  # 10=attributes, 11=events, 12=links, 13=status
  @span_trace_id 1
  @span_span_id 2
  @span_parent_span_id 4
  @span_name 6
  @span_start_time 8
  @span_end_time 9
  @span_attributes 10
  @span_status 13

  @doc "Starts the recorder. Idempotent; safe to call from test setup."
  @spec start() :: :ok
  def start do
    ensure_table()
    :ok
  end

  @doc "Clears recorded spans between tests."
  @spec reset() :: :ok
  def reset do
    if :ets.whereis(@table) != :undefined, do: :ets.delete_all_objects(@table)
    :ok
  end

  @doc "Returns all recorded spans."
  @spec spans() :: [map()]
  def spans do
    if :ets.whereis(@table) == :undefined do
      []
    else
      @table |> :ets.tab2list() |> Enum.map(fn {_, span} -> span end)
    end
  end

  @doc "Returns spans whose name equals `name`."
  @spec spans_named(binary() | atom()) :: [map()]
  def spans_named(name) do
    spans()
    |> Enum.filter(fn span -> normalize_name(span[:name]) == normalize_name(name) end)
  end

  @doc "Returns the value of attribute `key` on `span`, or `nil`."
  @spec span_attr(term(), atom()) :: term()
  def span_attr(span, key) do
    case span[:attributes] do
      nil -> nil
      {:attributes, _count_limit, _value_len_limit, _dropped, map} -> lookup_map_attr(map, key)
      map when is_map(map) -> lookup_map_attr(map, key)
      list when is_list(list) -> lookup_list_attr(list, key)
      _ -> nil
    end
  end

  @doc """
  Returns the recorded span's status code: `:unset`, `:ok`, or `:error`.

  The status is a field of the span record (not an attribute); a span whose
  status was never set is `:unset`.
  """
  @spec status_code(map()) :: :unset | :ok | :error
  def status_code(%{status: {:status, code, _message}}) when code in [:unset, :ok, :error],
    do: code

  def status_code(_), do: :unset

  defp lookup_map_attr(map, key) do
    Map.get(map, key) || Map.get(map, Atom.to_string(key))
  end

  defp lookup_list_attr(list, key) do
    case List.keyfind(list, key, 0) do
      {^key, value} -> value
      _ -> nil
    end
  end

  defp normalize_name(n) when is_atom(n), do: Atom.to_string(n)
  defp normalize_name(n) when is_binary(n), do: n
  defp normalize_name(_), do: nil

  # --- otel_exporter_traces callbacks ---

  @impl true
  def init(_opts) do
    # The SDK may call init/1 from more than one process; table creation must
    # never fail init (a failed exporter silently drops every span).
    ensure_table()
    {:ok, []}
  end

  @impl true
  def export(spans_tab, _resource, _config) do
    # Recreate the table if its owner died (named tables vanish with their
    # owner process); exporting runs in long-lived SDK processes.
    ensure_table()

    # spans_tab is an ETS table of #span{} record tuples. Fold and store a
    # friendlier map view keyed by span id.
    fun = fn span, _acc ->
      span_view = %{
        name: record_field(span, @span_name),
        trace_id: record_field(span, @span_trace_id),
        span_id: record_field(span, @span_span_id),
        parent_span_id: record_field(span, @span_parent_span_id),
        start_time: record_field(span, @span_start_time),
        end_time: record_field(span, @span_end_time),
        attributes: record_field(span, @span_attributes),
        status: record_field(span, @span_status)
      }

      :ets.insert(@table, {Map.get(span_view, :span_id), span_view})
    end

    :ets.foldl(fun, [], spans_tab)
    :ok
  rescue
    e in [ArgumentError, ErlangError, RuntimeError, UndefinedFunctionError] ->
      IO.warn("TestExporter.export failed: #{inspect(e)}")
      :ok
  end

  @impl true
  def shutdown(_config), do: :ok

  defp record_field(span, index) when is_tuple(span) and tuple_size(span) > index do
    :erlang.element(index + 1, span)
  end

  defp record_field(_, _), do: nil

  # The owner is intentionally unlinked from test processes so the recorder
  # survives test teardown and exporter worker turnover.
  defp ensure_table do
    case Process.whereis(@owner) do
      pid when is_pid(pid) -> :ok
      nil -> start_owner()
    end

    :ok
  end

  defp start_owner do
    case GenServer.start(@owner, @table, name: @owner) do
      {:ok, _pid} -> :ok
      {:error, {:already_started, _pid}} -> :ok
    end
  end
end
