defmodule VialKeeper.Observability.Instrumentation.Search do
  @moduledoc """
  Emitters for full-text cache rebuild:

  * `vial_keeper.search.rebuild` — span + counter + histogram
  * `vial_keeper.search.rebuild.batch` — bounded batch span + counter + histogram
  * `vial_keeper.search.refresh` — incremental winner-refresh span + counter + histogram
  * `vial_keeper.search.query` — Tantivy query span + counter + histogram

  Covers reconstructing posting lists from winning documents: index create,
  explicit rebuild, and cache-miss rebuild during query. The inverted index is
  not authoritative document state.
  """

  alias VialKeeper.Observability.{Meters, Tracer}

  @span "vial_keeper.search.rebuild"
  @count_metric :"vial_keeper.search.rebuild.count"
  @duration_metric :"vial_keeper.search.rebuild.duration"
  @batch_span "vial_keeper.search.rebuild.batch"
  @batch_count_metric :"vial_keeper.search.rebuild.batch.count"
  @batch_duration_metric :"vial_keeper.search.rebuild.batch.duration"
  @refresh_span "vial_keeper.search.refresh"
  @refresh_count_metric :"vial_keeper.search.refresh.count"
  @refresh_duration_metric :"vial_keeper.search.refresh.duration"
  @query_span "vial_keeper.search.query"
  @query_count_metric :"vial_keeper.search.query.count"
  @query_duration_metric :"vial_keeper.search.query.duration"

  @type trigger :: :create | :rebuild | :cache_miss

  @doc """
  Records `vial_keeper.search.rebuild` around reconstruction of posting lists.

  `fun` returns `{:ok, entries}` on success, where `entries` is the number of
  winning documents scanned into the cache, or `{:error, Error.t()}`.
  """
  @spec rebuild(binary() | nil, term(), trigger(), (-> result)) :: result when result: term()
  def rebuild(uuid, index_id, trigger, fun)
      when (is_binary(uuid) or is_nil(uuid)) and trigger in [:create, :rebuild, :cache_miss] and
             is_function(fun, 0) do
    started = System.monotonic_time()

    base_attrs =
      [index_id: index_id, index_type: :full_text, trigger: trigger] ++
        if(uuid, do: [db_uuid: uuid], else: [])

    Tracer.with_span(@span, base_attrs, fn ->
      result = fun.()
      duration = System.monotonic_time() - started
      emit_result(base_attrs, duration, result)
      result
    end)
  end

  @doc "Records one bounded Tantivy rebuild batch without exposing document data."
  @spec rebuild_batch(binary() | nil, term(), non_neg_integer(), (-> term())) :: term()
  def rebuild_batch(uuid, index_id, entries, fun)
      when (is_binary(uuid) or is_nil(uuid)) and is_integer(entries) and entries >= 0 and
             is_function(fun, 0) do
    attrs =
      [index_id: index_id, index_type: :full_text, entries: entries] ++
        if(uuid, do: [db_uuid: uuid], else: [])

    started = System.monotonic_time()
    result = measure_operation(@batch_span, @batch_count_metric, @batch_duration_metric, attrs, fun)

    :telemetry.execute(
      [:vial_keeper, :search, :rebuild, :batch],
      %{duration: System.monotonic_time() - started, entries: entries},
      %{outcome: if(match?({:error, _}, result), do: :failed, else: :ok)}
    )

    result
  end

  @doc "Records one incremental winner-refresh batch without exposing document data."
  @spec refresh(binary() | nil, non_neg_integer(), (-> term())) :: term()
  def refresh(uuid, entries, fun)
      when (is_binary(uuid) or is_nil(uuid)) and is_integer(entries) and entries >= 0 and
             is_function(fun, 0) do
    attrs = [index_type: :full_text, entries: entries] ++ if(uuid, do: [db_uuid: uuid], else: [])

    measure_operation(@refresh_span, @refresh_count_metric, @refresh_duration_metric, attrs, fun)
  end

  defp measure_operation(span, count_metric, duration_metric, attrs, fun) do
    started = System.monotonic_time()

    Tracer.with_span(span, attrs, fn ->
      result = fun.()
      duration = System.monotonic_time() - started
      outcome = if match?({:error, _}, result), do: :failed, else: :ok
      result_attrs = attrs ++ [outcome: outcome]
      Meters.add(count_metric, result_attrs)
      Meters.record(duration_metric, duration, result_attrs)
      result
    end)
  end

  @doc "Records one Tantivy query without recording query text or document IDs."
  @spec query(binary() | nil, binary(), (-> term())) :: term()
  def query(uuid, mode, fun)
      when (is_binary(uuid) or is_nil(uuid)) and is_binary(mode) and is_function(fun, 0) do
    started = System.monotonic_time()
    mode = normalize_mode(mode)

    attrs =
      [index_type: :full_text, backend: :tantivy, search_mode: mode] ++
        if(uuid, do: [db_uuid: uuid], else: [])

    Tracer.with_span(@query_span, attrs, fn ->
      result = fun.()
      duration = System.monotonic_time() - started
      result_count = result_count(result)
      outcome = if match?({:error, _}, result), do: :failed, else: :ok
      query_attrs = attrs ++ [entries: result_count, outcome: outcome]
      Meters.add(@query_count_metric, query_attrs)
      Meters.record(@query_duration_metric, duration, query_attrs)
      result
    end)
  end

  defp normalize_mode(mode) when mode in ~w(any all phrase prefix), do: mode
  defp normalize_mode(_mode), do: "other"

  defp emit_result(base_attrs, duration, {:ok, entries})
       when is_integer(entries) and entries >= 0 do
    attrs = base_attrs ++ [outcome: :ok, entries: entries]
    Meters.add(@count_metric, attrs)
    Meters.record(@duration_metric, duration, attrs)
    _ = Tracer.set_attributes(Keyword.take(attrs, [:outcome, :entries, :trigger]))
    :ok
  end

  defp emit_result(base_attrs, duration, {:error, %VialKeeper.Error{} = error}) do
    attrs = base_attrs ++ [outcome: :failed, error_code: error.code]
    Meters.add(@count_metric, attrs)
    Meters.record(@duration_metric, duration, attrs)
    _ = Tracer.record_error(error)
    _ = Tracer.apply_error_status(error)
    :ok
  end

  defp emit_result(base_attrs, duration, _result) do
    attrs = base_attrs ++ [outcome: :ok]
    Meters.add(@count_metric, attrs)
    Meters.record(@duration_metric, duration, attrs)
    :ok
  end

  defp result_count({:ok, results}) when is_list(results), do: length(results)
  defp result_count(_result), do: 0
end
