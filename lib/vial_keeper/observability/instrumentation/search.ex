defmodule VialKeeper.Observability.Instrumentation.Search do
  @moduledoc """
  Emitters for full-text cache rebuild:

    * `vial_keeper.search.rebuild` — span + counter + histogram

  Covers reconstructing posting lists from winning documents: index create,
  explicit rebuild, and cache-miss rebuild during query. The inverted index is
  not authoritative document state.
  """

  alias VialKeeper.Observability.{Meters, Tracer}

  @span "vial_keeper.search.rebuild"
  @count_metric :"vial_keeper.search.rebuild.count"
  @duration_metric :"vial_keeper.search.rebuild.duration"

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
end
