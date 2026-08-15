defmodule VialKeeper.Observability.Instrumentation.Query do
  @moduledoc """
  Emitters for query and index events:

    * `vial_keeper.query.execute` — span + histogram
    * `vial_keeper.index.build`   — span + histogram

  Query execute is recorded from shared query services so every backend emits
  the same product signal. Index build is recorded at the backend index
  create/rebuild entry because physical index construction stays backend-owned.
  """

  alias VialKeeper.Observability.{Meters, Tracer}

  @query_span "vial_keeper.query.execute"
  @index_span "vial_keeper.index.build"

  @doc """
  Records `vial_keeper.query.execute` for a query. `started` is the native
  monotonic timestamp the service captures for its overrun guard (reused, not a
  second clock).

  The `fun` may return either the raw result, or `{result, examined_count}` so
  the span can record the true candidate count after the query runs.
  """
  @spec execute(binary() | nil, non_neg_integer(), term(), (-> term())) :: term()
  def execute(uuid, _initial_examined, started, fun)
      when (is_binary(uuid) or is_nil(uuid)) and is_function(fun, 0) do
    start_attrs = start_attrs(uuid)

    Tracer.with_span(@query_span, start_attrs, fn ->
      {result, examined} = split_result(fun.())

      duration = System.monotonic_time() - started

      attrs = query_attrs(start_attrs, examined)

      Meters.record(:"vial_keeper.query.execute.duration", duration, attrs)
      _ = record_plan(result)
      _ = record_examined(examined)
      _ = record_result_error(result)

      result
    end)
  end

  @doc """
  Records `vial_keeper.index.build` for a create/rebuild of `index_id` of
  `index_type`.
  """
  @spec build_index(binary() | nil, term(), atom() | nil, (-> term())) :: term()
  def build_index(uuid, index_id, index_type, fun)
      when (is_binary(uuid) or is_nil(uuid)) and is_function(fun, 0) do
    attrs =
      [index_id: index_id, index_type: index_type] ++
        if(uuid, do: [db_uuid: uuid], else: [])

    started = System.monotonic_time()

    Tracer.with_span(@index_span, attrs, fn ->
      result = fun.()
      duration = System.monotonic_time() - started

      Meters.record(:"vial_keeper.index.build.duration", duration, attrs)

      case result do
        {:error, %VialKeeper.Error{} = error} ->
          _ = Tracer.record_error(error)
          _ = Tracer.apply_error_status(error)

        _ ->
          :ok
      end

      result
    end)
  end

  defp start_attrs(nil), do: []
  defp start_attrs(uuid), do: [db_uuid: uuid]

  defp split_result({{:ok, _} = result, count}), do: {result, count}
  defp split_result({{:error, _} = result, _count}), do: {result, nil}
  defp split_result(result), do: {result, nil}

  defp query_attrs(attrs, nil), do: attrs
  defp query_attrs(attrs, examined), do: attrs ++ [examined: examined]

  defp record_plan({:ok, result}) when is_map(result) do
    plan_kind = Map.get(result, :plan_kind, Map.get(result, "plan_kind"))
    bindings = Map.get(result, :index_bindings, Map.get(result, "index_bindings", []))

    attrs =
      [
        plan_kind: plan_kind,
        selected_index_count: length_if_list(bindings),
        union_branch_count:
          if(plan_kind in [:union, "union"], do: length_if_list(bindings), else: 0)
      ]

    _ = Tracer.set_attributes(attrs)
    :ok
  end

  defp record_plan(_result), do: :ok

  defp length_if_list(value) when is_list(value), do: length(value)
  defp length_if_list(_value), do: 0

  defp record_examined(nil), do: :ok
  defp record_examined(examined), do: Tracer.set_attributes(examined: examined)

  defp record_result_error({:error, %VialKeeper.Error{} = error}) do
    _ = Tracer.record_error(error)
    _ = Tracer.apply_error_status(error)
    :ok
  end

  defp record_result_error(_), do: :ok
end
