defmodule ElixirDB.Observability.Instrumentation.Query do
  @moduledoc """
  Emitters for Plan §11 query/index events:

    * `elixir_db.query.execute` — span + histogram
    * `elixir_db.index.build`   — span + histogram

  Called from the SQLite adapter at the adapter entry, but via this service-level
  helper so the adapter itself stays OTel-agnostic (it only knows about a
  project callback, plan §5.5).
  """

  alias ElixirDB.Observability.{Meters, Tracer}

  @query_span "elixir_db.query.execute"
  @index_span "elixir_db.index.build"

  @doc """
  Records `elixir_db.query.execute` for a query. `started` is the native
  monotonic timestamp the adapter captures for its overrun guard (reused, not a
  second clock).

  The `fun` may return either the raw result, or `{result, examined_count}` so
  the span can record the true candidate count after the query runs.
  """
  @spec execute(binary() | nil, non_neg_integer(), term(), (-> term())) :: term()
  def execute(uuid, _initial_examined, started, fun)
      when (is_binary(uuid) or is_nil(uuid)) and is_function(fun, 0) do
    start_attrs = if(uuid, do: [db_uuid: uuid], else: [])

    Tracer.with_span(@query_span, start_attrs, fn ->
      fun_result = fun.()

      {result, examined} =
        case fun_result do
          {{:ok, _} = r, count} -> {r, count}
          {{:error, _} = r, _count} -> {r, nil}
          other -> {other, nil}
        end

      duration = System.monotonic_time() - started

      attrs = start_attrs ++ if(examined, do: [examined: examined], else: [])

      Meters.record(:"elixir_db.query.execute.duration", duration, attrs)
      _ = if(examined, do: Tracer.set_attributes(examined: examined))

      case result do
        {:error, %ElixirDB.Error{} = error} ->
          _ = Tracer.record_error(error)
          _ = Tracer.apply_error_status(error)

        _ ->
          :ok
      end

      result
    end)
  end

  @doc """
  Records `elixir_db.index.build` for a create/rebuild of `index_id` of
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

      Meters.record(:"elixir_db.index.build.duration", duration, attrs)

      case result do
        {:error, %ElixirDB.Error{} = error} ->
          _ = Tracer.record_error(error)
          _ = Tracer.apply_error_status(error)

        _ ->
          :ok
      end

      result
    end)
  end
end
