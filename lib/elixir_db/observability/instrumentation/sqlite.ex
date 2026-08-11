defmodule ElixirDB.Observability.Instrumentation.SQLite do
  @moduledoc """
  Low-cardinality OpenTelemetry spans for SQLite implementation phases.

  These probes sit below the public database and query spans. They measure
  bounded backend phases such as document lookup, bulk preparation, changes
  decoding, index catalog reads, and candidate gathering. Shared query
  filtering, ordering, cursors, and projection are product spans on
  `elixir_db.query.execute`, not SQLite phase probes.

  The phase names are intentionally fixed so trace searches remain stable:

    * `elixir_db.sqlite.document.lookup`
    * `elixir_db.sqlite.document.leaves`
    * `elixir_db.sqlite.revision.lookup`
    * `elixir_db.sqlite.mutation.bulk.prepare`
    * `elixir_db.sqlite.mutation.bulk.finalize`
    * `elixir_db.sqlite.changes.identity`
    * `elixir_db.sqlite.changes.fetch`
    * `elixir_db.sqlite.changes.decode`
    * `elixir_db.sqlite.changes.has_more`
    * `elixir_db.sqlite.query.prepare_request`
    * `elixir_db.sqlite.query.identity`
    * `elixir_db.sqlite.query.index_catalog`
    * `elixir_db.sqlite.query.plan`
    * `elixir_db.sqlite.query.candidates`
    * `elixir_db.sqlite.query.filter`
    * `elixir_db.sqlite.query.sort`
    * `elixir_db.sqlite.query.cursor`
    * `elixir_db.sqlite.query.project`
    * `elixir_db.sqlite.transaction.begin`
    * `elixir_db.sqlite.transaction.commit`
    * `elixir_db.sqlite.transaction.rollback`
  """

  alias ElixirDB.Observability.Tracer
  @phase_timing_key {__MODULE__, :phase_timings}

  @typedoc "A stable SQLite phase key mapped to a low-cardinality span name."
  @type sqlite_phase ::
          :document_lookup
          | :document_leaves
          | :revision_lookup
          | :bulk_prepare
          | :bulk_finalize
          | :changes_identity
          | :changes_fetch
          | :changes_decode
          | :changes_has_more
          | :query_prepare_request
          | :query_identity
          | :query_index_catalog
          | :query_plan
          | :query_candidates
          | :query_filter
          | :query_sort
          | :query_cursor
          | :query_project
          | :transaction_begin
          | :transaction_commit
          | :transaction_rollback

  @span_names %{
    document_lookup: "elixir_db.sqlite.document.lookup",
    document_leaves: "elixir_db.sqlite.document.leaves",
    revision_lookup: "elixir_db.sqlite.revision.lookup",
    bulk_prepare: "elixir_db.sqlite.mutation.bulk.prepare",
    bulk_finalize: "elixir_db.sqlite.mutation.bulk.finalize",
    changes_identity: "elixir_db.sqlite.changes.identity",
    changes_fetch: "elixir_db.sqlite.changes.fetch",
    changes_decode: "elixir_db.sqlite.changes.decode",
    changes_has_more: "elixir_db.sqlite.changes.has_more",
    query_prepare_request: "elixir_db.sqlite.query.prepare_request",
    query_identity: "elixir_db.sqlite.query.identity",
    query_index_catalog: "elixir_db.sqlite.query.index_catalog",
    query_plan: "elixir_db.sqlite.query.plan",
    query_candidates: "elixir_db.sqlite.query.candidates",
    query_filter: "elixir_db.sqlite.query.filter",
    query_sort: "elixir_db.sqlite.query.sort",
    query_cursor: "elixir_db.sqlite.query.cursor",
    query_project: "elixir_db.sqlite.query.project",
    transaction_begin: "elixir_db.sqlite.transaction.begin",
    transaction_commit: "elixir_db.sqlite.transaction.commit",
    transaction_rollback: "elixir_db.sqlite.transaction.rollback"
  }

  @doc "Traces a SQLite implementation phase without recording customer data."
  @spec trace_sqlite_phase(sqlite_phase(), (-> term())) :: term()
  def trace_sqlite_phase(phase, fun) when is_function(fun, 0),
    do: trace_sqlite_phase(phase, [], fun)

  @doc "Traces a SQLite implementation phase with bounded allow-listed attributes."
  @spec trace_sqlite_phase(sqlite_phase(), keyword(), (-> term())) :: term()
  def trace_sqlite_phase(phase, attrs, fun)
      when is_atom(phase) and is_list(attrs) and is_function(fun, 0) do
    case Process.get(@phase_timing_key) do
      nil ->
        if Tracer.tracing_enabled?(), do: run_phase(phase, attrs, fun), else: fun.()

      timings ->
        started = System.monotonic_time()

        try do
          run_phase(phase, attrs, fun)
        after
          elapsed = System.monotonic_time() - started
          Process.put(@phase_timing_key, [{phase, elapsed} | timings])
        end
    end
  end

  @doc "Enables process-local phase timing until `take_phase_timings/0` is called."
  @spec start_phase_timings() :: :ok
  def start_phase_timings do
    Process.put(@phase_timing_key, [])
    :ok
  end

  @doc "Returns and clears process-local phase timings in execution order."
  @spec take_phase_timings() :: [{sqlite_phase(), integer()}]
  def take_phase_timings do
    @phase_timing_key
    |> Process.get([])
    |> Enum.reverse()
    |> then(fn timings ->
      Process.delete(@phase_timing_key)
      timings
    end)
  end

  defp run_phase(phase, attrs, fun) do
    Tracer.with_span(Map.fetch!(@span_names, phase), attrs, fn ->
      result = fun.()
      record_phase_error(result)
      result
    end)
  end

  defp record_phase_error({:error, %ElixirDB.Error{} = error}) do
    _ = Tracer.record_error(error)
    _ = Tracer.apply_error_status(error)
    :ok
  end

  defp record_phase_error(_result), do: :ok
end
