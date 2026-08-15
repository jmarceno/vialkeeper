defmodule VialKeeper.Observability.Instrumentation.SQLite do
  @moduledoc """
  Low-cardinality OpenTelemetry spans for SQLite implementation phases.

  These probes sit below the public database and query spans. They measure
  bounded backend phases such as document lookup, bulk preparation, changes
  decoding, index catalog reads, and candidate gathering. Shared query
  filtering, ordering, cursors, and projection are product spans on
  `vial_keeper.query.execute`, not SQLite phase probes.

  The phase names are intentionally fixed so trace searches remain stable:

    * `vial_keeper.sqlite.document.lookup`
    * `vial_keeper.sqlite.document.leaves`
    * `vial_keeper.sqlite.revision.lookup`
    * `vial_keeper.sqlite.mutation.bulk.prepare`
    * `vial_keeper.sqlite.mutation.bulk.finalize`
    * `vial_keeper.sqlite.changes.identity`
    * `vial_keeper.sqlite.changes.fetch`
    * `vial_keeper.sqlite.changes.decode`
    * `vial_keeper.sqlite.changes.has_more`
    * `vial_keeper.sqlite.query.prepare_request`
    * `vial_keeper.sqlite.query.identity`
    * `vial_keeper.sqlite.query.index_catalog`
    * `vial_keeper.sqlite.query.plan`
    * `vial_keeper.sqlite.query.candidates`
    * `vial_keeper.sqlite.query.filter`
    * `vial_keeper.sqlite.query.sort`
    * `vial_keeper.sqlite.query.cursor`
    * `vial_keeper.sqlite.query.project`
    * `vial_keeper.sqlite.transaction.begin`
    * `vial_keeper.sqlite.transaction.commit`
    * `vial_keeper.sqlite.transaction.rollback`
  """

  alias VialKeeper.Observability.Tracer
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
    document_lookup: "vial_keeper.sqlite.document.lookup",
    document_leaves: "vial_keeper.sqlite.document.leaves",
    revision_lookup: "vial_keeper.sqlite.revision.lookup",
    bulk_prepare: "vial_keeper.sqlite.mutation.bulk.prepare",
    bulk_finalize: "vial_keeper.sqlite.mutation.bulk.finalize",
    changes_identity: "vial_keeper.sqlite.changes.identity",
    changes_fetch: "vial_keeper.sqlite.changes.fetch",
    changes_decode: "vial_keeper.sqlite.changes.decode",
    changes_has_more: "vial_keeper.sqlite.changes.has_more",
    query_prepare_request: "vial_keeper.sqlite.query.prepare_request",
    query_identity: "vial_keeper.sqlite.query.identity",
    query_index_catalog: "vial_keeper.sqlite.query.index_catalog",
    query_plan: "vial_keeper.sqlite.query.plan",
    query_candidates: "vial_keeper.sqlite.query.candidates",
    query_filter: "vial_keeper.sqlite.query.filter",
    query_sort: "vial_keeper.sqlite.query.sort",
    query_cursor: "vial_keeper.sqlite.query.cursor",
    query_project: "vial_keeper.sqlite.query.project",
    transaction_begin: "vial_keeper.sqlite.transaction.begin",
    transaction_commit: "vial_keeper.sqlite.transaction.commit",
    transaction_rollback: "vial_keeper.sqlite.transaction.rollback"
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

  defp record_phase_error({:error, %VialKeeper.Error{} = error}) do
    _ = Tracer.record_error(error)
    _ = Tracer.apply_error_status(error)
    :ok
  end

  defp record_phase_error(_result), do: :ok
end
