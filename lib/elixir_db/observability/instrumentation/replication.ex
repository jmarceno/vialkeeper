defmodule ElixirDB.Observability.Instrumentation.Replication do
  @moduledoc """
  Emitters for Plan §11 replication events:

    * `elixir_db.replication.batch`      — span + histogram (started/ended by
      `ElixirDB.Replication.Worker` so it wraps the full batch cycle and phase
      tasks inherit its trace context)
    * `elixir_db.replication.checkpoint` — span + counter (wraps each
      `put_checkpoint` CAS write)

  Both spans are children of the worker's trace context, which is detached/
  re-attached across the worker's async phase tasks (see Worker.start_batch_span).
  """

  alias ElixirDB.Observability.{Meters, Tracer}

  @checkpoint_span "elixir_db.replication.checkpoint"

  @doc """
  Wraps a checkpoint CAS write (`put_checkpoint`) in the
  `elixir_db.replication.checkpoint` span and increments the counter. Migrated
  from the former bare telemetry checkpoint emitter; the non-allowlisted
  `source_sequence` metadata is NOT carried.

  The span wraps the actual endpoint call so its duration is real and failures
  set error.code/status (§5.6, §6.5). Returns the fun's result.
  """
  @spec checkpoint_span(binary(), :source | :target, (-> result)) :: result when result: term()
  def checkpoint_span(replication_id, endpoint, fun)
      when is_binary(replication_id) and endpoint in [:source, :target] and is_function(fun, 0) do
    Tracer.with_span(
      @checkpoint_span,
      [replication_id: replication_id, endpoint: endpoint],
      fn ->
        result = fun.()

        case result do
          {:ok, _} ->
            Meters.add(:"elixir_db.replication.checkpoint.count",
              replication_id: replication_id,
              endpoint: endpoint
            )

          {:error, %ElixirDB.Error{} = error} ->
            Meters.add(:"elixir_db.replication.checkpoint.count",
              replication_id: replication_id,
              endpoint: endpoint,
              error_code: error.code
            )

            _ = Tracer.record_error(error)
            _ = Tracer.apply_error_status(error)
        end

        result
      end
    )
  end
end
