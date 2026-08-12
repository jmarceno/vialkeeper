defmodule ElixirDB.Observability.Instrumentation.Replication do
  @moduledoc """
  Emitters for replication events:

    * `elixir_db.replication.batch`      — span + histogram (started/ended by
      `ElixirDB.Replication.Worker` so it wraps the full batch cycle and phase
      tasks inherit its trace context)
    * `elixir_db.replication.checkpoint` — span + counter (wraps each
      `put_checkpoint` CAS write)
    * `elixir_db.replication.transfer` — span + histogram for one transfer barrier
    * `elixir_db.replication.blob.transfer` — span + counter + histogram for
      each missing-blob transfer (privacy: no digests/paths/bodies)

  Both spans are children of the worker's trace context, which is detached/
  re-attached across the worker's async phase tasks (see Worker.start_batch_span).
  """

  alias ElixirDB.Observability.{Meters, Tracer}

  @checkpoint_span "elixir_db.replication.checkpoint"
  @transfer_span "elixir_db.replication.transfer"
  @transfer_count :"elixir_db.replication.transfer.count"
  @transfer_duration :"elixir_db.replication.transfer.duration"
  @blob_transfer_span "elixir_db.replication.blob.transfer"
  @blob_transfer_count :"elixir_db.replication.blob.transfer.count"
  @blob_transfer_duration :"elixir_db.replication.blob.transfer.duration"

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

  @doc """
  Wraps one bounded transfer barrier and records its aggregate measurements.

  Only allow-listed bounded values are emitted. The triple return form is an
  internal pipeline seam and is reduced to the normal phase result.
  """
  @spec transfer_span(binary(), (-> result)) :: result when result: term()
  def transfer_span(replication_id, fun)
      when is_binary(replication_id) and is_function(fun, 0) do
    started = System.monotonic_time()

    Tracer.with_span(@transfer_span, [replication_id: replication_id], fn ->
      result = fun.()
      duration = System.monotonic_time() - started
      emit_transfer(result, duration, replication_id)
      result
    end)
  end

  defp emit_transfer({:ok, _context, measurements}, duration, replication_id)
       when is_list(measurements) do
    attrs = Keyword.put(measurements, :replication_id, replication_id)
    Meters.add(@transfer_count, attrs)
    Meters.record(@transfer_duration, duration, attrs)
    _ = Tracer.set_attributes(attrs)
    :ok
  end

  defp emit_transfer({:error, %ElixirDB.Error{} = error, measurements}, duration, replication_id)
       when is_list(measurements) do
    attrs =
      Keyword.merge(measurements,
        replication_id: replication_id,
        outcome: :rejected,
        error_code: error.code
      )

    Meters.add(@transfer_count, attrs)
    Meters.record(@transfer_duration, duration, attrs)
    _ = Tracer.set_attributes(attrs)
    _ = Tracer.record_error(error)
    _ = Tracer.apply_error_status(error)
    :ok
  end

  defp emit_transfer(_result, _duration, _replication_id), do: :ok

  @doc """
  Wraps one replication blob transfer with bounded size/duration metrics.

  Callers may pass `:logical_bytes` only — never digests, names, or paths.
  """
  @spec blob_transfer_span(binary(), keyword(), (-> result)) :: result when result: term()
  def blob_transfer_span(replication_id, attrs \\ [], fun)
      when is_binary(replication_id) and is_list(attrs) and is_function(fun, 0) do
    started = System.monotonic_time()

    base =
      Keyword.merge(
        [replication_id: replication_id],
        Keyword.take(attrs, [:logical_bytes, :payload_length])
      )

    Tracer.with_span(@blob_transfer_span, base, fn ->
      result = fun.()
      duration = System.monotonic_time() - started
      emit_blob_transfer(base, duration, result)
      result
    end)
  end

  defp emit_blob_transfer(base, duration, :ok) do
    attrs = Keyword.put(base, :outcome, :ok)
    Meters.add(@blob_transfer_count, attrs)
    Meters.record(@blob_transfer_duration, duration, attrs)
    _ = Tracer.set_attributes(attrs)
    :ok
  end

  defp emit_blob_transfer(base, duration, {:ok, map}) when is_map(map) do
    attrs =
      base
      |> Keyword.put(:outcome, :ok)
      |> Keyword.put(:logical_bytes, Map.get(map, :logical_bytes) || Map.get(map, :length))

    Meters.add(@blob_transfer_count, attrs)
    Meters.record(@blob_transfer_duration, duration, attrs)
    _ = Tracer.set_attributes(attrs)
    :ok
  end

  defp emit_blob_transfer(base, duration, {:error, %ElixirDB.Error{} = error}) do
    attrs = Keyword.merge(base, outcome: :rejected, error_code: error.code)
    Meters.add(@blob_transfer_count, attrs)
    Meters.record(@blob_transfer_duration, duration, attrs)
    _ = Tracer.record_error(error)
    _ = Tracer.apply_error_status(error)
    :ok
  end

  defp emit_blob_transfer(_base, _duration, _other), do: :ok
end
