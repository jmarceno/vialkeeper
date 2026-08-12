defmodule ElixirDB.Observability.Instrumentation.Attachment do
  @moduledoc """
  Emitters for attachment byte-path observability:

    * `elixir_db.attachment.read`  — span + counter + histogram
    * `elixir_db.attachment.write` — span + counter + histogram
    * `elixir_db.attachment.gc`    — span + counter + histogram

  Attributes are privacy-bounded: never attachment names, digests, paths,
  document ids, content types, or bodies. Allowed measurements are counts,
  sizes, durations, and low-cardinality booleans (`compressed`, `deduplicated`).
  """

  require OpenTelemetry.Tracer

  alias ElixirDB.Observability.{Attributes, Meters, Tracer}

  @read_span "elixir_db.attachment.read"
  @write_span "elixir_db.attachment.write"
  @gc_span "elixir_db.attachment.gc"

  @read_count :"elixir_db.attachment.read.count"
  @read_duration :"elixir_db.attachment.read.duration"
  @write_count :"elixir_db.attachment.write.count"
  @write_duration :"elixir_db.attachment.write.duration"
  @gc_count :"elixir_db.attachment.gc.count"
  @gc_duration :"elixir_db.attachment.gc.duration"

  @type read_handle :: %{
          span_ctx: term(),
          started: integer(),
          db_uuid: binary()
        }

  @doc """
  Wraps an attachment write (upload or replication put) and records bounded
  size/chunk/encoding attributes from a successful result map.
  """
  @spec write(binary(), (-> result)) :: result when result: term()
  def write(uuid, fun) when is_binary(uuid) and is_function(fun, 0) do
    started = System.monotonic_time()

    Tracer.with_span(@write_span, [db_uuid: uuid], fn ->
      result = fun.()
      duration = System.monotonic_time() - started
      emit_write(uuid, duration, result)
      result
    end)
  end

  @doc """
  Starts an attachment read span that outlives ticket/open and ends when the
  stream finalizer runs (or on cancel/error before streaming starts).
  """
  @spec start_read(binary()) :: read_handle()
  def start_read(uuid) when is_binary(uuid) do
    started = System.monotonic_time()
    attrs = Attributes.build(db_uuid: uuid)

    span_ctx =
      OpenTelemetry.Tracer.start_span(@read_span, %{kind: :internal, attributes: attrs})

    %{span_ctx: span_ctx, started: started, db_uuid: uuid}
  end

  @doc "Ends a read span after successful stream open with bounded ticket sizes."
  @spec finish_read_open(read_handle(), keyword()) :: read_handle()
  def finish_read_open(%{span_ctx: span_ctx} = handle, attrs) when is_list(attrs) do
    previous = OpenTelemetry.Tracer.set_current_span(span_ctx)
    _ = Tracer.set_attributes(attrs)
    _ = OpenTelemetry.Tracer.set_current_span(previous)
    handle
  end

  @doc "Ends a read span when the body stream completes or is abandoned."
  @spec end_read(read_handle(), keyword()) :: :ok
  def end_read(%{span_ctx: span_ctx, started: started, db_uuid: uuid}, attrs \\ [])
      when is_list(attrs) do
    if already_ended?(span_ctx) do
      :ok
    else
      mark_ended(span_ctx)
      duration = System.monotonic_time() - started
      previous = OpenTelemetry.Tracer.set_current_span(span_ctx)

      try do
        emit_attrs =
          [db_uuid: uuid, outcome: :ok]
          |> Keyword.merge(
            Keyword.take(attrs, [
              :logical_bytes,
              :stream_chunks,
              :compressed,
              :outcome,
              :error_code
            ])
          )

        _ = Tracer.set_attributes(emit_attrs)
        Meters.add(@read_count, emit_attrs)
        Meters.record(@read_duration, duration, emit_attrs)
      after
        OpenTelemetry.Span.end_span(span_ctx)
        _ = OpenTelemetry.Tracer.set_current_span(previous)
      end

      :ok
    end
  end

  @doc "Ends a read span after an open/resolve failure (before streaming)."
  @spec fail_read(read_handle(), ElixirDB.Error.t()) :: :ok
  def fail_read(%{span_ctx: span_ctx, started: started, db_uuid: uuid}, %ElixirDB.Error{} = error) do
    if already_ended?(span_ctx) do
      :ok
    else
      mark_ended(span_ctx)
      duration = System.monotonic_time() - started
      previous = OpenTelemetry.Tracer.set_current_span(span_ctx)

      try do
        attrs = [db_uuid: uuid, outcome: :rejected, error_code: error.code]
        _ = Tracer.record_error(error)
        _ = Tracer.apply_error_status(error)
        Meters.add(@read_count, attrs)
        Meters.record(@read_duration, duration, attrs)
      after
        OpenTelemetry.Span.end_span(span_ctx)
        _ = OpenTelemetry.Tracer.set_current_span(previous)
      end

      :ok
    end
  end

  defp already_ended?(span_ctx), do: Process.get({:attachment_read_ended, span_ctx}) == true

  defp mark_ended(span_ctx), do: Process.put({:attachment_read_ended, span_ctx}, true)

  @doc """
  Wraps attachment GC. Track A owns `Attachments.gc/1`; call this around the GC
  body so spans/metrics emit once GC is implemented.
  """
  @spec gc(binary(), (-> {:ok, map()} | {:error, ElixirDB.Error.t()})) ::
          {:ok, map()} | {:error, ElixirDB.Error.t()}
  def gc(uuid, fun) when is_binary(uuid) and is_function(fun, 0) do
    started = System.monotonic_time()

    Tracer.with_span(@gc_span, [db_uuid: uuid], fn ->
      result = fun.()
      duration = System.monotonic_time() - started
      emit_gc(uuid, duration, result)
      result
    end)
  end

  defp emit_write(uuid, duration, {:ok, map}) when is_map(map) do
    attrs = write_attrs(uuid, map, :ok)
    Meters.add(@write_count, attrs)
    Meters.record(@write_duration, duration, attrs)
    _ = Tracer.set_attributes(attrs)
    :ok
  end

  defp emit_write(uuid, duration, {:error, %ElixirDB.Error{} = error}) do
    attrs = [db_uuid: uuid, outcome: :rejected, error_code: error.code]
    Meters.add(@write_count, attrs)
    Meters.record(@write_duration, duration, attrs)
    _ = Tracer.record_error(error)
    _ = Tracer.apply_error_status(error)
    :ok
  end

  defp emit_write(_uuid, _duration, _other), do: :ok

  defp emit_gc(uuid, duration, {:ok, stats}) when is_map(stats) do
    attrs = [
      db_uuid: uuid,
      outcome: :ok,
      blobs_deleted: Map.get(stats, :blobs_deleted, 0),
      bytes_deleted: Map.get(stats, :bytes_deleted, 0)
    ]

    Meters.add(@gc_count, attrs)
    Meters.record(@gc_duration, duration, attrs)
    _ = Tracer.set_attributes(attrs)
    :ok
  end

  defp emit_gc(uuid, duration, {:error, %ElixirDB.Error{} = error}) do
    attrs = [db_uuid: uuid, outcome: :rejected, error_code: error.code]
    Meters.add(@gc_count, attrs)
    Meters.record(@gc_duration, duration, attrs)
    _ = Tracer.record_error(error)
    _ = Tracer.apply_error_status(error)
    :ok
  end

  defp emit_gc(_uuid, _duration, _other), do: :ok

  defp write_attrs(uuid, map, outcome) do
    [
      db_uuid: uuid,
      outcome: outcome,
      logical_bytes: ElixirDB.MapAccess.get_first(map, [:length, :logical_size]),
      stream_chunks: Map.get(map, :stream_chunks),
      compressed: Map.get(map, :encoding) in [:compressed, :zstd],
      deduplicated: Map.get(map, :deduplicated?) == true
    ]
  end
end
