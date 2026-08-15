defmodule VialKeeper.Observability.Meters do
  @moduledoc """
  Counter and histogram declarations for database observability.

  Instruments are created lazily and cached in a persistent term-backed table so
  repeated hot-path calls do not allocate. When no SDK is running (no collector
  configured), the meter resolves to the no-op meter and every operation is a
  safe no-op.

  ## Metric catalog

  | Metric | Kind |
  |---|---|
  | `vial_keeper.database.open.count` | counter |
  | `vial_keeper.database.command.duration` | histogram |
  | `vial_keeper.database.overload.count` | counter |
  | `vial_keeper.database.admission.wait` | histogram |
  | `vial_keeper.database.read_pool.wait` | histogram |
  | `vial_keeper.database.read_pool.active` | updown counter |
  | `vial_keeper.database.read_pool.queued` | updown counter |
  | `vial_keeper.database.read_pool.quiesce.duration` | histogram |
  | `vial_keeper.database.compact.count` | counter |
  | `vial_keeper.database.compact.duration` | histogram |
  | `vial_keeper.changes.read.duration` | histogram |
  | `vial_keeper.changes.history_truncated.count` | counter |
  | `vial_keeper.query.execute.duration` | histogram |
  | `vial_keeper.federation.query.duration` | histogram |
  | `vial_keeper.derived_view.batch.duration` | histogram |
  | `vial_keeper.index.build.duration` | histogram |
  | `vial_keeper.search.rebuild.count` | counter |
  | `vial_keeper.search.rebuild.duration` | histogram |
  | `vial_keeper.replication.batch.duration` | histogram |
  | `vial_keeper.replication.transfer.count` | counter |
  | `vial_keeper.replication.transfer.duration` | histogram |
  | `vial_keeper.replication.checkpoint.count` | counter |
  | `vial_keeper.http.request.duration` | histogram |
  | `vial_keeper.shadow.read.count` | counter |
  | `vial_keeper.shadow.route.fallback.count` | counter |
  | `vial_keeper.attachment.read.count` | counter |
  | `vial_keeper.attachment.read.duration` | histogram |
  | `vial_keeper.attachment.write.count` | counter |
  | `vial_keeper.attachment.write.duration` | histogram |
  | `vial_keeper.attachment.gc.count` | counter |
  | `vial_keeper.attachment.gc.duration` | histogram |
  | `vial_keeper.replication.blob.transfer.count` | counter |
  | `vial_keeper.replication.blob.transfer.duration` | histogram |
  | `vial_keeper.replication.wire.bytes` | histogram |
  | `vial_keeper.replication.wire.codec.duration` | histogram |
  | `vial_keeper.query.subscription.open` | counter |
  | `vial_keeper.query.subscription.update` | counter |
  | `vial_keeper.query.subscription.overload` | counter |
  """

  alias VialKeeper.Observability.Attributes

  @instruments [
    {:"vial_keeper.database.open.count", :counter},
    {:"vial_keeper.database.command.duration", :histogram},
    {:"vial_keeper.database.overload.count", :counter},
    {:"vial_keeper.database.admission.wait", :histogram},
    {:"vial_keeper.database.read_pool.wait", :histogram},
    {:"vial_keeper.database.read_pool.active", :updown_counter},
    {:"vial_keeper.database.read_pool.queued", :updown_counter},
    {:"vial_keeper.database.read_pool.quiesce.duration", :histogram},
    {:"vial_keeper.database.compact.count", :counter},
    {:"vial_keeper.database.compact.duration", :histogram},
    {:"vial_keeper.changes.read.duration", :histogram},
    {:"vial_keeper.changes.history_truncated.count", :counter},
    {:"vial_keeper.query.execute.duration", :histogram},
    {:"vial_keeper.federation.query.duration", :histogram},
    {:"vial_keeper.derived_view.batch.duration", :histogram},
    {:"vial_keeper.index.build.duration", :histogram},
    {:"vial_keeper.search.rebuild.count", :counter},
    {:"vial_keeper.search.rebuild.duration", :histogram},
    {:"vial_keeper.replication.batch.duration", :histogram},
    {:"vial_keeper.replication.transfer.count", :counter},
    {:"vial_keeper.replication.transfer.duration", :histogram},
    {:"vial_keeper.replication.checkpoint.count", :counter},
    {:"vial_keeper.replication.bootstrap.count", :counter},
    {:"vial_keeper.import.stale_fence.count", :counter},
    {:"vial_keeper.http.request.duration", :histogram},
    {:"vial_keeper.shadow.read.count", :counter},
    {:"vial_keeper.shadow.route.fallback.count", :counter},
    {:"vial_keeper.attachment.read.count", :counter},
    {:"vial_keeper.attachment.read.duration", :histogram},
    {:"vial_keeper.attachment.write.count", :counter},
    {:"vial_keeper.attachment.write.duration", :histogram},
    {:"vial_keeper.attachment.gc.count", :counter},
    {:"vial_keeper.attachment.gc.duration", :histogram},
    {:"vial_keeper.replication.blob.transfer.count", :counter},
    {:"vial_keeper.replication.blob.transfer.duration", :histogram},
    {:"vial_keeper.replication.wire.bytes", :histogram},
    {:"vial_keeper.replication.wire.codec.duration", :histogram},
    {:"vial_keeper.query.subscription.open", :counter},
    {:"vial_keeper.query.subscription.update", :counter},
    {:"vial_keeper.query.subscription.overload", :counter},
    {:"vial_keeper.view.builder.started", :counter},
    {:"vial_keeper.view.rebuild.activated", :counter},
    {:"vial_keeper.view.batch.applied", :counter},
    {:"vial_keeper.view.history_truncated", :counter}
  ]

  @doc "Returns the instrument catalog declared by this module."
  @spec instruments() :: [{atom(), :counter | :histogram | :updown_counter}]
  def instruments, do: @instruments

  @doc "Increments the named counter by 1 with allow-listed `attrs`."
  @spec add(atom(), keyword()) :: :ok
  def add(name, attrs \\ [])

  def add(name, attrs) when is_atom(name) and is_list(attrs) do
    if metrics_enabled?() do
      case instrument(name, :counter) do
        nil ->
          :ok

        instrument ->
          ctx = OpenTelemetry.Ctx.get_current()
          :otel_counter.add(ctx, instrument, 1, Attributes.build(attrs))
      end
    else
      :ok
    end
  end

  @doc "Records `value` on the named histogram with allow-listed `attrs`."
  @spec record(atom(), number(), keyword()) :: :ok
  def record(name, value, attrs \\ [])

  def record(name, value, attrs)
      when is_atom(name) and is_number(value) and is_list(attrs) do
    if metrics_enabled?() do
      case instrument(name, :histogram) do
        nil ->
          :ok

        instrument ->
          ctx = OpenTelemetry.Ctx.get_current()
          :otel_histogram.record(ctx, instrument, value, Attributes.build(attrs))
      end
    else
      :ok
    end
  end

  @doc "Adds `value` (positive or negative) to the named up-down counter."
  @spec updown_add(atom(), integer(), keyword()) :: :ok
  def updown_add(name, value, attrs \\ [])

  def updown_add(name, value, attrs)
      when is_atom(name) and is_integer(value) and is_list(attrs) do
    if metrics_enabled?() do
      case instrument(name, :updown_counter) do
        nil ->
          :ok

        instrument ->
          ctx = OpenTelemetry.Ctx.get_current()
          :otel_updown_counter.add(ctx, instrument, value, Attributes.build(attrs))
      end
    else
      :ok
    end
  end

  defp metrics_enabled?,
    do: Application.get_env(:opentelemetry_experimental, :readers, []) not in [[], nil]

  defp instrument(name, kind) do
    case :persistent_term.get({__MODULE__, name}, nil) do
      nil -> create_instrument(name, kind)
      instrument -> instrument
    end
  end

  defp create_instrument(name, kind) do
    # Only create instruments whose name is in our catalog AND matches the
    # requested kind. Unknown/mismatched names return nil and become no-ops,
    # so a typo at an emission site can never crash a hot path.
    instrument =
      if {name, kind} in @instruments do
        try do
          meter = :opentelemetry_experimental.get_meter(__MODULE__)
          opts = %{description: Atom.to_string(name)}

          case kind do
            :counter -> :otel_meter.create_counter(meter, name, opts)
            :histogram -> :otel_meter.create_histogram(meter, name, opts)
            :updown_counter -> :otel_meter.create_updown_counter(meter, name, opts)
          end
        catch
          # No SDK running; meter provider unavailable. Emission is a no-op.
          :exit, _ -> nil
          :error, _ -> nil
        end
      else
        nil
      end

    :persistent_term.put({__MODULE__, name}, instrument)
    instrument
  end
end
