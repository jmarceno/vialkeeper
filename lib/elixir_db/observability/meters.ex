defmodule ElixirDB.Observability.Meters do
  @moduledoc """
  Counter and histogram declarations for Plan §11 metrics.

  Instruments are created lazily and cached in a persistent term-backed table so
  repeated hot-path calls do not allocate. When no SDK is running (no collector
  configured), the meter resolves to the no-op meter and every operation is a
  safe no-op.

  ## Metric catalog (Plan §3)

  | Metric | Kind |
  |---|---|
  | `elixir_db.database.open.count` | counter |
  | `elixir_db.database.command.duration` | histogram |
  | `elixir_db.database.overload.count` | counter |
  | `elixir_db.database.compact.count` | counter |
  | `elixir_db.database.compact.duration` | histogram |
  | `elixir_db.changes.read.duration` | histogram |
  | `elixir_db.changes.history_truncated.count` | counter |
  | `elixir_db.query.execute.duration` | histogram |
  | `elixir_db.index.build.duration` | histogram |
  | `elixir_db.replication.batch.duration` | histogram |
  | `elixir_db.replication.checkpoint.count` | counter |
  | `elixir_db.http.request.duration` | histogram |
  | `elixir_db.attachment.read.count` | counter |
  | `elixir_db.attachment.read.duration` | histogram |
  | `elixir_db.attachment.write.count` | counter |
  | `elixir_db.attachment.write.duration` | histogram |
  | `elixir_db.attachment.gc.count` | counter |
  | `elixir_db.attachment.gc.duration` | histogram |
  | `elixir_db.replication.blob.transfer.count` | counter |
  | `elixir_db.replication.blob.transfer.duration` | histogram |
  """

  alias ElixirDB.Observability.Attributes

  @instruments [
    {:"elixir_db.database.open.count", :counter},
    {:"elixir_db.database.command.duration", :histogram},
    {:"elixir_db.database.overload.count", :counter},
    {:"elixir_db.database.compact.count", :counter},
    {:"elixir_db.database.compact.duration", :histogram},
    {:"elixir_db.changes.read.duration", :histogram},
    {:"elixir_db.changes.history_truncated.count", :counter},
    {:"elixir_db.query.execute.duration", :histogram},
    {:"elixir_db.index.build.duration", :histogram},
    {:"elixir_db.replication.batch.duration", :histogram},
    {:"elixir_db.replication.checkpoint.count", :counter},
    {:"elixir_db.replication.bootstrap.count", :counter},
    {:"elixir_db.import.stale_fence.count", :counter},
    {:"elixir_db.http.request.duration", :histogram},
    {:"elixir_db.attachment.read.count", :counter},
    {:"elixir_db.attachment.read.duration", :histogram},
    {:"elixir_db.attachment.write.count", :counter},
    {:"elixir_db.attachment.write.duration", :histogram},
    {:"elixir_db.attachment.gc.count", :counter},
    {:"elixir_db.attachment.gc.duration", :histogram},
    {:"elixir_db.replication.blob.transfer.count", :counter},
    {:"elixir_db.replication.blob.transfer.duration", :histogram}
  ]

  @doc "Returns the instrument catalog declared by this module."
  @spec instruments() :: [{atom(), :counter | :histogram}]
  def instruments, do: @instruments

  @doc "Increments the named counter by 1 with allow-listed `attrs`."
  @spec add(atom(), keyword()) :: :ok
  def add(name, attrs \\ [])

  def add(name, attrs) when is_atom(name) and is_list(attrs) do
    case instrument(name, :counter) do
      nil ->
        :ok

      instrument ->
        ctx = OpenTelemetry.Ctx.get_current()
        :otel_counter.add(ctx, instrument, 1, Attributes.build(attrs))
    end
  end

  @doc "Records `value` on the named histogram with allow-listed `attrs`."
  @spec record(atom(), number(), keyword()) :: :ok
  def record(name, value, attrs \\ [])

  def record(name, value, attrs)
      when is_atom(name) and is_number(value) and is_list(attrs) do
    case instrument(name, :histogram) do
      nil ->
        :ok

      instrument ->
        ctx = OpenTelemetry.Ctx.get_current()
        :otel_histogram.record(ctx, instrument, value, Attributes.build(attrs))
    end
  end

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
