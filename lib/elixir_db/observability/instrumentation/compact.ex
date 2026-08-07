defmodule ElixirDB.Observability.Instrumentation.Compact do
  @moduledoc """
  Emitters for Plan §11 / Architecture §20.5 compact-retention events:

    * `elixir_db.database.compact` — span + counter + histogram

  Wraps compact-retention through the database owner. Scheduled and explicit
  runs share the same instrumentation path.
  """

  alias ElixirDB.Observability.{Meters, Tracer}

  @compact_span "elixir_db.database.compact"
  @count_metric :"elixir_db.database.compact.count"
  @duration_metric :"elixir_db.database.compact.duration"

  @doc """
  Records a scheduled compaction request before the owner command is issued.
  """
  @spec requested(binary(), :scheduled | :explicit) :: :ok
  def requested(uuid, trigger) when is_binary(uuid) and trigger in [:scheduled, :explicit] do
    Meters.add(@count_metric, db_uuid: uuid, outcome: :requested, trigger: trigger)
    :ok
  end

  @doc """
  Wraps a compact-retention command in the compact span and records duration and
  outcome counters with bounded compaction statistics.
  """
  @spec run(binary(), :scheduled | :explicit, (-> {:ok, map()} | {:error, ElixirDB.Error.t()})) ::
          {:ok, map()} | {:error, ElixirDB.Error.t()}
  def run(uuid, trigger, fun)
      when is_binary(uuid) and trigger in [:scheduled, :explicit] and is_function(fun, 0) do
    started = System.monotonic_time()
    base_attrs = [db_uuid: uuid, trigger: trigger]

    Tracer.with_span(@compact_span, base_attrs, fn ->
      result = fun.()
      duration = System.monotonic_time() - started
      emit_result(uuid, trigger, duration, result)
      result
    end)
  end

  defp emit_result(uuid, trigger, duration, {:ok, stats}) when is_map(stats) do
    outcome = if Map.get(stats, :noop?, Map.get(stats, "noop", false)), do: :noop, else: :committed
    attrs = compaction_attrs(uuid, trigger, stats, outcome)

    Meters.add(@count_metric, attrs)
    Meters.record(@duration_metric, duration, attrs)

    floor_changed = Map.get(stats, :new_floor, 0) > Map.get(stats, :old_floor, 0)

    epoch_changed =
      Map.get(stats, :new_compaction_epoch, 0) > Map.get(stats, :old_compaction_epoch, 0)

    span_attrs =
      attrs
      |> Keyword.take([
        :outcome,
        :trigger,
        :removed_changes,
        :removed_revisions,
        :removed_boundaries
      ])
      |> Keyword.merge(
        floor_changed: floor_changed,
        compaction_epoch_changed: epoch_changed,
        old_floor: Map.get(stats, :old_floor),
        new_floor: Map.get(stats, :new_floor),
        old_compaction_epoch: Map.get(stats, :old_compaction_epoch),
        new_compaction_epoch: Map.get(stats, :new_compaction_epoch)
      )

    _ = Tracer.set_attributes(span_attrs)
    :ok
  end

  defp emit_result(uuid, trigger, duration, {:error, %ElixirDB.Error{} = error}) do
    attrs = [db_uuid: uuid, trigger: trigger, outcome: :failed, error_code: error.code]

    Meters.add(@count_metric, attrs)
    Meters.record(@duration_metric, duration, attrs)

    _ = Tracer.record_error(error)
    _ = Tracer.apply_error_status(error)
    :ok
  end

  defp compaction_attrs(uuid, trigger, stats, outcome) do
    [
      db_uuid: uuid,
      trigger: trigger,
      outcome: outcome,
      removed_changes: Map.get(stats, :removed_changes, 0),
      removed_revisions: Map.get(stats, :removed_revisions, 0),
      removed_boundaries: Map.get(stats, :removed_boundaries, 0),
      active_peer_count: Map.get(stats, :active_peer_count, 0),
      expired_peer_count: Map.get(stats, :expired_peer_count, 0),
      blocking_peer_count: Map.get(stats, :blocking_peer_count, 0),
      bootstrap_required_count: Map.get(stats, :bootstrap_required_count, 0)
    ]
  end
end
