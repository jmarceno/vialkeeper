defmodule VialKeeper.Replication.CheckpointReconciler do
  @moduledoc "Pure checkpoint reconciliation decisions for replication catch-up."

  alias VialKeeper.MapAccess

  @type reconcile_result :: %{
          since: non_neg_integer(),
          bootstrap_required: boolean(),
          reason: :ok | :epoch_mismatch | :below_floor | :no_common_history,
          source_history_epoch: binary() | nil,
          source_compaction_epoch: non_neg_integer(),
          retention_floor: non_neg_integer()
        }

  @spec common_sequence(map() | nil, map() | nil) :: non_neg_integer()
  def common_sequence(nil, _target), do: 0
  def common_sequence(_source, nil), do: 0

  def common_sequence(source, target) when is_map(source) and is_map(target) do
    source_history = MapAccess.get(source, :history, [])
    target_history = MapAccess.get(target, :history, [])

    source_history
    |> Enum.sort_by(&MapAccess.get(&1, :source_sequence, 0), :desc)
    |> Enum.find_value(0, fn source_entry ->
      if Enum.any?(target_history, &same_session?(&1, source_entry)),
        do: MapAccess.get(source_entry, :source_sequence, 0),
        else: nil
    end)
  end

  # SAFETY: a remote peer may persist a checkpoint value that is not a map (a scalar or
  # list). Rather than raise BadMapError when indexing [:history], degrade to "no common
  # sequence" so the worker retries from scratch.
  def common_sequence(_source, _target), do: 0

  @spec reconcile(map() | nil, map() | nil, map()) :: reconcile_result()
  def reconcile(source_checkpoint, target_checkpoint, source_identity)
      when is_map(source_identity) do
    source_value = checkpoint_value(source_checkpoint)
    target_value = checkpoint_value(target_checkpoint)
    source_epoch = identity_epoch(source_identity)
    floor = retention_floor(source_identity)
    compaction_epoch = MapAccess.get(source_identity, :compaction_epoch, 0) || 0
    checkpoint_epoch = checkpoint_epoch_for(target_value, source_value)
    since = common_sequence(source_value, target_value)

    reconcile_decision(checkpoint_epoch, source_epoch, floor, compaction_epoch, since)
  end

  defp reconcile_decision(checkpoint_epoch, source_epoch, floor, compaction_epoch, since) do
    bootstrap = fn reason ->
      reconcile_result(floor, true, reason, source_epoch, compaction_epoch, floor)
    end

    cond do
      epoch_mismatch?(checkpoint_epoch, source_epoch) or missing_epoch?(checkpoint_epoch) ->
        bootstrap.(:epoch_mismatch)

      floor > 0 and since < floor ->
        bootstrap.(:below_floor)

      since == 0 and floor > 0 and no_valid_epoch?(checkpoint_epoch, source_epoch) ->
        bootstrap.(:no_common_history)

      true ->
        reconcile_result(max(since, floor), false, :ok, source_epoch, compaction_epoch, floor)
    end
  end

  defp reconcile_result(
         since,
         bootstrap_required,
         reason,
         source_history_epoch,
         source_compaction_epoch,
         retention_floor
       ) do
    %{
      since: since,
      bootstrap_required: bootstrap_required,
      reason: reason,
      source_history_epoch: source_history_epoch,
      source_compaction_epoch: source_compaction_epoch,
      retention_floor: retention_floor
    }
  end

  defp checkpoint_value(nil), do: nil
  defp checkpoint_value(%{value: value}), do: value
  defp checkpoint_value(%{"value" => value}), do: value
  defp checkpoint_value(value) when is_map(value), do: value
  defp checkpoint_value(_), do: nil

  defp identity_epoch(identity) do
    MapAccess.get(identity, :history_epoch) || MapAccess.get(identity, "history_epoch")
  end

  defp retention_floor(identity) do
    MapAccess.get(identity, :retention_floor_sequence) ||
      MapAccess.get(identity, :retention_floor) ||
      MapAccess.get(identity, "retention_floor") ||
      MapAccess.get(identity, "retention_floor_sequence") ||
      0
  end

  defp checkpoint_epoch_for(target_value, source_value) do
    MapAccess.get(target_value, :source_history_epoch) ||
      MapAccess.get(target_value, "source_history_epoch") ||
      MapAccess.get(source_value, :source_history_epoch) ||
      MapAccess.get(source_value, "source_history_epoch")
  end

  defp epoch_mismatch?(checkpoint_epoch, source_epoch) do
    is_binary(checkpoint_epoch) and checkpoint_epoch != "" and is_binary(source_epoch) and
      checkpoint_epoch != source_epoch
  end

  defp missing_epoch?(checkpoint_epoch),
    do: not is_binary(checkpoint_epoch) or checkpoint_epoch == ""

  defp no_valid_epoch?(checkpoint_epoch, source_epoch) do
    not is_binary(checkpoint_epoch) or checkpoint_epoch == "" or checkpoint_epoch != source_epoch
  end

  defp same_session?(left, right) when is_map(left) and is_map(right),
    do:
      MapAccess.get(left, :session_id) == MapAccess.get(right, :session_id) and
        MapAccess.get(left, :source_sequence) == MapAccess.get(right, :source_sequence)

  # SAFETY: a malformed checkpoint history may contain non-map entries. Treat any
  # non-map entry as "not the same session" instead of raising BadMapError.
  defp same_session?(_left, _right), do: false
end
