defmodule ElixirDB.Replication.CheckpointReconciler do
  @moduledoc false

  def common_sequence(nil, _target), do: 0
  def common_sequence(_source, nil), do: 0

  def common_sequence(source, target) when is_map(source) and is_map(target) do
    source_history = source[:history] || source["history"] || []
    target_history = target[:history] || target["history"] || []

    source_history
    |> Enum.sort_by(&(&1[:source_sequence] || &1["source_sequence"] || 0), :desc)
    |> Enum.find_value(0, fn source_entry ->
      if Enum.any?(target_history, &same_session?(&1, source_entry)),
        do: source_entry[:source_sequence] || source_entry["source_sequence"] || 0,
        else: nil
    end)
  end

  # SAFETY: a remote peer may persist a checkpoint value that is not a map (a scalar or
  # list). Rather than raise BadMapError when indexing [:history], degrade to "no common
  # sequence" so the worker retries from scratch.
  def common_sequence(_source, _target), do: 0

  defp same_session?(left, right) when is_map(left) and is_map(right),
    do:
      (left[:session_id] || left["session_id"]) == (right[:session_id] || right["session_id"]) and
        (left[:source_sequence] || left["source_sequence"]) ==
          (right[:source_sequence] || right["source_sequence"])

  # SAFETY: a malformed checkpoint history may contain non-map entries. Treat any
  # non-map entry as "not the same session" instead of raising BadMapError.
  defp same_session?(_left, _right), do: false
end
