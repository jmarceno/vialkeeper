defmodule ElixirDB.Replication.CheckpointReconciler do
  @moduledoc false

  alias ElixirDB.MapAccess

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

  defp same_session?(left, right) when is_map(left) and is_map(right),
    do:
      MapAccess.get(left, :session_id) == MapAccess.get(right, :session_id) and
        MapAccess.get(left, :source_sequence) == MapAccess.get(right, :source_sequence)

  # SAFETY: a malformed checkpoint history may contain non-map entries. Treat any
  # non-map entry as "not the same session" instead of raising BadMapError.
  defp same_session?(_left, _right), do: false
end
