defmodule ElixirDB.Replication.CheckpointReconciler do
  @moduledoc false

  def common_sequence(nil, nil), do: 0

  def common_sequence(source, target) do
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

  defp same_session?(left, right),
    do:
      (left[:session_id] || left["session_id"]) == (right[:session_id] || right["session_id"]) and
        (left[:source_sequence] || left["source_sequence"]) ==
          (right[:source_sequence] || right["source_sequence"])
end
