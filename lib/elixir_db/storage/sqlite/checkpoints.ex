defmodule ElixirDB.Storage.SQLite.Checkpoints do
  @moduledoc false

  alias ElixirDB.Domain.Checkpoint
  alias ElixirDB.MapAccess

  @spec validate_cas(map() | nil, map()) :: :ok | {:error, ElixirDB.Error.t()}
  def validate_cas(nil, value) when is_map(value), do: validate_shape(value)

  def validate_cas(%{value: current}, value) when is_map(current) and is_map(value) do
    with :ok <- validate_shape(value) do
      validate_no_regression(current, value)
    end
  end

  def validate_cas(%{"value" => current}, value) when is_map(current) and is_map(value) do
    validate_cas(%{value: current}, value)
  end

  def validate_cas(_current, value) when is_map(value), do: validate_shape(value)

  defp validate_shape(value) do
    case Checkpoint.from_wire(normalize_wire(value)) do
      {:ok, _} -> :ok
      {:error, error} -> {:error, error}
    end
  end

  defp validate_no_regression(current, incoming) do
    if regresses?(current, incoming),
      do:
        {:error,
         ElixirDB.Error.checkpoint_conflict("checkpoint regressed safe or compaction epoch", %{
           previous_safe:
             MapAccess.get(current, :safe_source_sequence) ||
               MapAccess.get(current, "safe_source_sequence"),
           incoming_safe:
             MapAccess.get(incoming, :safe_source_sequence) ||
               MapAccess.get(incoming, "safe_source_sequence")
         })},
      else: :ok
  end

  defp regresses?(current, incoming) do
    prev_seq = int_field(current, :source_sequence)
    new_seq = int_field(incoming, :source_sequence)
    prev_safe = int_field(current, :safe_source_sequence)
    new_safe = int_field(incoming, :safe_source_sequence)
    prev_compaction = int_field(current, :installed_source_compaction_epoch)
    new_compaction = int_field(incoming, :installed_source_compaction_epoch)
    prev_epoch = epoch_field(current)
    new_epoch = epoch_field(incoming)

    new_seq < prev_seq or new_safe < prev_safe or new_compaction < prev_compaction or
      epoch_regressed?(prev_epoch, new_epoch, prev_seq, new_seq)
  end

  defp epoch_regressed?(prev_epoch, new_epoch, prev_seq, new_seq) do
    is_binary(prev_epoch) and is_binary(new_epoch) and prev_epoch != new_epoch and
      new_seq > prev_seq
  end

  defp int_field(map, key) do
    MapAccess.get(map, key) || MapAccess.get(map, Atom.to_string(key)) || 0
  end

  defp epoch_field(map) do
    MapAccess.get(map, :source_history_epoch) || MapAccess.get(map, "source_history_epoch")
  end

  defp normalize_wire(value) do
    Map.new(value, fn
      {key, val} when is_atom(key) -> {Atom.to_string(key), val}
      pair -> pair
    end)
  end
end
