defmodule VialKeeper.Retention.SafeReport do
  @moduledoc """
  Conservative safe peer-position decision for stable-frontier retention.

  A target may advance `safe_source_sequence` only when epoch matches, the pull
  position is durably applied, boundaries through the source compaction epoch are
  installed, no unacknowledged local mutation may refer to an earlier source
  position, and the report is monotonic. A normal pull checkpoint alone does not
  advance safe position.
  """

  alias __MODULE__.Result

  @type input :: %{
          required(:source_history_epoch) => binary(),
          optional(:checkpoint_source_history_epoch) => binary() | nil,
          required(:source_sequence) => non_neg_integer(),
          required(:previous_safe_source_sequence) => non_neg_integer(),
          required(:proposed_safe_source_sequence) => non_neg_integer(),
          required(:source_compaction_epoch) => non_neg_integer(),
          required(:installed_source_compaction_epoch) => non_neg_integer(),
          required(:boundaries_installed_through) => non_neg_integer(),
          required(:position_durably_applied) => boolean(),
          required(:has_unacknowledged_local_mutations) => boolean(),
          required(:checkpoint_only) => boolean(),
          optional(:bootstrap_applied) => boolean()
        }

  @type result :: Result.t()

  @spec decide(input()) :: result()
  def decide(input) when is_map(input) do
    proposed = Map.fetch!(input, :proposed_safe_source_sequence)
    previous = Map.fetch!(input, :previous_safe_source_sequence)

    case blocking_reason(input) do
      nil ->
        if proposed == previous,
          do: Result.unchanged(previous),
          else: Result.advanced(proposed)

      reason ->
        Result.blocked(previous, reason)
    end
  end

  defp blocking_reason(input) do
    Enum.find_value(blocking_checks(), fn {predicate, reason} ->
      if predicate.(input), do: reason
    end)
  end

  defp blocking_checks do
    [
      {&checkpoint_only?/1, :checkpoint_only},
      {&not_durably_applied?/1, :not_durably_applied},
      {&epoch_mismatch?/1, :epoch_mismatch},
      {&non_monotonic?/1, :non_monotonic},
      {&above_source_sequence?/1, :above_source_sequence},
      {&local_mutations_pending?/1, :local_mutations_pending},
      {&boundaries_incomplete?/1, :boundaries_incomplete}
    ]
  end

  defp checkpoint_only?(input), do: Map.fetch!(input, :checkpoint_only)
  defp not_durably_applied?(input), do: not Map.fetch!(input, :position_durably_applied)

  defp non_monotonic?(input) do
    Map.fetch!(input, :proposed_safe_source_sequence) <
      Map.fetch!(input, :previous_safe_source_sequence)
  end

  defp above_source_sequence?(input) do
    Map.fetch!(input, :proposed_safe_source_sequence) > Map.fetch!(input, :source_sequence)
  end

  defp local_mutations_pending?(input),
    do: Map.fetch!(input, :has_unacknowledged_local_mutations)

  defp epoch_mismatch?(input) do
    if Map.get(input, :bootstrap_applied, false) do
      false
    else
      epoch_mismatch_without_bootstrap?(input)
    end
  end

  defp epoch_mismatch_without_bootstrap?(input) do
    current = Map.fetch!(input, :source_history_epoch)
    checkpoint = Map.get(input, :checkpoint_source_history_epoch)

    is_binary(checkpoint) and checkpoint != "" and checkpoint != current
  end

  defp boundaries_incomplete?(input) do
    source_compaction = Map.fetch!(input, :source_compaction_epoch)
    installed = Map.fetch!(input, :installed_source_compaction_epoch)
    through = Map.fetch!(input, :boundaries_installed_through)

    (source_compaction > 0 and through < source_compaction) or installed < source_compaction
  end
end
