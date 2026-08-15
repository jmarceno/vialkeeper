defmodule VialKeeper.DerivedView.RebuildState do
  @moduledoc "Stable state returned when a derived-source rebuild begins."

  @type t :: %{
          required(:materialization_id) => binary(),
          required(:source_database_uuid) => binary(),
          required(:generation) => pos_integer(),
          required(:start_sequence) => non_neg_integer(),
          required(:catchup_sequence) => non_neg_integer(),
          required(:previous_checkpoint_sequence) => non_neg_integer()
        }

  @spec new(t()) :: t()
  def new(effect) when is_map(effect) do
    %{
      materialization_id: Map.fetch!(effect, :materialization_id),
      source_database_uuid: Map.fetch!(effect, :source_database_uuid),
      generation: Map.fetch!(effect, :generation),
      start_sequence: Map.fetch!(effect, :start_sequence),
      catchup_sequence: Map.fetch!(effect, :catchup_sequence),
      previous_checkpoint_sequence: Map.fetch!(effect, :previous_checkpoint_sequence)
    }
  end
end
