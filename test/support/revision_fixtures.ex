defmodule VialKeeper.RevisionFixtures do
  @moduledoc "Shared deterministic history identifiers for revision tests."

  @shared_history_id "11111111-1111-4111-8111-111111111111"
  @independent_history_id "22222222-2222-4222-8222-222222222222"

  @spec shared_history_id() :: binary()
  def shared_history_id, do: @shared_history_id

  @spec independent_history_id() :: binary()
  def independent_history_id, do: @independent_history_id
end
