defmodule VialKeeper.Observability.Instrumentation.Import do
  @moduledoc """
  Bounded import-side observability for compact-retention stale-fence no-ops.
  """

  alias VialKeeper.Observability.Meters

  @metric :"vial_keeper.import.stale_fence.count"

  @doc "Increments the stale-fence no-op counter by `count` skipped revisions."
  @spec stale_fence_noop(binary(), non_neg_integer()) :: :ok
  def stale_fence_noop(uuid, count) when is_binary(uuid) and is_integer(count) and count >= 0 do
    if count > 0, do: Meters.add(@metric, db_uuid: uuid, entries: count)
    :ok
  end
end
