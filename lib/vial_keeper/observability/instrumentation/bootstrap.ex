defmodule VialKeeper.Observability.Instrumentation.Bootstrap do
  @moduledoc """
  Stub emitters for snapshot/bootstrap replication hooks (Architecture §18).

  Replication paths MAY call these when bootstrap flows are wired; until then
  the functions are safe no-ops when export is disabled.
  """

  alias VialKeeper.Observability.Meters

  @metric :"vial_keeper.replication.bootstrap.count"

  @spec requested(binary()) :: :ok
  def requested(uuid) when is_binary(uuid) do
    Meters.add(@metric, db_uuid: uuid, outcome: :requested)
    :ok
  end

  @spec completed(binary()) :: :ok
  def completed(uuid) when is_binary(uuid) do
    Meters.add(@metric, db_uuid: uuid, outcome: :completed)
    :ok
  end

  @spec rebase_required(binary()) :: :ok
  def rebase_required(uuid) when is_binary(uuid) do
    Meters.add(@metric, db_uuid: uuid, outcome: :rebase_required)
    :ok
  end
end
