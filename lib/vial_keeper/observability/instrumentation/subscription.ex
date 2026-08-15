defmodule VialKeeper.Observability.Instrumentation.Subscription do
  @moduledoc """
  Bounded observability for live query subscriptions.

  Counters only — never emit selectors, document IDs, revision IDs, PIDs, or
  projected bodies as attributes.
  """

  alias VialKeeper.Observability.Meters

  @doc "Records a successful subscription open."
  @spec open(binary()) :: :ok
  def open(uuid) when is_binary(uuid) do
    Meters.add(:"vial_keeper.query.subscription.open", db_uuid: uuid)
  end

  @doc "Records a delivered subscription update event of the given type."
  @spec update(binary(), atom()) :: :ok
  def update(uuid, type) when is_binary(uuid) and is_atom(type) do
    Meters.add(:"vial_keeper.query.subscription.update", db_uuid: uuid, event_type: type)
  end

  @doc "Records a subscription overload termination."
  @spec overload(binary()) :: :ok
  def overload(uuid) when is_binary(uuid) do
    Meters.add(:"vial_keeper.query.subscription.overload", db_uuid: uuid)
  end
end
