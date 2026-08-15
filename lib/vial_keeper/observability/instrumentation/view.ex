defmodule VialKeeper.Observability.Instrumentation.View do
  @moduledoc """
  Bounded observability for declarative view builders.

  Counters only — never emit view definitions, document IDs, revision IDs, or
  emitted keys as attributes.
  """

  alias VialKeeper.Observability.Meters

  @doc "Records that a view builder started for a database."
  @spec builder_started(binary()) :: :ok
  def builder_started(uuid) when is_binary(uuid) do
    Meters.add(:"vial_keeper.view.builder.started", db_uuid: uuid)
  end

  @doc "Records a completed view rebuild activation."
  @spec rebuild_activated(binary()) :: :ok
  def rebuild_activated(uuid) when is_binary(uuid) do
    Meters.add(:"vial_keeper.view.rebuild.activated", db_uuid: uuid)
  end

  @doc "Records an incremental view batch application."
  @spec batch_applied(binary()) :: :ok
  def batch_applied(uuid) when is_binary(uuid) do
    Meters.add(:"vial_keeper.view.batch.applied", db_uuid: uuid)
  end

  @doc "Records that a view builder abandoned work due to history truncation."
  @spec history_truncated(binary()) :: :ok
  def history_truncated(uuid) when is_binary(uuid) do
    Meters.add(:"vial_keeper.view.history_truncated", db_uuid: uuid)
  end
end
