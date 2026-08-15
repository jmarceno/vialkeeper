defmodule VialKeeper.Replication.Id do
  @moduledoc "Stable identifiers for replication jobs and sessions."

  alias VialKeeper.JSON.Canonical

  def calculate(source_uuid, target_uuid, direction, mode, filter \\ nil) do
    with {:ok, json} <-
           Canonical.encode(%{
             "source_database_uuid" => source_uuid,
             "target_database_uuid" => target_uuid,
             "direction" => direction,
             "mode" => mode,
             "filter" => filter,
             "replication_protocol_major" => VialKeeper.protocol_major()
           }) do
      {:ok, :crypto.hash(:sha256, json) |> Base.encode16(case: :lower)}
    end
  end
end
