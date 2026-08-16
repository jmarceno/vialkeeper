defmodule VialKeeper.Replication.Id do
  @moduledoc "Stable identifiers for replication jobs and sessions."

  alias VialKeeper.JSON.Canonical

  @spec calculate(binary(), binary(), term(), term()) ::
          {:ok, binary()} | {:error, VialKeeper.Error.t()}
  @spec calculate(binary(), binary(), term(), term(), term()) ::
          {:ok, binary()} | {:error, VialKeeper.Error.t()}
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
