defmodule VialKeeper.Storage.SQLite.Meta do
  @moduledoc "SQLite metadata-row loading and normalized identity persistence helpers."

  alias VialKeeper.JSON.StrictDecoder
  alias VialKeeper.Storage.SQLite.Connection

  @query "SELECT database_uuid, history_epoch, current_sequence, retention_floor_sequence, compaction_epoch, retention_boundary_digest, config_json FROM db_meta WHERE id = 1"

  @spec load(Connection.handle()) :: {:ok, map()} | {:error, VialKeeper.Error.t()}
  def load(conn) do
    case Connection.query(conn, @query) do
      {:ok, [[uuid, history_epoch, sequence, floor, compaction_epoch, digest, config_json]]} ->
        with {:ok, config} <- StrictDecoder.decode(config_json) do
          {:ok,
           %{
             database_uuid: uuid,
             history_epoch: history_epoch,
             current_sequence: sequence,
             retention_floor_sequence: floor,
             compaction_epoch: compaction_epoch,
             retention_boundary_digest: digest,
             config: config
           }}
        end

      {:error, reason} ->
        {:error, normalize_error(reason)}
    end
  end

  defp normalize_error(reason),
    do: VialKeeper.Error.internal_error("SQLite operation failed", %{cause: inspect(reason)})
end
