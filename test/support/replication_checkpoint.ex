defmodule VialKeeper.TestReplicationCheckpoint do
  @moduledoc "Test support for seeding and inspecting replication checkpoints."

  alias VialKeeper.Replication.LocalEndpoint
  alias VialKeeper.Runtime.DatabaseCatalog

  @spec seed_matching_checkpoints!(binary(), binary(), binary()) :: :ok
  def seed_matching_checkpoints!(source_uuid, target_uuid, replication_id) do
    for uuid <- [source_uuid, target_uuid] do
      case fetch_checkpoint(uuid, replication_id) do
        {:ok, _} ->
          :ok

        :missing ->
          seed_checkpoint!(uuid, replication_id, source_uuid)

        _ ->
          seed_checkpoint!(uuid, replication_id, source_uuid)
      end
    end

    :ok
  end

  defp fetch_checkpoint(uuid, replication_id) do
    case DatabaseCatalog.command(uuid, {:command, :get_local_record, "checkpoints", replication_id}) do
      {:ok, %{value: _value}} -> {:ok, :present}
      {:ok, nil} -> :missing
      {:ok, _} -> {:ok, :present}
      other -> other
    end
  end

  defp seed_checkpoint!(uuid, replication_id, source_uuid) do
    {:ok, identity} = DatabaseCatalog.command(source_uuid, {:command, :identity, %{}})
    {:ok, endpoint} = LocalEndpoint.new(uuid)

    value = %{
      "version" => 1,
      "replication_id" => replication_id,
      "checkpoint_version" => 1,
      "session_id" => VialKeeper.UUID.v4(),
      "source_sequence" => 0,
      "source_history_epoch" => identity.history_epoch,
      "source_compaction_epoch" => Map.get(identity, :compaction_epoch, 0),
      "safe_source_sequence" => 0,
      "installed_source_compaction_epoch" => 0,
      "history" => [],
      "expected_checkpoint_version" => 0
    }

    {:ok, _} = LocalEndpoint.put_checkpoint(endpoint, replication_id, value)
  end
end
