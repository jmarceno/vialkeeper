defmodule ElixirDB.StorageAdapter.CheckpointsTest do
  use ElixirDB.Storage.AdapterCase, adapter: ElixirDB.Storage.SQLite.Adapter

  test "checkpoint local records support CAS write, replay, and conflict", %{adapter: adapter} do
    replication_id = "rep_" <> Base.encode16(:crypto.strong_rand_bytes(8), case: :lower)

    value = %{
      "version" => 1,
      "replication_id" => replication_id,
      "session_id" => ElixirDB.UUID.v4(),
      "source_sequence" => 3,
      "checkpoint_version" => 1,
      "source_history_epoch" => "epoch-test",
      "source_compaction_epoch" => 0,
      "safe_source_sequence" => 0,
      "installed_source_compaction_epoch" => 0,
      "history" => []
    }

    assert {:ok, nil} = @adapter.get_local_record(adapter, "checkpoints", replication_id)

    assert {:ok, %{version: 1, value: ^value, replayed: false}} =
             @adapter.put_local_record_cas(adapter, %{
               namespace: "checkpoints",
               key: replication_id,
               expected_version: 0,
               value: value
             })

    assert {:ok, %{version: 1, value: ^value}} =
             @adapter.get_local_record(adapter, "checkpoints", replication_id)

    # Byte-equivalent lost-response replay at the same expected version.
    assert {:ok, %{version: 1, replayed: true}} =
             @adapter.put_local_record_cas(adapter, %{
               namespace: "checkpoints",
               key: replication_id,
               expected_version: 0,
               value: value
             })

    next = Map.put(value, "source_sequence", 7) |> Map.put("checkpoint_version", 2)

    assert {:ok, %{version: 2, value: ^next, replayed: false}} =
             @adapter.put_local_record_cas(adapter, %{
               namespace: "checkpoints",
               key: replication_id,
               expected_version: 1,
               value: next
             })

    stale = Map.put(value, "source_sequence", 99)

    assert {:error, %ElixirDB.Error{code: :checkpoint_conflict}} =
             @adapter.put_local_record_cas(adapter, %{
               namespace: "checkpoints",
               key: replication_id,
               expected_version: 1,
               value: stale
             })

    assert {:ok, %{version: 2, value: %{"source_sequence" => 7}}} =
             @adapter.get_local_record(adapter, "checkpoints", replication_id)
  end
end
