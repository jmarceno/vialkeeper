defmodule ElixirDB.StorageAdapter.IntegrityTest do
  use ElixirDB.Storage.AdapterCase, adapter: ElixirDB.Storage.SQLite.Adapter

  alias ElixirDB.JSON.Canonical
  alias ElixirDB.Storage.SQLite.Connection

  test "integrity passes on a fresh database", %{adapter: adapter} do
    assert {:ok, %{ok: true}} = @adapter.integrity_check(adapter, %{})
  end

  test "integrity detects corrupted revision digest", %{adapter: adapter} do
    assert {:ok, %{revision: revision}} =
             @adapter.apply_local_mutation(adapter, %{
               operation: :put,
               document_id: "doc",
               body: %{"n" => 1}
             })

    assert :ok =
             Connection.execute(
               adapter.conn,
               "UPDATE revisions SET digest = 'bad' WHERE revision_id = ?",
               [revision]
             )

    assert {:error, %ElixirDB.Error{code: :integrity_violation}} =
             @adapter.integrity_check(adapter, %{})
  end

  test "integrity detects change rows at or below the retention floor", %{adapter: adapter} do
    assert {:ok, %{revision: revision, sequence: sequence}} =
             @adapter.apply_local_mutation(adapter, %{
               operation: :put,
               document_id: "doc",
               body: %{"n" => 1}
             })

    assert :ok =
             Connection.execute(
               adapter.conn,
               "UPDATE db_meta SET retention_floor_sequence = ? WHERE id = 1",
               [sequence]
             )

    assert {:error, %ElixirDB.Error{code: :integrity_violation, message: message}} =
             @adapter.integrity_check(adapter, %{})

    assert message =~ "retention floor"
    assert revision != nil
  end

  test "integrity detects checkpoint installed compaction ahead of source", %{adapter: adapter} do
    value = %{
      "version" => 1,
      "replication_id" => "rep-integrity",
      "checkpoint_version" => 1,
      "session_id" => "sess",
      "source_sequence" => 3,
      "source_compaction_epoch" => 2,
      "installed_source_compaction_epoch" => 5,
      "safe_source_sequence" => 3,
      "source_history_epoch" => identity_epoch(adapter),
      "history" => []
    }

    {:ok, json} = Canonical.encode(value)

    assert :ok =
             Connection.execute(
               adapter.conn,
               "INSERT INTO local_records(namespace, record_key, record_version, value_json) VALUES ('checkpoints', 'rep-integrity', 1, ?)",
               [json]
             )

    assert {:error, %ElixirDB.Error{code: :integrity_violation, message: message}} =
             @adapter.integrity_check(adapter, %{})

    assert message =~ "installed compaction"
  end

  test "integrity detects non-descending checkpoint history", %{adapter: adapter} do
    value = %{
      "version" => 1,
      "replication_id" => "rep-history",
      "checkpoint_version" => 1,
      "session_id" => "sess",
      "source_sequence" => 5,
      "source_history_epoch" => identity_epoch(adapter),
      "source_compaction_epoch" => 0,
      "safe_source_sequence" => 5,
      "installed_source_compaction_epoch" => 0,
      "history" => [
        %{"session_id" => "a", "source_sequence" => 3},
        %{"session_id" => "b", "source_sequence" => 5}
      ]
    }

    {:ok, json} = Canonical.encode(value)

    assert :ok =
             Connection.execute(
               adapter.conn,
               "INSERT INTO local_records(namespace, record_key, record_version, value_json) VALUES ('checkpoints', 'rep-history', 1, ?)",
               [json]
             )

    assert {:error, %ElixirDB.Error{code: :integrity_violation, message: message}} =
             @adapter.integrity_check(adapter, %{})

    assert message =~ "checkpoint history"
  end

  defp identity_epoch(adapter) do
    {:ok, identity} = @adapter.identity(adapter)
    identity.history_epoch
  end
end
