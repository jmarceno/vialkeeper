defmodule VialKeeper.StorageAdapter.ReplicationJobsTest do
  use VialKeeper.Storage.AdapterCase, adapter: VialKeeper.Storage.SQLite.Adapter

  @moduletag :sqlite_physical

  test "replication jobs can be listed, upserted, and deleted", %{adapter: adapter} do
    assert {:ok, []} = @adapter.list_replication_jobs(adapter)

    job_id = "job_" <> Base.encode16(:crypto.strong_rand_bytes(6), case: :lower)

    definition = %{
      "job_id" => job_id,
      "mode" => "one_shot",
      "direction" => "push",
      "endpoint" => %{"kind" => "local", "database_uuid" => VialKeeper.UUID.v4()},
      "enabled" => true
    }

    assert {:ok, %{job_id: ^job_id}} =
             @adapter.put_replication_job(adapter, %{
               job_id: job_id,
               definition: definition,
               enabled: true
             })

    assert {:ok, [job]} = @adapter.list_replication_jobs(adapter)
    assert job.job_id == job_id
    assert job.enabled == true
    assert job.definition["mode"] == "one_shot"
    assert job.definition["direction"] == "push"

    updated = Map.put(definition, "mode", "continuous")

    assert {:ok, %{job_id: ^job_id}} =
             @adapter.put_replication_job(adapter, %{
               job_id: job_id,
               definition: updated,
               enabled: false
             })

    assert {:ok, [job2]} = @adapter.list_replication_jobs(adapter)
    assert job2.enabled == false
    assert job2.definition["mode"] == "continuous"

    assert :ok = @adapter.delete_replication_job(adapter, job_id)
    assert {:ok, []} = @adapter.list_replication_jobs(adapter)
  end
end
