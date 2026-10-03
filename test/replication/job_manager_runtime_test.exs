defmodule VialKeeper.Replication.JobManagerRuntimeTest do
  @moduledoc "Covers JobManager runtime-table edge cases."

  use ExUnit.Case, async: false

  alias VialKeeper.Error
  alias VialKeeper.Replication.JobManager

  @table :vial_keeper_replication_jobs

  setup do
    job_id = "job-#{System.unique_integer([:positive])}"
    uuid = VialKeeper.UUID.v4()
    on_exit(fn -> :ets.delete(@table, job_id) end)
    {:ok, job_id: job_id, uuid: uuid}
  end

  defp dead_pid do
    pid = spawn(fn -> :ok end)
    ref = Process.monitor(pid)
    assert_receive {:DOWN, ^ref, :process, ^pid, _}
    pid
  end

  test "cancel for a job tracked under another database is not found", %{
    job_id: job_id,
    uuid: uuid
  } do
    true = :ets.insert(@table, {job_id, :waiting, dead_pid(), uuid, "rid", %{}})

    assert {:error, %Error{code: :replication_job_not_found}} =
             JobManager.cancel(VialKeeper.UUID.v4(), job_id)
  end

  test "a worker that died in an active state does not keep its database active", %{
    job_id: job_id,
    uuid: uuid
  } do
    true = :ets.insert(@table, {job_id, :transfer, dead_pid(), uuid, "rid", %{}})
    refute JobManager.active?(uuid)

    live = spawn(fn -> Process.sleep(:infinity) end)
    on_exit(fn -> Process.exit(live, :kill) end)
    true = :ets.insert(@table, {job_id, :transfer, live, uuid, "rid", %{}})
    assert JobManager.active?(uuid)
  end
end
