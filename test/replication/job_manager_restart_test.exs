defmodule VialKeeper.Replication.JobManagerRestartTest do
  @moduledoc """
  A JobManager crash must not leave untracked replication workers behind: the
  workers restart with it and continuous jobs resume from their checkpoints.
  """

  use ExUnit.Case, async: false

  @moduletag :integration

  alias VialKeeper.Replication.JobManager
  alias VialKeeper.Runtime.DatabaseCatalog

  @table :vial_keeper_replication_jobs

  setup do
    prefix = "jm-restart-#{System.unique_integer([:positive])}"
    root = VialKeeper.Config.database_root()
    paths = [prefix <> "-a.vialkeeper", prefix <> "-b.vialkeeper"]
    Enum.each(paths, &VialKeeper.TempDatabase.cleanup(Path.join(root, &1)))
    [{:ok, a}, {:ok, b}] = Enum.map(paths, &DatabaseCatalog.create/1)

    on_exit(fn ->
      for {identity, path} <- Enum.zip([a, b], paths) do
        _ = DatabaseCatalog.close(identity.database_uuid)
        _ = DatabaseCatalog.unregister(identity.database_uuid)
        VialKeeper.TempDatabase.cleanup(Path.join(root, path))
      end
    end)

    {:ok, a: a.database_uuid, b: b.database_uuid}
  end

  test "workers restart with JobManager and continuous jobs resume", %{a: a, b: b} do
    assert {:ok, %{job_id: job_id}} =
             JobManager.put(a, %{
               "persist" => true,
               "mode" => "continuous",
               "direction" => "push",
               "enabled" => true,
               "endpoint" => %{"kind" => "local", "database_uuid" => b}
             })

    on_exit(fn -> _ = JobManager.disable(a, job_id) end)

    old_worker = await_worker(job_id, nil)
    worker_ref = Process.monitor(old_worker)
    manager_ref = Process.monitor(Process.whereis(JobManager))

    Process.exit(Process.whereis(JobManager), :kill)
    assert_receive {:DOWN, ^manager_ref, :process, _, :killed}, 5_000
    assert_receive {:DOWN, ^worker_ref, :process, ^old_worker, _}, 10_000

    new_worker = await_worker(job_id, old_worker)
    assert Process.alive?(new_worker)
    assert JobManager.active?(a)

    assert {:ok, %{state: :disabled}} = JobManager.disable(a, job_id)
    refute Process.alive?(new_worker)
  end

  # Waits until the job is tracked with a live worker other than `previous`.
  defp await_worker(job_id, previous, attempts \\ 500) do
    entry =
      try do
        :ets.lookup(@table, job_id)
      rescue
        ArgumentError -> []
      end

    case entry do
      [{^job_id, _state, pid, _uuid, _rid, _details}] when pid != previous ->
        if Process.alive?(pid), do: pid, else: retry(job_id, previous, attempts)

      _ ->
        retry(job_id, previous, attempts)
    end
  end

  defp retry(_job_id, _previous, 0), do: flunk("replication worker was not (re)started")

  defp retry(job_id, previous, attempts) do
    Process.sleep(10)
    await_worker(job_id, previous, attempts - 1)
  end
end
