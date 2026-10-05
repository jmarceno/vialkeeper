defmodule VialKeeper.Storage.Contracts.Physical.ReplicationJobs do
  @moduledoc """
  Shared replication jobs tests for the SQLite-dialect storage engines.

  Injected into one test module per engine (`test/physical/sqlite/` and
  `test/physical/turso/`).
  """

  defmacro __using__(opts) do
    # quality:reason contract tests are injected via quote into each adapter module
    # credo:disable-for-next-line Credo.Check.Refactor.LongQuoteBlocks
    quote do
      use VialKeeper.Storage.AdapterCase, unquote(opts)

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
  end
end
