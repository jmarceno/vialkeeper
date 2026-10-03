defmodule VialKeeper.Replication.Supervisor do
  @moduledoc """
  Supervises the replication runtime as one unit (`:rest_for_one`).

  `JobManager` owns the in-memory table that tracks running workers, so it must
  never outlive or be outlived by them. If it restarts, every worker is stopped
  and restarted with it, then `Resumer` resumes continuous jobs from their
  durable checkpoints. Unpersisted one-shot jobs are not revived.
  """
  use Supervisor

  alias VialKeeper.Runtime.DatabaseCatalog

  @spec start_link(term()) :: Supervisor.on_start()
  def start_link(_args \\ []), do: Supervisor.start_link(__MODULE__, [], name: __MODULE__)

  @impl true
  def init(_args) do
    children = [
      {Registry, keys: :unique, name: VialKeeper.Replication.WorkerRegistry},
      VialKeeper.Replication.JobManager,
      VialKeeper.Replication.WorkerSupervisor,
      Supervisor.child_spec(
        {Task, &DatabaseCatalog.resume_replication_jobs/0},
        id: :replication_resumer,
        restart: :transient
      )
    ]

    Supervisor.init(children, strategy: :rest_for_one)
  end
end
