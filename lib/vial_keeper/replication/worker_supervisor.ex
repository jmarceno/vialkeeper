defmodule VialKeeper.Replication.WorkerSupervisor do
  @moduledoc "Dynamic supervisor for per-database replication workers."
  use DynamicSupervisor

  @spec start_link() :: Supervisor.on_start()
  @spec start_link(term()) :: Supervisor.on_start()
  def start_link(_args \\ []), do: DynamicSupervisor.start_link(__MODULE__, [], name: __MODULE__)

  @impl true
  def init(_args) do
    # Enforces the worker cap atomically; JobManager's pre-check alone can race.
    max_workers = VialKeeper.Config.host_limits()[:max_replication_workers] || 32
    DynamicSupervisor.init(strategy: :one_for_one, max_children: max_workers)
  end
end
