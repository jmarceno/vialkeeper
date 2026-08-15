defmodule VialKeeper.Shadow.Supervisor do
  @moduledoc "Supervision boundary for source-local shadow control and routing."
  use Supervisor

  def start_link(opts \\ []), do: Supervisor.start_link(__MODULE__, opts, name: __MODULE__)

  @impl true
  def init(opts) do
    children =
      [
        {VialKeeper.Shadow.Registry, opts},
        {VialKeeper.Shadow.RouteTable, opts},
        {Registry, keys: :unique, name: VialKeeper.Shadow.ControllerRegistry},
        {VialKeeper.Shadow.ControllerSupervisor, opts},
        {VialKeeper.Shadow.ControlTaskSupervisor, opts},
        {VialKeeper.Shadow.WorkerRegistry, opts},
        {VialKeeper.Shadow.WorkerSupervisor, opts}
      ] ++ worker_child(opts) ++ [{VialKeeper.Shadow.ControllerManager, opts}]

    Supervisor.init(children, strategy: :rest_for_one)
  end

  defp worker_child(opts) do
    if Keyword.get(Application.get_env(:vial_keeper, :shadow_worker, []), :enabled, false) do
      [{VialKeeper.Shadow.Worker, opts}]
    else
      []
    end
  end
end
