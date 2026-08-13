defmodule ElixirDB.Shadow.Supervisor do
  @moduledoc "Supervision boundary for source-local shadow control and routing."
  use Supervisor

  def start_link(opts \\ []), do: Supervisor.start_link(__MODULE__, opts, name: __MODULE__)

  @impl true
  def init(opts) do
    children = [
      {ElixirDB.Shadow.Registry, opts},
      {ElixirDB.Shadow.RouteTable, opts},
      {Registry, keys: :unique, name: ElixirDB.Shadow.ControllerRegistry},
      {ElixirDB.Shadow.ControllerSupervisor, opts},
      {ElixirDB.Shadow.ControlTaskSupervisor, opts},
      {ElixirDB.Shadow.WorkerRegistry, opts},
      {ElixirDB.Shadow.WorkerSupervisor, opts}
    ]

    Supervisor.init(children, strategy: :rest_for_one)
  end
end
