defmodule VialKeeper.Shadow.ControllerSupervisor do
  @moduledoc "Dynamic supervisor for one isolated reconciler per enabled source."
  use DynamicSupervisor

  def start_link(opts \\ []), do: DynamicSupervisor.start_link(__MODULE__, opts, name: __MODULE__)

  @spec start_controller(map()) :: DynamicSupervisor.on_start_child()
  def start_controller(args),
    do: DynamicSupervisor.start_child(__MODULE__, {VialKeeper.Shadow.Reconciler, args})

  @spec stop_controller(pid()) :: :ok | {:error, term()}
  def stop_controller(pid), do: DynamicSupervisor.terminate_child(__MODULE__, pid)

  @impl true
  def init(_opts),
    do: DynamicSupervisor.init(strategy: :one_for_one, max_restarts: 3, max_seconds: 5)
end
