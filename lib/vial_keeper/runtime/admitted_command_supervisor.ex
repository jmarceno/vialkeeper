defmodule VialKeeper.Runtime.AdmittedCommandSupervisor do
  @moduledoc "Dynamic supervisor for the short-lived executors of admitted commands."
  use DynamicSupervisor

  alias VialKeeper.Runtime.ChildSpec

  def child_spec(uuid) do
    ChildSpec.supervisor(
      {:admitted_command_supervisor, uuid},
      {__MODULE__, :start_link, [uuid]},
      :permanent
    )
  end

  def start_link(uuid), do: DynamicSupervisor.start_link(__MODULE__, uuid, name: via(uuid))

  def via(uuid),
    do:
      {:via, Registry, {VialKeeper.Runtime.DatabaseRegistry, {:admitted_command_supervisor, uuid}}}

  @spec lookup(binary()) :: {:ok, pid()} | {:error, :not_found}
  def lookup(uuid) do
    case Registry.lookup(VialKeeper.Runtime.DatabaseRegistry, {:admitted_command_supervisor, uuid}) do
      [{pid, _}] -> {:ok, pid}
      [] -> {:error, :not_found}
    end
  end

  @impl true
  def init(_uuid), do: DynamicSupervisor.init(strategy: :one_for_one)
end
