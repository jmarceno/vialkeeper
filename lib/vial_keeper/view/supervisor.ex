defmodule VialKeeper.View.Supervisor do
  @moduledoc """
  Per-database supervisor for declarative view builders and their manager.
  """
  use Supervisor

  alias VialKeeper.Runtime.ChildSpec
  alias VialKeeper.View.{BuilderSupervisor, Manager}

  @spec child_spec(binary()) :: Supervisor.child_spec()
  def child_spec(uuid),
    do:
      ChildSpec.supervisor({:view_supervisor, uuid}, {__MODULE__, :start_link, [uuid]}, :transient)

  @spec start_link(binary()) :: Supervisor.on_start()
  def start_link(uuid), do: Supervisor.start_link(__MODULE__, uuid, name: via_name(uuid))

  defp via_name(uuid),
    do: {:via, Registry, {VialKeeper.Runtime.DatabaseRegistry, {:view_supervisor, uuid}}}

  @spec init(binary()) :: {:ok, {Supervisor.sup_flags(), [Supervisor.child_spec()]}}
  @impl true
  def init(uuid) do
    children = [
      BuilderSupervisor.child_spec(uuid),
      Manager.child_spec(uuid)
    ]

    Supervisor.init(children, strategy: :rest_for_one)
  end
end
