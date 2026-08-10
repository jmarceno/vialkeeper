defmodule ElixirDB.View.Supervisor do
  @moduledoc """
  Per-database supervisor for declarative view builders and their manager.
  """
  use Supervisor

  alias ElixirDB.Runtime.ChildSpec
  alias ElixirDB.View.{BuilderSupervisor, Manager}

  def child_spec(uuid),
    do:
      ChildSpec.supervisor({:view_supervisor, uuid}, {__MODULE__, :start_link, [uuid]}, :transient)

  def start_link(uuid), do: Supervisor.start_link(__MODULE__, uuid, name: via_name(uuid))

  defp via_name(uuid),
    do: {:via, Registry, {ElixirDB.Runtime.DatabaseRegistry, {:view_supervisor, uuid}}}

  @impl true
  def init(uuid) do
    children = [
      BuilderSupervisor.child_spec(uuid),
      Manager.child_spec(uuid)
    ]

    Supervisor.init(children, strategy: :rest_for_one)
  end
end
