defmodule ElixirDB.DerivedView.SessionSupervisor do
  @moduledoc "Supervises one derived materializer worker and its source-read tasks."
  use Supervisor

  alias ElixirDB.DerivedView.Worker
  alias ElixirDB.Runtime.ChildSpec

  @registry ElixirDB.Runtime.DatabaseRegistry

  @spec child_spec(binary()) :: map()
  def child_spec(uuid) when is_binary(uuid),
    do:
      ChildSpec.supervisor(
        {:derived_session, uuid},
        {__MODULE__, :start_link, [uuid]},
        :transient,
        :infinity
      )

  @spec start_link(binary()) :: Supervisor.on_start()
  def start_link(uuid), do: Supervisor.start_link(__MODULE__, uuid, name: via(uuid))

  @spec via(binary()) :: {:via, Registry, {module(), term()}}
  def via(uuid), do: {:via, Registry, {@registry, {:derived_session, uuid}}}

  @spec task_via(binary()) :: {:via, Registry, {module(), term()}}
  def task_via(uuid), do: {:via, Registry, {@registry, {:derived_task_supervisor, uuid}}}

  @impl true
  def init(uuid) do
    children = [
      ChildSpec.supervisor(
        {:derived_task_supervisor, uuid},
        {Task.Supervisor, :start_link, [[name: task_via(uuid)]]},
        :permanent,
        :infinity
      ),
      Worker.child_spec(uuid)
    ]

    Supervisor.init(children, strategy: :one_for_all)
  end
end
