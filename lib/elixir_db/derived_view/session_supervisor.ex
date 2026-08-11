defmodule ElixirDB.DerivedView.SessionSupervisor do
  @moduledoc "Supervises one derived materializer worker and its source-read tasks."
  use Supervisor

  alias ElixirDB.DerivedView.Worker

  @registry ElixirDB.Runtime.DatabaseRegistry

  @spec child_spec(binary()) :: map()
  def child_spec(uuid) when is_binary(uuid) do
    %{
      id: {:derived_session, uuid},
      start: {__MODULE__, :start_link, [uuid]},
      restart: :transient,
      shutdown: :infinity,
      type: :supervisor
    }
  end

  @spec start_link(binary()) :: Supervisor.on_start()
  def start_link(uuid), do: Supervisor.start_link(__MODULE__, uuid, name: via(uuid))

  @spec via(binary()) :: {:via, Registry, {module(), term()}}
  def via(uuid), do: {:via, Registry, {@registry, {:derived_session, uuid}}}

  @spec task_via(binary()) :: {:via, Registry, {module(), term()}}
  def task_via(uuid), do: {:via, Registry, {@registry, {:derived_task_supervisor, uuid}}}

  @impl true
  def init(uuid) do
    children = [
      %{
        id: {:derived_task_supervisor, uuid},
        start: {Task.Supervisor, :start_link, [[name: task_via(uuid)]]},
        restart: :permanent,
        shutdown: :infinity,
        type: :supervisor
      },
      Worker.child_spec(uuid)
    ]

    Supervisor.init(children, strategy: :one_for_all)
  end
end
