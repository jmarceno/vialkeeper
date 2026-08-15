defmodule VialKeeper.Runtime.AdmissionSupervisor do
  @moduledoc "Supervises one database admission scheduler and its command executors."
  use Supervisor

  alias VialKeeper.Runtime.{AdmittedCommandSupervisor, ChildSpec, DatabaseAdmission}

  def child_spec(uuid, limit, policy) do
    ChildSpec.supervisor(
      {:admission_supervisor, uuid},
      {__MODULE__, :start_link, [{uuid, limit, policy}]},
      :permanent
    )
  end

  def start_link({uuid, limit, policy}),
    do: Supervisor.start_link(__MODULE__, {uuid, limit, policy}, name: via(uuid))

  def via(uuid),
    do: {:via, Registry, {VialKeeper.Runtime.DatabaseRegistry, {:admission_supervisor, uuid}}}

  @impl true
  def init({uuid, limit, policy}) do
    children = [
      AdmittedCommandSupervisor.child_spec(uuid),
      ChildSpec.worker(
        {:admission, uuid},
        {DatabaseAdmission, :start_link, [{uuid, limit, policy}]},
        :permanent
      )
    ]

    Supervisor.init(children, strategy: :one_for_all)
  end
end
