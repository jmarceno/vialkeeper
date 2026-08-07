defmodule ElixirDB.Runtime.DatabaseRuntimeSupervisor do
  @moduledoc false
  use Supervisor
  alias ElixirDB.Runtime.ChildSpec

  def start_link(%{uuid: uuid} = args), do: Supervisor.start_link(__MODULE__, args, name: via(uuid))
  def via(uuid), do: {:via, Registry, {ElixirDB.Runtime.DatabaseRegistry, {:runtime, uuid}}}

  def child_spec(%{uuid: uuid} = args) do
    ChildSpec.supervisor({:database_runtime, uuid}, {__MODULE__, :start_link, [args]}, :transient)
  end

  @impl true
  def init(%{uuid: uuid, path: path}) do
    limit = ElixirDB.Config.host_limits()[:admission_limit] || 128

    children = [
      {ElixirDB.Runtime.FileLease, path},
      {ElixirDB.Runtime.DatabaseOwner, {uuid, path}},
      {ElixirDB.Runtime.DatabaseAdmission, {uuid, limit}},
      {ElixirDB.Runtime.ChangeNotifier, uuid}
    ]

    Supervisor.init(children, strategy: :rest_for_one)
  end
end
