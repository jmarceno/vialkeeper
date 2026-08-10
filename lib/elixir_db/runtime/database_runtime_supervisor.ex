defmodule ElixirDB.Runtime.DatabaseRuntimeSupervisor do
  @moduledoc "Supervises the processes that make up one open database runtime."
  use Supervisor
  alias ElixirDB.DatabaseBundle
  alias ElixirDB.Runtime.{AdmissionPolicy, AdmissionSupervisor, ChildSpec}

  def start_link(%{uuid: uuid} = args), do: Supervisor.start_link(__MODULE__, args, name: via(uuid))
  def via(uuid), do: {:via, Registry, {ElixirDB.Runtime.DatabaseRegistry, {:runtime, uuid}}}

  def child_spec(%{uuid: uuid} = args) do
    ChildSpec.supervisor({:database_runtime, uuid}, {__MODULE__, :start_link, [args]}, :transient)
  end

  @impl true
  def init(%{uuid: uuid, bundle: %DatabaseBundle{} = bundle}) do
    limit = ElixirDB.Config.host_limits()[:admission_limit] || 128
    policy = admission_policy(limit)
    sqlite_path = DatabaseBundle.sqlite_path(bundle)

    children = [
      {ElixirDB.Runtime.FileLease, sqlite_path},
      {ElixirDB.Runtime.DatabaseOwner, {uuid, bundle}},
      AdmissionSupervisor.child_spec(uuid, limit, policy),
      {ElixirDB.Runtime.AttachmentCoordinator, uuid},
      {ElixirDB.Runtime.ChangeNotifier, uuid},
      {ElixirDB.View.Supervisor, uuid},
      {ElixirDB.Query.SubscriptionSupervisor, uuid},
      {ElixirDB.Runtime.RetentionScheduler, uuid}
    ]

    Supervisor.init(children, strategy: :rest_for_one)
  end

  defp admission_policy(limit) do
    keyword = Map.to_list(ElixirDB.Config.admission_policy())
    {:ok, policy} = AdmissionPolicy.from_keyword(keyword, limit)
    policy
  end
end
