defmodule ElixirDB.Runtime.DatabaseRuntimeSupervisor do
  @moduledoc "Supervises the processes that make up one open database runtime."
  use Supervisor
  alias ElixirDB.DatabaseBundle
  alias ElixirDB.Runtime.{AdmissionPolicy, AdmissionSupervisor, ChildSpec, ReadPoolSupervisor}

  def start_link(%{uuid: uuid} = args), do: Supervisor.start_link(__MODULE__, args, name: via(uuid))
  def via(uuid), do: {:via, Registry, {ElixirDB.Runtime.DatabaseRegistry, {:runtime, uuid}}}

  def child_spec(%{uuid: uuid} = args) do
    ChildSpec.supervisor({:database_runtime, uuid}, {__MODULE__, :start_link, [args]}, :transient)
  end

  @impl true
  def init(%{uuid: uuid, bundle: %DatabaseBundle{} = bundle} = args) do
    limit = ElixirDB.Config.host_limits()[:admission_limit] || 128
    policy = admission_policy(limit)

    children = children_for_kind(uuid, bundle, Map.get(args, :database_kind), limit, policy)

    Supervisor.init(children, strategy: :rest_for_one)
  end

  defp children_for_kind(uuid, bundle, :shadow, limit, policy) do
    [
      {ElixirDB.Runtime.Ownership, DatabaseBundle.root(bundle)},
      {ElixirDB.Runtime.DatabaseOwner, {uuid, bundle, :shadow}},
      AdmissionSupervisor.child_spec(uuid, limit, policy),
      read_pool_child(uuid),
      {ElixirDB.Runtime.AttachmentCoordinator, {uuid, :read_only}}
    ]
  end

  defp children_for_kind(uuid, bundle, kind, limit, policy) do
    [
      {ElixirDB.Runtime.Ownership, DatabaseBundle.root(bundle)},
      {ElixirDB.Runtime.DatabaseOwner, {uuid, bundle, kind}},
      AdmissionSupervisor.child_spec(uuid, limit, policy),
      read_pool_child(uuid),
      {ElixirDB.Runtime.AttachmentCoordinator, uuid},
      {ElixirDB.Runtime.ChangeNotifier, uuid},
      {ElixirDB.View.Supervisor, uuid},
      {ElixirDB.Query.SubscriptionSupervisor, uuid},
      {ElixirDB.Runtime.RetentionScheduler, uuid}
    ]
  end

  defp read_pool_child(uuid) do
    limits = ElixirDB.Config.host_limits()
    pool_size = limits[:read_pool_size] || 4
    queue_limit = limits[:read_queue_limit] || 128
    ReadPoolSupervisor.child_spec(uuid, pool_size, queue_limit)
  end

  defp admission_policy(limit) do
    keyword = Map.to_list(ElixirDB.Config.admission_policy())
    {:ok, policy} = AdmissionPolicy.from_keyword(keyword, limit)
    policy
  end
end
