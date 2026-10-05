defmodule VialKeeper.Runtime.DatabaseRuntimeSupervisor do
  @moduledoc """
  Supervises the processes that make up one open database runtime.

  Children start in order under `:rest_for_one`. The sequence ledger starts
  before the owner, so a ledger crash restarts every writer and no writer can
  keep a reservation from a dead ledger. The writer pool starts after the read
  pool.
  """
  use Supervisor
  alias VialKeeper.DatabaseBundle

  alias VialKeeper.Runtime.{
    AdmissionPolicy,
    AdmissionSupervisor,
    ChildSpec,
    ReadPoolSupervisor,
    SequenceLedger,
    WriterPoolSupervisor
  }

  @registry VialKeeper.Runtime.DatabaseRegistry
  @previous_wait_ms 1_000

  @spec start_link(map()) :: Supervisor.on_start()
  def start_link(%{uuid: uuid} = args), do: Supervisor.start_link(__MODULE__, args, name: via(uuid))

  @spec via(binary()) :: {:via, module(), term()}
  def via(uuid), do: {:via, Registry, {VialKeeper.Runtime.DatabaseRegistry, {:runtime, uuid}}}

  @spec child_spec(map()) :: map()
  def child_spec(%{uuid: uuid} = args) do
    ChildSpec.supervisor({:database_runtime, uuid}, {__MODULE__, :start_link, [args]}, :transient)
  end

  @impl true
  def init(%{uuid: uuid, bundle: %DatabaseBundle{} = bundle} = args) do
    :ok = await_previous_runtime(uuid, System.monotonic_time(:millisecond) + @previous_wait_ms)
    limit = VialKeeper.Config.host_limits()[:admission_limit] || 128
    policy = admission_policy(limit)

    children = children_for_kind(uuid, bundle, Map.get(args, :database_kind), limit, policy)

    Supervisor.init(children, strategy: :rest_for_one)
  end

  # After a killed runtime, its ledger and owner may still be closing their
  # connections, and the registry drops their names asynchronously. A restart
  # waits, bounded, until both names are free instead of failing on them and
  # using up the parent's restart budget. Past the bound the restart proceeds
  # and the parent's budget applies as before.
  defp await_previous_runtime(uuid, deadline_ms) do
    cond do
      Enum.all?([{:sequence_ledger, uuid}, {:owner, uuid}], &(Registry.lookup(@registry, &1) == [])) ->
        :ok

      System.monotonic_time(:millisecond) >= deadline_ms ->
        :ok

      true ->
        Process.sleep(1)
        await_previous_runtime(uuid, deadline_ms)
    end
  end

  defp children_for_kind(uuid, bundle, :shadow, limit, policy) do
    [
      {VialKeeper.Runtime.Ownership, DatabaseBundle.root(bundle)},
      SequenceLedger.child_spec(uuid),
      {VialKeeper.Runtime.DatabaseOwner, {uuid, bundle, :shadow}},
      AdmissionSupervisor.child_spec(uuid, limit, policy),
      read_pool_child(uuid),
      writer_pool_child(uuid),
      {VialKeeper.Runtime.AttachmentCoordinator, {uuid, :read_only}}
    ]
  end

  defp children_for_kind(uuid, bundle, kind, limit, policy) do
    [
      {VialKeeper.Runtime.Ownership, DatabaseBundle.root(bundle)},
      SequenceLedger.child_spec(uuid),
      {VialKeeper.Runtime.DatabaseOwner, {uuid, bundle, kind}},
      AdmissionSupervisor.child_spec(uuid, limit, policy),
      read_pool_child(uuid),
      writer_pool_child(uuid),
      {VialKeeper.Runtime.AttachmentCoordinator, uuid},
      {VialKeeper.Runtime.ChangeNotifier, uuid},
      {VialKeeper.View.Supervisor, uuid},
      {VialKeeper.Query.SubscriptionSupervisor, uuid},
      {VialKeeper.Runtime.RetentionScheduler, uuid}
    ]
  end

  defp read_pool_child(uuid) do
    limits = VialKeeper.Config.host_limits()
    pool_size = limits[:read_pool_size] || 4
    queue_limit = limits[:read_queue_limit] || 128
    ReadPoolSupervisor.child_spec(uuid, pool_size, queue_limit)
  end

  defp writer_pool_child(uuid) do
    limits = VialKeeper.Config.host_limits()
    pool_size = limits[:writer_pool_size] || 8
    queue_limit = limits[:write_queue_limit] || 128
    WriterPoolSupervisor.child_spec(uuid, pool_size, queue_limit)
  end

  defp admission_policy(limit) do
    keyword = Map.to_list(VialKeeper.Config.admission_policy())
    {:ok, policy} = AdmissionPolicy.from_keyword(keyword, limit)
    policy
  end
end
