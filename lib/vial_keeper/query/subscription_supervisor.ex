defmodule VialKeeper.Query.SubscriptionSupervisor do
  @moduledoc "Supervises one database's live-query hub and dynamic subscription tasks."
  use Supervisor

  alias VialKeeper.Query.SubscriptionHub
  alias VialKeeper.Runtime.ChildSpec

  def child_spec(uuid),
    do:
      ChildSpec.supervisor(
        {:query_subscription_supervisor, uuid},
        {__MODULE__, :start_link, [uuid]},
        :transient
      )

  def start_link(uuid), do: Supervisor.start_link(__MODULE__, uuid, name: via(uuid))

  def via(uuid),
    do:
      {:via, Registry,
       {VialKeeper.Runtime.DatabaseRegistry, {:query_subscription_supervisor, uuid}}}

  def dynamic_supervisor(uuid) do
    case Registry.lookup(
           VialKeeper.Runtime.DatabaseRegistry,
           {:query_subscription_dynamic_supervisor, uuid}
         ) do
      [{pid, _}] -> {:ok, pid}
      [] -> {:error, VialKeeper.Error.database_closed("subscription supervisor is not running")}
    end
  end

  @impl true
  def init(uuid) do
    children = [
      SubscriptionHub.child_spec(uuid),
      ChildSpec.supervisor(
        {:query_subscription_dynamic_supervisor, uuid},
        {DynamicSupervisor, :start_link, [[strategy: :one_for_one, name: dynamic_via(uuid)]]},
        :permanent,
        :infinity
      )
    ]

    Supervisor.init(children, strategy: :rest_for_one)
  end

  defp dynamic_via(uuid),
    do:
      {:via, Registry,
       {VialKeeper.Runtime.DatabaseRegistry, {:query_subscription_dynamic_supervisor, uuid}}}
end
