defmodule ElixirDB.Query.SubscriptionSupervisor do
  @moduledoc false
  use Supervisor

  alias ElixirDB.Query.SubscriptionHub
  alias ElixirDB.Runtime.ChildSpec

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
      {:via, Registry, {ElixirDB.Runtime.DatabaseRegistry, {:query_subscription_supervisor, uuid}}}

  def dynamic_supervisor(uuid) do
    case Registry.lookup(
           ElixirDB.Runtime.DatabaseRegistry,
           {:query_subscription_dynamic_supervisor, uuid}
         ) do
      [{pid, _}] -> {:ok, pid}
      [] -> {:error, ElixirDB.Error.database_closed("subscription supervisor is not running")}
    end
  end

  @impl true
  def init(uuid) do
    children = [
      SubscriptionHub.child_spec(uuid),
      %{
        id: {:query_subscription_dynamic_supervisor, uuid},
        start:
          {DynamicSupervisor, :start_link, [[strategy: :one_for_one, name: dynamic_via(uuid)]]},
        restart: :permanent,
        shutdown: :infinity,
        type: :supervisor
      }
    ]

    Supervisor.init(children, strategy: :rest_for_one)
  end

  defp dynamic_via(uuid),
    do:
      {:via, Registry,
       {ElixirDB.Runtime.DatabaseRegistry, {:query_subscription_dynamic_supervisor, uuid}}}
end
