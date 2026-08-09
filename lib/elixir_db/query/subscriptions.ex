defmodule ElixirDB.Query.Subscriptions do
  @moduledoc "Public service boundary for transient live query subscriptions."

  alias ElixirDB.Query.{Subscription, SubscriptionRequest, SubscriptionSupervisor}
  alias ElixirDB.Runtime.DatabaseCatalog

  def open(uuid, request, client_pid \\ self()) do
    with {:ok, identity} <- DatabaseCatalog.command(uuid, {:command, :identity, %{}}),
         {:ok, normalized} <- normalize(request, identity),
         :ok <- ensure_active_limit(uuid, identity),
         {:ok, supervisor} <- SubscriptionSupervisor.dynamic_supervisor(uuid),
         do:
           DynamicSupervisor.start_child(
             supervisor,
             {Subscription, uuid: uuid, client_pid: client_pid, request: normalized}
           )
  end

  def next(pid, timeout \\ 30_000), do: Subscription.next(pid, timeout)

  def close(pid) when is_pid(pid) do
    if Process.alive?(pid), do: Subscription.close(pid)
    :ok
  end

  def count(uuid), do: SubscriptionSupervisor.dynamic_supervisor(uuid) |> count_children()

  defp normalize(request, identity) do
    config = Map.get(identity, :config, %{})

    with {:ok, normalized} <- SubscriptionRequest.normalize(request, config) do
      {:ok,
       Map.merge(normalized, %{
         max_members: get_in(config, ["subscriptions", "max_members"]) || 500,
         max_buffered_events: get_in(config, ["subscriptions", "max_buffered_events"]) || 256
       })}
    end
  end

  defp ensure_active_limit(uuid, identity) do
    configured = get_in(identity, [:config, "subscriptions", "max_active"]) || 128

    if count(uuid) < configured,
      do: :ok,
      else: {:error, ElixirDB.Error.subscription_overloaded("maximum active subscriptions reached")}
  end

  defp count_children({:ok, supervisor}) do
    %{active: count} = DynamicSupervisor.count_children(supervisor)
    count
  end

  defp count_children(_), do: 0
end
