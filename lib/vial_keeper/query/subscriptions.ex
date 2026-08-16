defmodule VialKeeper.Query.Subscriptions do
  @moduledoc "Public service boundary for transient live query subscriptions."

  alias VialKeeper.Query.{
    Subscription,
    SubscriptionHub,
    SubscriptionRequest,
    SubscriptionSupervisor
  }

  alias VialKeeper.Query.Subscription.Events
  alias VialKeeper.Runtime.DatabaseCatalog

  @spec open(binary(), map()) :: {:ok, pid()} | {:error, VialKeeper.Error.t()}
  @spec open(binary(), map(), pid()) :: {:ok, pid()} | {:error, VialKeeper.Error.t()}
  def open(uuid, request, client_pid \\ self()) do
    with {:ok, identity} <-
           DatabaseCatalog.command_as(uuid, :subscription, {:command, :identity, %{}}),
         {:ok, normalized} <- normalize(request, identity),
         {:ok, supervisor} <- SubscriptionSupervisor.dynamic_supervisor(uuid) do
      case DynamicSupervisor.start_child(
             supervisor,
             {Subscription, uuid: uuid, client_pid: client_pid, request: normalized}
           ) do
        {:ok, pid} ->
          {:ok, pid}

        {:error, %VialKeeper.Error{} = error} ->
          {:error, error}

        {:error, {:shutdown, %VialKeeper.Error{} = error}} ->
          {:error, error}

        {:error, reason} ->
          {:error,
           VialKeeper.Error.internal_error("subscription failed to start", %{cause: inspect(reason)})}
      end
    end
  end

  @spec next(pid()) :: {:ok | :closed | :error, Events.t()} | {:error, VialKeeper.Error.t()}
  @spec next(pid(), timeout()) ::
          {:ok | :closed | :error, Events.t()} | {:error, VialKeeper.Error.t()}
  def next(pid, timeout \\ 30_000) do
    Subscription.next(pid, timeout)
  catch
    :exit, {:noproc, _} -> {:closed, Events.closed()}
    :exit, {{:noproc, _}, _} -> {:closed, Events.closed()}
  end

  @spec close(pid()) :: :ok
  def close(pid) when is_pid(pid) do
    if Process.alive?(pid), do: Subscription.close(pid)
    :ok
  end

  @spec count(binary()) :: non_neg_integer()
  def count(uuid) do
    case SubscriptionHub.count(uuid) do
      n when is_integer(n) -> n
      {:error, _} -> 0
    end
  end

  defp normalize(request, identity) do
    config = Map.get(identity, :config, %{})

    with {:ok, normalized} <- SubscriptionRequest.normalize(request, config) do
      {:ok,
       Map.merge(normalized, %{
         max_members: get_in(config, ["subscriptions", "max_members"]) || 500,
         max_buffered_events: get_in(config, ["subscriptions", "max_buffered_events"]) || 256,
         max_active: get_in(config, ["subscriptions", "max_active"]) || 128
       })}
    end
  end
end
