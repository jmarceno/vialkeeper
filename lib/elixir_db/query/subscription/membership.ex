defmodule ElixirDB.Query.Subscription.Membership do
  @moduledoc "Membership transition evaluation for live query subscriptions."

  alias ElixirDB.Query.{Projection, Selector}
  alias ElixirDB.Query.Subscription.Events

  @spec transition(map(), map(), MapSet.t(), non_neg_integer(), non_neg_integer()) ::
          {:ok, map() | nil, MapSet.t()} | {:error, ElixirDB.Error.t()}
  def transition(envelope, request, membership, sequence, max_members) do
    predicate = Map.get(request, :predicate)
    matching = matching?(envelope, predicate)
    member? = MapSet.member?(membership, envelope.id)

    case {member?, matching} do
      {false, false} ->
        {:ok, nil, membership}

      {false, true} ->
        with :ok <- ensure_membership_bound(membership, max_members) do
          {:ok, upsert_event(envelope, request, sequence), MapSet.put(membership, envelope.id)}
        end

      {true, true} ->
        {:ok, upsert_event(envelope, request, sequence), membership}

      {true, false} ->
        {:ok, Events.remove(sequence, envelope.id, envelope.revision),
         MapSet.delete(membership, envelope.id)}
    end
  end

  defp matching?(%{deleted: true}, _predicate), do: false

  defp matching?(%{body: body}, predicate) do
    case Selector.matches?(body, predicate) do
      {:ok, value} -> value
      {:error, _} -> false
    end
  end

  defp upsert_event(envelope, request, sequence) do
    Events.upsert(
      sequence,
      Projection.project(
        %{id: envelope.id, revision: envelope.revision, body: envelope.body},
        request
      )
    )
  end

  defp ensure_membership_bound(membership, max_members) do
    if MapSet.size(membership) >= max_members do
      {:error,
       ElixirDB.Error.resource_limit("subscription membership exceeds max_members", %{
         maximum: max_members
       })}
    else
      :ok
    end
  end
end
