defmodule VialKeeper.Query.Subscription.Membership do
  @moduledoc "Membership transition evaluation for live query subscriptions."

  alias VialKeeper.Query.{Projection, Selector}
  alias VialKeeper.Query.Subscription.Events

  @spec transition(map(), map(), MapSet.t(), non_neg_integer(), non_neg_integer()) ::
          {:ok, map() | nil, MapSet.t()} | {:error, VialKeeper.Error.t()}
  def transition(envelope, request, membership, sequence, max_members) do
    predicate = Map.get(request, :predicate)

    with {:ok, matching} <- matching(envelope, predicate) do
      member? = MapSet.member?(membership, envelope.id)
      apply_transition(member?, matching, envelope, request, membership, sequence, max_members)
    end
  end

  defp apply_transition(false, false, _envelope, _request, membership, _sequence, _max_members),
    do: {:ok, nil, membership}

  defp apply_transition(false, true, envelope, request, membership, sequence, max_members) do
    with :ok <- ensure_membership_bound(membership, max_members) do
      {:ok, upsert_event(envelope, request, sequence), MapSet.put(membership, envelope.id)}
    end
  end

  defp apply_transition(true, true, envelope, request, membership, sequence, _max_members),
    do: {:ok, upsert_event(envelope, request, sequence), membership}

  defp apply_transition(true, false, envelope, _request, membership, sequence, _max_members),
    do:
      {:ok, Events.remove(sequence, envelope.id, envelope.revision),
       MapSet.delete(membership, envelope.id)}

  defp matching(%{deleted: true}, _predicate), do: {:ok, false}

  defp matching(%{body: body}, predicate), do: Selector.matches?(body, predicate)

  defp upsert_event(envelope, request, sequence) do
    Events.upsert(sequence, Projection.project(envelope, request))
  end

  defp ensure_membership_bound(membership, max_members) do
    if MapSet.size(membership) >= max_members do
      {:error,
       VialKeeper.Error.resource_limit("subscription membership exceeds max_members", %{
         maximum: max_members
       })}
    else
      :ok
    end
  end
end
