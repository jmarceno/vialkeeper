defmodule ElixirDB.Retention.CompactionPlan do
  @moduledoc """
  Pure compaction planner for stable-frontier retention.

  Given a candidate floor, history depth, document revision graphs, current
  boundaries, and peer installed compaction epochs, returns removals, boundary
  upserts/removals, change sequences to delete, and summary stats.

  Compaction never inspects opaque attachment-shaped fields inside revision
  bodies; it only removes whole revision rows selected by the retention plan.
  """

  alias ElixirDB.Domain.{PeerPosition, RetentionBoundary}
  alias ElixirDB.Domain.Revision

  @type document_input :: %{
          required(:document_id) => binary(),
          required(:latest_change_sequence) => non_neg_integer(),
          required(:winning_revision) => binary() | nil,
          required(:revisions) => [Revision.t()]
        }

  @type stored_boundary :: %{
          required(:boundary) => RetentionBoundary.t(),
          required(:compaction_epoch) => non_neg_integer(),
          optional(:source_database_uuid) => binary(),
          optional(:source_history_epoch) => binary()
        }

  @type t :: %__MODULE__{
          removals: %{binary() => [binary()]},
          boundaries_to_upsert: [RetentionBoundary.t()],
          boundaries_to_remove: [RetentionBoundary.t()],
          documents_to_empty: [binary()],
          delete_changes_through: non_neg_integer(),
          stats: map()
        }

  defstruct [
    :removals,
    :boundaries_to_upsert,
    :boundaries_to_remove,
    :documents_to_empty,
    :delete_changes_through,
    :stats
  ]

  @type opts :: %{
          required(:candidate_floor) => non_neg_integer(),
          required(:history_depth) => non_neg_integer(),
          optional(:documents) => [document_input()],
          optional(:boundaries) => [stored_boundary()],
          optional(:local_database_uuid) => binary(),
          optional(:peer_installed_epochs) => %{optional(binary()) => non_neg_integer()},
          optional(:compaction_epoch) => non_neg_integer(),
          optional(:peers) => [PeerPosition.t()],
          optional(:now_ms) => non_neg_integer()
        }

  @spec plan(opts()) :: t()
  def plan(opts) when is_map(opts) do
    candidate_floor = Map.fetch!(opts, :candidate_floor)
    history_depth = Map.fetch!(opts, :history_depth)
    documents = Map.get(opts, :documents, [])
    boundaries = Map.get(opts, :boundaries, [])
    peer_epochs = Map.get(opts, :peer_installed_epochs, %{})

    {removals, upserts, documents_to_empty, stats} =
      Enum.reduce(documents, {%{}, [], [], init_stats()}, fn doc, acc ->
        plan_document(doc, candidate_floor, history_depth, acc)
      end)

    boundaries_to_remove =
      removable_boundaries(
        boundaries,
        peer_epochs,
        Map.get(opts, :peers, []),
        Map.get(opts, :now_ms)
      )

    %__MODULE__{
      removals: removals,
      boundaries_to_upsert: Enum.reverse(upserts),
      boundaries_to_remove: boundaries_to_remove,
      documents_to_empty: documents_to_empty,
      delete_changes_through: candidate_floor,
      stats:
        stats
        |> Map.put(:documents_compacted, map_size(removals))
        |> Map.put(:boundaries_upserted, length(upserts))
        |> Map.put(:boundaries_removed, length(boundaries_to_remove))
        |> Map.put(:changes_removed, candidate_floor)
        |> Map.put(:revisions_removed, count_removals(removals))
    }
  end

  defp init_stats,
    do: %{
      documents_compacted: 0,
      revisions_removed: 0,
      boundaries_upserted: 0,
      boundaries_removed: 0,
      changes_removed: 0
    }

  defp plan_document(
         %{
           latest_change_sequence: latest
         },
         candidate_floor,
         _history_depth,
         {removals, upserts, documents_to_empty, stats}
       )
       when latest > candidate_floor do
    {removals, upserts, documents_to_empty, stats}
  end

  defp plan_document(
         %{
           winning_revision: winner_id,
           revisions: revisions
         },
         _candidate_floor,
         _history_depth,
         {removals, upserts, documents_to_empty, stats}
       )
       when revisions == [] or is_nil(winner_id) do
    {removals, upserts, documents_to_empty, stats}
  end

  defp plan_document(
         %{
           document_id: document_id,
           latest_change_sequence: _latest,
           winning_revision: winner_id,
           revisions: revisions
         },
         _candidate_floor,
         history_depth,
         {removals, upserts, documents_to_empty, stats}
       ) do
    by_id = Map.new(revisions, &{&1.revision_id, &1})
    winner = Map.fetch!(by_id, winner_id)
    leaves = Enum.filter(revisions, &leaf?(&1, by_id))

    if all_leaves_deleted?(leaves) do
      retired = plan_retired_histories(document_id, revisions, leaves)
      removable = Enum.map(revisions, & &1.revision_id)

      {
        Map.put(removals, document_id, removable),
        upserts ++ retired,
        [document_id | documents_to_empty],
        %{stats | revisions_removed: stats.revisions_removed + length(removable)}
      }
    else
      retained = retained_winning_ids(winner, by_id, history_depth)
      removable = removable_ids(revisions, retained, winner_id)
      new_upserts = boundary_upserts(document_id, revisions, retained, removable, by_id)

      {
        if(removable == [],
          do: removals,
          else: Map.put(removals, document_id, removable)
        ),
        upserts ++ new_upserts,
        documents_to_empty,
        %{stats | revisions_removed: stats.revisions_removed + length(removable)}
      }
    end
  end

  defp retained_winning_ids(winner, by_id, history_depth) do
    winner
    |> winning_chain(by_id)
    |> Enum.take(history_depth + 1)
    |> MapSet.new(& &1.revision_id)
  end

  defp winning_chain(revision, by_id) do
    case revision.parent_revision do
      nil ->
        [revision]

      parent_id ->
        case Map.get(by_id, parent_id) do
          nil -> [revision]
          parent -> [revision | winning_chain(parent, by_id)]
        end
    end
  end

  defp removable_ids(revisions, retained, winner_id) do
    revisions
    |> Enum.reject(fn revision ->
      revision.revision_id == winner_id or MapSet.member?(retained, revision.revision_id)
    end)
    |> Enum.map(& &1.revision_id)
  end

  defp boundary_upserts(document_id, revisions, retained, removable, by_id) do
    removable_set = MapSet.new(removable)

    retired_roots =
      revisions
      |> Enum.filter(fn revision ->
        MapSet.member?(removable_set, revision.revision_id) and
          (is_nil(revision.parent_revision) or
             MapSet.member?(retained, revision.parent_revision))
      end)
      |> Enum.map(& &1.revision_id)
      |> Enum.uniq()

    truncation_upserts =
      Enum.reduce(revisions, [], fn revision, upserts ->
        if MapSet.member?(retained, revision.revision_id) and
             parent_missing?(revision, by_id, retained) do
          [
            RetentionBoundary.active(
              document_id,
              revision.history_id,
              revision.generation,
              retired_roots_for_history(retired_roots, revisions, revision.history_id)
            )
            | upserts
          ]
        else
          upserts
        end
      end)

    branch_upserts = branch_boundary_upserts(document_id, revisions, retained, retired_roots)

    (truncation_upserts ++ branch_upserts)
    |> Enum.uniq_by(fn boundary -> {boundary.document_id, boundary.history_id} end)
  end

  defp branch_boundary_upserts(_document_id, _revisions, _retained, []), do: []

  defp branch_boundary_upserts(document_id, revisions, retained, retired_roots) do
    revisions
    |> Enum.map(& &1.history_id)
    |> Enum.uniq()
    |> Enum.flat_map(
      &branch_boundary_for_history(document_id, revisions, retained, retired_roots, &1)
    )
  end

  defp branch_boundary_for_history(document_id, revisions, retained, retired_roots, history_id) do
    case retired_roots_for_history(retired_roots, revisions, history_id) do
      [] ->
        []

      roots ->
        [
          RetentionBoundary.active(
            document_id,
            history_id,
            minimum_generation_for_history(revisions, retained, history_id),
            roots
          )
        ]
    end
  end

  defp retired_roots_for_history(retired_roots, revisions, history_id) do
    retired_roots
    |> Enum.filter(fn root_id ->
      match?(%{history_id: ^history_id}, Enum.find(revisions, &(&1.revision_id == root_id)))
    end)
    |> Enum.sort()
  end

  defp minimum_generation_for_history(revisions, retained, history_id) do
    revisions
    |> Enum.filter(&(&1.history_id == history_id and MapSet.member?(retained, &1.revision_id)))
    |> Enum.map(& &1.generation)
    |> case do
      [] -> 1
      generations -> Enum.min(generations)
    end
  end

  defp parent_missing?(%Revision{parent_revision: nil}, _by_id, _retained), do: false

  defp parent_missing?(%Revision{parent_revision: parent, revision_id: id}, by_id, retained) do
    MapSet.member?(retained, id) and
      (not Map.has_key?(by_id, parent) or not MapSet.member?(retained, parent))
  end

  defp plan_retired_histories(document_id, revisions, leaves) do
    leaves
    |> Enum.map(& &1.history_id)
    |> Enum.uniq()
    |> Enum.map(fn history_id ->
      RetentionBoundary.retired(document_id, history_id, roots_for_history(revisions, history_id))
    end)
  end

  defp roots_for_history(revisions, history_id) do
    revisions
    |> Enum.filter(&(&1.history_id == history_id and is_nil(&1.parent_revision)))
    |> Enum.map(& &1.revision_id)
    |> Enum.sort()
  end

  defp all_leaves_deleted?(leaves),
    do: leaves != [] and Enum.all?(leaves, & &1.deleted)

  defp leaf?(revision, by_id) do
    not Enum.any?(by_id, fn {_id, child} -> child.parent_revision == revision.revision_id end)
  end

  defp removable_boundaries(boundaries, peer_epochs, peers, now_ms) do
    installed = Map.new(peer_epochs)

    boundaries
    |> Enum.filter(fn %{compaction_epoch: epoch} ->
      boundary_removable?(epoch, peers, installed, now_ms)
    end)
    |> Enum.map(& &1.boundary)
  end

  defp boundary_removable?(epoch, peers, installed, now_ms) do
    Enum.all?(peers, fn peer ->
      PeerPosition.expired?(peer, now_ms) or Map.get(installed, peer.peer_database_uuid, 0) >= epoch
    end)
  end

  defp count_removals(removals),
    do: removals |> Map.values() |> List.flatten() |> length()
end
