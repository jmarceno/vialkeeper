defmodule ElixirDB.Retention.CompactionPlanTest do
  use ExUnit.Case, async: true

  alias ElixirDB.Domain.Revision
  alias ElixirDB.Retention.CompactionPlan

  test "zero history depth retains only the winning revision" do
    revisions = linear_chain("doc", 4)
    winner = List.last(revisions)

    plan =
      plan!(%{
        candidate_floor: 10,
        history_depth: 0,
        documents: [
          document("doc", 5, winner.revision_id, revisions)
        ]
      })

    assert Map.get(plan.removals, "doc") |> Enum.sort() ==
             revisions
             |> Enum.drop(-1)
             |> Enum.map(& &1.revision_id)
             |> Enum.sort()

    assert [%{history_id: history_id, minimum_retained_generation: 4}] =
             plan.boundaries_to_upsert

    assert history_id == winner.history_id
  end

  test "history depth retains configured ancestor generations" do
    revisions = linear_chain("doc", 5)
    winner = List.last(revisions)

    plan =
      plan!(%{
        candidate_floor: 10,
        history_depth: 2,
        documents: [
          document("doc", 5, winner.revision_id, revisions)
        ]
      })

    retained_generations = [5, 4, 3]

    assert Enum.all?(retained_generations, fn generation ->
             revisions
             |> Enum.find(&(&1.generation == generation))
             |> then(fn revision ->
               revision.revision_id not in Map.get(plan.removals, "doc", [])
             end)
           end)

    assert Enum.count(Map.get(plan.removals, "doc", [])) == 2
  end

  test "settled losing branches are removed" do
    revisions = branched_chain("doc")
    winner = Enum.find(revisions, &(&1.generation == 3 and not &1.deleted))

    plan =
      plan!(%{
        candidate_floor: 10,
        history_depth: 0,
        documents: [
          document("doc", 4, winner.revision_id, revisions)
        ]
      })

    losing = Enum.find(revisions, &(&1.generation == 3 and &1.deleted))

    assert losing.revision_id in Map.get(plan.removals, "doc", [])
    refute winner.revision_id in Map.get(plan.removals, "doc", [])
  end

  test "all-deleted history removes non-winning revisions and retires boundary" do
    revisions = deleted_chain("doc")

    plan =
      plan!(%{
        candidate_floor: 10,
        history_depth: 1,
        documents: [
          document("doc", 3, List.last(revisions).revision_id, revisions)
        ]
      })

    assert [%{retired: true, retired_branch_roots: [root]}] = plan.boundaries_to_upsert
    root_revision = Enum.find(revisions, &is_nil(&1.parent_revision))
    assert root == root_revision.revision_id
    assert List.last(revisions).revision_id in Map.get(plan.removals, "doc", [])
    assert "doc" in plan.documents_to_empty
  end

  test "documents above the floor are untouched" do
    revisions = linear_chain("doc", 3)
    winner = List.last(revisions)

    plan =
      plan!(%{
        candidate_floor: 2,
        history_depth: 0,
        documents: [
          document("doc", 5, winner.revision_id, revisions)
        ]
      })

    assert plan.removals == %{}
    assert plan.boundaries_to_upsert == []
    assert plan.stats.revisions_removed == 0
  end

  test "compaction plan ignores opaque attachment fields in revision bodies" do
    revisions = linear_chain("doc", 3)
    winner = List.last(revisions)

    with_attachments =
      Enum.map(revisions, fn revision ->
        %{
          revision
          | body: Map.put(revision.body || %{}, "_attachments", %{"a" => %{"data" => "x"}})
        }
      end)

    plain_plan =
      plan!(%{
        candidate_floor: 10,
        history_depth: 0,
        documents: [
          document("doc", 3, winner.revision_id, revisions)
        ]
      })

    attachment_plan =
      plan!(%{
        candidate_floor: 10,
        history_depth: 0,
        documents: [
          document("doc", 3, winner.revision_id, with_attachments)
        ]
      })

    assert attachment_plan.removals == plain_plan.removals
    assert attachment_plan.boundaries_to_upsert == plain_plan.boundaries_to_upsert
    assert attachment_plan.delete_changes_through == plain_plan.delete_changes_through
  end

  defp plan!(opts), do: CompactionPlan.plan(opts)

  defp document(document_id, latest_sequence, winner_id, revisions) do
    %{
      document_id: document_id,
      latest_change_sequence: latest_sequence,
      winning_revision: winner_id,
      revisions: revisions
    }
  end

  defp linear_chain(document_id, generations) do
    history_id = "hist-#{document_id}"

    Enum.reduce(1..generations, {nil, []}, fn generation, {parent, acc} ->
      revision = %Revision{
        document_id: document_id,
        history_id: history_id,
        revision_id: "#{generation}-rev",
        generation: generation,
        parent_revision: parent,
        digest: "d#{generation}",
        deleted: false,
        body: %{"n" => generation},
        attachments: %{}
      }

      {revision.revision_id, [revision | acc]}
    end)
    |> elem(1)
    |> Enum.reverse()
  end

  defp branched_chain(document_id) do
    history_id = "hist-#{document_id}"

    root = %Revision{
      document_id: document_id,
      history_id: history_id,
      revision_id: "1-root",
      generation: 1,
      parent_revision: nil,
      digest: "d1",
      deleted: false,
      body: %{"n" => 1},
      attachments: %{}
    }

    winner = %Revision{
      document_id: document_id,
      history_id: history_id,
      revision_id: "3-win",
      generation: 3,
      parent_revision: root.revision_id,
      digest: "d3",
      deleted: false,
      body: %{"n" => 3},
      attachments: %{}
    }

    loser = %Revision{
      document_id: document_id,
      history_id: history_id,
      revision_id: "3-lose",
      generation: 3,
      parent_revision: root.revision_id,
      digest: "d3l",
      deleted: true,
      body: nil,
      attachments: %{}
    }

    [root, winner, loser]
  end

  defp deleted_chain(document_id) do
    history_id = "hist-#{document_id}"

    root = %Revision{
      document_id: document_id,
      history_id: history_id,
      revision_id: "1-root",
      generation: 1,
      parent_revision: nil,
      digest: "d1",
      deleted: true,
      body: nil,
      attachments: %{}
    }

    tombstone = %Revision{
      document_id: document_id,
      history_id: history_id,
      revision_id: "2-tomb",
      generation: 2,
      parent_revision: root.revision_id,
      digest: "d2",
      deleted: true,
      body: nil,
      attachments: %{}
    }

    [root, tombstone]
  end
end
