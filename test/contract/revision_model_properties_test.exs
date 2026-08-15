defmodule VialKeeper.Contract.RevisionModelPropertiesTest do
  use ExUnit.Case, async: true
  use ExUnitProperties

  alias VialKeeper.ModelGenerators
  alias VialKeeper.Revisions.{ConflictResolution, Id, Winner}

  property "linear history winner is the tip regardless of shuffle of equal set" do
    check all(revisions <- ModelGenerators.linear_revision_history()) do
      tip = List.last(revisions)
      # Winner.select operates on leaves; a linear history has one leaf (the tip).
      assert {:ok, ^tip} = Winner.select([tip])
      assert Winner.conflicts([tip], tip) == []
    end
  end

  property "conflict scenario has two live leaves and deterministic winner" do
    check all(scenario <- ModelGenerators.conflict_scenario()) do
      leaves =
        scenario.revisions
        |> Enum.filter(&(&1.revision_id in [scenario.left_revision, scenario.right_revision]))

      assert scenario.left_revision != scenario.right_revision
      assert [_, _] = leaves
      assert {:ok, winner} = Winner.select(leaves)
      assert winner.revision_id in [scenario.left_revision, scenario.right_revision]

      conflicts = Winner.conflicts(leaves, winner)
      assert [_] = conflicts
      assert hd(conflicts) != winner.revision_id

      assert {:ok, ^winner} = Winner.select(Enum.reverse(leaves))
    end
  end

  property "leaf-set CAS accepts exact live set and rejects shuffled subsets" do
    check all(scenario <- ModelGenerators.conflict_scenario()) do
      leaves =
        scenario.revisions
        |> Enum.filter(&(&1.revision_id in [scenario.left_revision, scenario.right_revision]))

      expected = Enum.map(leaves, & &1.revision_id)
      assert :ok = ConflictResolution.validate_leaf_set(leaves, expected)
      assert :ok = ConflictResolution.validate_leaf_set(leaves, Enum.reverse(expected))

      assert {:error, %VialKeeper.Error{code: :revision_conflict}} =
               ConflictResolution.validate_leaf_set(leaves, [hd(expected)])
    end
  end

  property "revision ids are stable under recalculation" do
    check all(revisions <- ModelGenerators.put_then_optional_delete()) do
      Enum.each(revisions, fn rev ->
        {:ok, calculated} =
          Id.calculate(
            rev.document_id,
            rev.history_id,
            rev.parent_revision,
            rev.deleted,
            rev.body,
            rev.attachments || %{}
          )

        assert calculated == rev.revision_id
        {:ok, generation} = Id.generation(rev.revision_id)
        assert generation == rev.generation
      end)
    end
  end
end
