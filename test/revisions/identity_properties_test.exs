defmodule VialKeeper.Revisions.IdentityPropertiesTest do
  @moduledoc "Identical logical revision inputs produce identical content-addressed IDs."

  use ExUnit.Case, async: true
  use ExUnitProperties

  alias VialKeeper.ModelGenerators
  alias VialKeeper.RevisionFixtures
  alias VialKeeper.Revisions.Id

  property "calculate/6 is deterministic for the same logical inputs" do
    check all(
            document_id <- ModelGenerators.document_id(),
            body <- ModelGenerators.document_body(),
            deleted <- StreamData.boolean(),
            max_runs: 40
          ) do
      history_id = RevisionFixtures.shared_history_id()
      body = if(deleted, do: nil, else: body)

      assert {:ok, first} =
               Id.calculate(document_id, history_id, nil, deleted, body, %{})

      assert {:ok, second} =
               Id.calculate(document_id, history_id, nil, deleted, body, %{})

      assert first == second
    end
  end
end
