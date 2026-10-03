defmodule VialKeeper.Revisions.IdentityPropertiesTest do
  @moduledoc "Identical logical revision inputs produce identical content-addressed IDs."

  use ExUnit.Case, async: true
  use ExUnitProperties

  alias VialKeeper.JSON.Canonical
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

  property "a supplied canonical body JSON yields the same ID and generation" do
    check all(
            document_id <- ModelGenerators.document_id(),
            body <- json_object(),
            parent <- StreamData.member_of([nil, "1-" <> String.duplicate("a", 64)]),
            max_runs: 100
          ) do
      history_id = RevisionFixtures.shared_history_id()
      assert {:ok, body_json} = Canonical.encode(body)

      assert {:ok, without} = Id.calculate(document_id, history_id, parent, false, body, %{})

      assert {:ok, ^without, generation} =
               Id.calculate_with_generation(
                 document_id,
                 history_id,
                 parent,
                 false,
                 body,
                 %{},
                 body_json
               )

      assert Id.generation(without) == {:ok, generation}
    end
  end

  # Objects with non-ASCII and supplementary-plane names, floats, and nesting,
  # so the embedded fragment is checked against every canonical ordering rule.
  defp json_object do
    key = StreamData.string(:printable, min_length: 1, max_length: 6)

    scalar =
      StreamData.one_of([
        StreamData.constant(nil),
        StreamData.boolean(),
        StreamData.integer(-1_000_000..1_000_000),
        StreamData.float(min: -1.0e12, max: 1.0e12),
        StreamData.string(:printable, max_length: 12)
      ])

    value =
      StreamData.tree(scalar, fn child ->
        StreamData.one_of([
          StreamData.list_of(child, max_length: 3),
          StreamData.map_of(key, child, max_length: 3)
        ])
      end)

    StreamData.map_of(key, value, max_length: 5)
  end
end
