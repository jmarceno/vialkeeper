defmodule ElixirDB.Integrity.RulesTest do
  use ExUnit.Case, async: true

  alias ElixirDB.Integrity.Rules

  test "fresh empty snapshot passes" do
    assert :ok = Rules.validate(base_snapshot())
  end

  test "invalid database uuid fails with generic integrity_violation" do
    snapshot = put_in(base_snapshot(), [:meta, :database_uuid], "")

    assert {:error, %ElixirDB.Error{code: :integrity_violation}} = Rules.validate(snapshot)
  end

  test "change rows at or below retention floor fail with integrity_violation" do
    snapshot =
      base_snapshot()
      |> put_in([:meta, :current_sequence], 2)
      |> put_in([:meta, :retention_floor_sequence], 1)
      |> Map.put(:changes, [
        %{
          sequence: 1,
          document_id: "doc",
          winning_revision: "1-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
          deleted: false,
          leaf_revisions: ["1-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"]
        }
      ])

    assert {:error, %ElixirDB.Error{code: :integrity_violation, message: message}} =
             Rules.validate(snapshot)

    assert message =~ "retention floor"
  end

  defp base_snapshot do
    %{
      meta: %{
        database_uuid: "11111111-1111-4111-8111-111111111111",
        history_epoch: "22222222-2222-4222-8222-222222222222",
        current_sequence: 0,
        retention_floor_sequence: 0,
        compaction_epoch: 0,
        retention_boundary_digest: nil
      },
      boundaries: [],
      peers: [],
      maintenance_counter: 0,
      documents: [],
      revisions: [],
      changes: [],
      pending_blobs: [],
      checkpoints: [],
      revision_attachments: []
    }
  end
end
