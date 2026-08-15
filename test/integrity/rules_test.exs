defmodule VialKeeper.Integrity.RulesTest do
  use ExUnit.Case, async: true

  alias VialKeeper.Domain.{BoundaryPage, RetentionBoundary}
  alias VialKeeper.Integrity.Rules
  alias VialKeeper.RevisionFixtures
  alias VialKeeper.Revisions.Id, as: RevisionId
  alias VialKeeper.TestRevisionId, as: Id

  @digest String.duplicate("ab", 32)

  test "fresh empty snapshot passes" do
    assert :ok = Rules.validate(base_snapshot())
  end

  test "invalid database uuid fails with generic integrity_violation" do
    snapshot = put_in(base_snapshot(), [:meta, :database_uuid], "")

    assert {:error, %VialKeeper.Error{code: :integrity_violation}} = Rules.validate(snapshot)
  end

  test "change rows at or below retention floor fail with integrity_violation" do
    {:ok, rev} = Id.calculate("doc", nil, false, %{"n" => 1})

    snapshot =
      base_snapshot()
      |> put_in([:meta, :current_sequence], 2)
      |> put_in([:meta, :retention_floor_sequence], 1)
      |> Map.put(:changes, [
        %{
          sequence: 1,
          document_id: "doc",
          winning_revision: rev,
          deleted: false,
          leaf_revisions: [rev]
        }
      ])

    assert {:error, %VialKeeper.Error{code: :integrity_violation, message: message}} =
             Rules.validate(snapshot)

    assert message =~ "retention floor"
  end

  test "dangling parent without boundary fails and boundary allowance passes" do
    history_id = RevisionFixtures.shared_history_id()
    {:ok, root} = Id.calculate("doc", nil, false, %{"n" => 1})
    {:ok, winner_id} = Id.calculate("doc", root, false, %{"n" => 2})
    {:ok, generation} = RevisionId.generation(winner_id)
    digest = winner_id |> String.split("-", parts: 2) |> List.last()

    winner = %{
      document_id: "doc",
      revision_id: winner_id,
      generation: generation,
      parent: root,
      history_id: history_id,
      digest: digest,
      deleted: false,
      body: %{"n" => 2},
      attachments: %{},
      is_leaf: true
    }

    documents = [
      %{
        document_id: "doc",
        winning_revision: winner_id,
        winning_deleted: false,
        update_sequence: 2,
        body: %{"n" => 2}
      }
    ]

    bare =
      base_snapshot()
      |> put_in([:meta, :current_sequence], 2)
      |> Map.put(:documents, documents)
      |> Map.put(:revisions, [winner])
      |> Map.put(:changes, [
        %{
          sequence: 2,
          document_id: "doc",
          winning_revision: winner_id,
          deleted: false,
          leaf_revisions: [
            %{"revision" => winner_id, "deleted" => false, "history_id" => history_id}
          ]
        }
      ])

    assert {:error, %VialKeeper.Error{code: :integrity_violation, message: message}} =
             Rules.validate(bare)

    assert message =~ "dangling parent"

    {:ok, boundary} =
      RetentionBoundary.new(%{
        document_id: "doc",
        history_id: history_id,
        minimum_retained_generation: generation,
        retired: false,
        retired_branch_roots: []
      })

    allowed =
      bare
      |> put_in([:meta, :retention_boundary_digest], BoundaryPage.digest_for([boundary]))
      |> Map.put(:boundaries, [
        %{
          boundary: boundary,
          source_database_uuid: bare.meta.database_uuid,
          source_history_epoch: bare.meta.history_epoch,
          compaction_epoch: 1
        }
      ])

    assert :ok = Rules.validate(allowed)
  end

  test "inconsistent attachment digest sizes fail with integrity_violation" do
    history_id = RevisionFixtures.shared_history_id()

    attachments = %{
      "a.bin" => %{digest: @digest, length: 4, content_type: "application/octet-stream"},
      "b.bin" => %{digest: @digest, length: 5, content_type: "application/octet-stream"}
    }

    {:ok, rev} = Id.calculate("doc", nil, false, %{}, attachments)
    {:ok, generation} = RevisionId.generation(rev)
    digest = rev |> String.split("-", parts: 2) |> List.last()

    revision = %{
      document_id: "doc",
      revision_id: rev,
      generation: generation,
      parent: nil,
      history_id: history_id,
      digest: digest,
      deleted: false,
      body: %{},
      attachments: attachments,
      is_leaf: true
    }

    snapshot =
      base_snapshot()
      |> put_in([:meta, :current_sequence], 1)
      |> Map.put(:documents, [
        %{
          document_id: "doc",
          winning_revision: rev,
          winning_deleted: false,
          update_sequence: 1,
          body: %{}
        }
      ])
      |> Map.put(:revisions, [revision])
      |> Map.put(:revision_attachments, Rules.flatten_revision_attachments([revision]))
      |> Map.put(:changes, [
        %{
          sequence: 1,
          document_id: "doc",
          winning_revision: rev,
          deleted: false,
          leaf_revisions: [
            %{"revision" => rev, "deleted" => false, "history_id" => history_id}
          ]
        }
      ])

    assert {:error, %VialKeeper.Error{code: :integrity_violation, message: message}} =
             Rules.validate(snapshot)

    assert message =~ "inconsistent logical sizes"
  end

  test "winner materialization mismatch fails with integrity_violation" do
    history_id = RevisionFixtures.shared_history_id()
    {:ok, rev} = Id.calculate("doc", nil, false, %{"n" => 1})
    {:ok, generation} = RevisionId.generation(rev)
    digest = rev |> String.split("-", parts: 2) |> List.last()

    revision = %{
      document_id: "doc",
      revision_id: rev,
      generation: generation,
      parent: nil,
      history_id: history_id,
      digest: digest,
      deleted: false,
      body: %{"n" => 1},
      attachments: %{},
      is_leaf: true
    }

    snapshot =
      base_snapshot()
      |> put_in([:meta, :current_sequence], 1)
      |> Map.put(:documents, [
        %{
          document_id: "doc",
          winning_revision: rev,
          winning_deleted: false,
          update_sequence: 1,
          body: %{"n" => 99}
        }
      ])
      |> Map.put(:revisions, [revision])
      |> Map.put(:changes, [
        %{
          sequence: 1,
          document_id: "doc",
          winning_revision: rev,
          deleted: false,
          leaf_revisions: [
            %{"revision" => rev, "deleted" => false, "history_id" => history_id}
          ]
        }
      ])

    assert {:error, %VialKeeper.Error{code: :integrity_violation, message: message}} =
             Rules.validate(snapshot)

    assert message =~ "materialized winner"
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
