defmodule ElixirDB.Domain.RetentionTypesTest do
  use ExUnit.Case, async: true

  alias ElixirDB.Domain.{
    BoundaryPage,
    PeerPosition,
    RetentionBoundary,
    RetentionState,
    SourcePosition
  }

  describe "SourcePosition" do
    test "accepts valid values" do
      assert {:ok, position} =
               SourcePosition.new(%{
                 database_uuid: "db-1",
                 history_epoch: "epoch-1",
                 sequence: 0
               })

      assert position.database_uuid == "db-1"
      assert position.history_epoch == "epoch-1"
      assert position.sequence == 0
    end

    test "rejects unknown fields" do
      assert {:error, error} =
               SourcePosition.new(%{
                 database_uuid: "db-1",
                 history_epoch: "epoch-1",
                 sequence: 0,
                 extra: true
               })

      assert error.code == :invalid_request
    end

    test "from_wire maps string keys" do
      assert {:ok, position} =
               SourcePosition.from_wire(%{
                 "database_uuid" => "db-1",
                 "history_epoch" => "epoch-1",
                 "sequence" => 42
               })

      assert position.sequence == 42
    end
  end

  describe "RetentionState" do
    test "floor_can_advance? is monotonic" do
      assert RetentionState.floor_can_advance?(10, 10)
      assert RetentionState.floor_can_advance?(10, 15)
      refute RetentionState.floor_can_advance?(10, 9)
    end

    test "compaction_epoch_after increments only when floor advanced" do
      assert RetentionState.compaction_epoch_after(true, 3) == 4
      assert RetentionState.compaction_epoch_after(false, 3) == 3
    end

    test "from_wire decodes mode strings" do
      assert {:ok, state} =
               RetentionState.from_wire(%{
                 "history_epoch" => "epoch-1",
                 "floor_sequence" => 0,
                 "compaction_epoch" => 0,
                 "boundary_digest" => nil,
                 "mode" => "stable_frontier",
                 "maintenance_counter" => 0
               })

      assert state.mode == :stable_frontier
    end

    test "normalizes empty boundary digest to nil" do
      assert {:ok, state} =
               RetentionState.from_wire(%{
                 "history_epoch" => "epoch-1",
                 "floor_sequence" => 0,
                 "compaction_epoch" => 0,
                 "boundary_digest" => "",
                 "mode" => "disabled",
                 "maintenance_counter" => 0
               })

      assert state.boundary_digest == nil
    end
  end

  describe "PeerPosition" do
    setup do
      future = DateTime.utc_now() |> DateTime.add(60, :second) |> DateTime.to_iso8601()
      past = DateTime.utc_now() |> DateTime.add(-60, :second) |> DateTime.to_iso8601()
      now_ms = DateTime.utc_now() |> DateTime.to_unix(:millisecond)

      peer =
        peer_fixture(%{
          safe_source_sequence: 10,
          installed_source_compaction_epoch: 2,
          lease_expires_at: future,
          status: :active
        })

      %{peer: peer, future: future, past: past, now_ms: now_ms}
    end

    test "expired? uses lease expiry", %{peer: peer, past: past, now_ms: now_ms} do
      expired_peer = %{peer | lease_expires_at: past}
      refute PeerPosition.expired?(peer, now_ms)
      assert PeerPosition.expired?(expired_peer, now_ms)
    end

    test "admits_to_frontier? requires active matching epoch", %{peer: peer, now_ms: now_ms} do
      assert PeerPosition.admits_to_frontier?(peer, "epoch-1", now_ms)

      refute PeerPosition.admits_to_frontier?(
               %{peer | status: :bootstrap_required},
               "epoch-1",
               now_ms
             )

      refute PeerPosition.admits_to_frontier?(
               %{peer | source_history_epoch: "other-epoch"},
               "epoch-1",
               now_ms
             )
    end

    test "regresses? detects rollback signals within the same epoch", %{peer: peer} do
      incoming = %{peer | safe_source_sequence: 5}
      assert PeerPosition.regresses?(peer, incoming)

      incoming = %{peer | installed_source_compaction_epoch: 1}
      assert PeerPosition.regresses?(peer, incoming)

      refute PeerPosition.regresses?(peer, %{peer | safe_source_sequence: 12})
    end

    test "epoch_changed? is separate from regresses?", %{peer: peer} do
      incoming = %{peer | source_history_epoch: "epoch-2"}
      assert PeerPosition.epoch_changed?(peer, incoming)
      refute PeerPosition.regresses?(peer, incoming)
    end
  end

  describe "RetentionBoundary" do
    test "retired boundary requires nil minimum generation" do
      assert {:ok, boundary} =
               RetentionBoundary.new(%{
                 document_id: "doc-1",
                 history_id: "hist-1",
                 minimum_retained_generation: nil,
                 retired: true,
                 retired_branch_roots: []
               })

      assert boundary.retired
    end

    test "active boundary requires positive minimum generation" do
      assert {:ok, _} =
               RetentionBoundary.new(%{
                 document_id: "doc-1",
                 history_id: "hist-1",
                 minimum_retained_generation: 1,
                 retired: false,
                 retired_branch_roots: ["root-b", "root-a"]
               })

      assert {:error, error} =
               RetentionBoundary.new(%{
                 document_id: "doc-1",
                 history_id: "hist-1",
                 minimum_retained_generation: nil,
                 retired: false,
                 retired_branch_roots: []
               })

      assert error.code == :invalid_request
    end
  end

  describe "BoundaryPage.digest_for/1" do
    test "orders boundaries deterministically by document_id then history_id" do
      boundary_a =
        boundary_fixture("doc-b", "hist-2", 2, false, ["z-root", "a-root"])

      boundary_b =
        boundary_fixture("doc-a", "hist-9", 1, false, [])

      boundary_c =
        boundary_fixture("doc-a", "hist-1", 3, true, [])

      digest_one = BoundaryPage.digest_for([boundary_a, boundary_b, boundary_c])
      digest_two = BoundaryPage.digest_for([boundary_c, boundary_b, boundary_a])

      assert digest_one == digest_two
      assert byte_size(digest_one) == 64
    end
  end

  defp peer_fixture(overrides) do
    defaults = %{
      peer_database_uuid: "peer-1",
      peer_history_epoch: "peer-epoch-1",
      source_database_uuid: "db-1",
      source_history_epoch: "epoch-1",
      safe_source_sequence: 0,
      installed_source_compaction_epoch: 0,
      last_seen_at: "2026-01-01T00:00:00Z",
      lease_expires_at: "2099-01-01T00:00:00Z",
      status: :active
    }

    {:ok, peer} = PeerPosition.new(Map.merge(defaults, overrides))
    peer
  end

  defp boundary_fixture(document_id, history_id, generation, retired, roots) do
    {:ok, boundary} =
      RetentionBoundary.new(%{
        document_id: document_id,
        history_id: history_id,
        minimum_retained_generation: if(retired, do: nil, else: generation),
        retired: retired,
        retired_branch_roots: roots
      })

    boundary
  end
end
