defmodule ElixirDB.StorageAdapter.RetentionContractTest do
  @moduledoc """
  Dual-backend smoke coverage for retention compaction, pending attachment
  protection, and integrity on a fresh database.
  """

  use ExUnit.Case, async: true

  alias ElixirDB.Storage.AdapterCase

  @adapters [ElixirDB.Storage.SQLite.Adapter, ElixirDB.Storage.Memory.Adapter]

  for adapter <- @adapters do
    @adapter adapter

    describe "#{inspect(@adapter)}" do
      setup do
        AdapterCase.open_temp_adapter(@adapter, %{})
      end

      test "disabled retention compaction is a no-op", %{adapter: adapter} do
        assert {:ok, %{revision: _}} =
                 @adapter.apply_local_mutation(adapter, %{
                   operation: :put,
                   document_id: "doc",
                   body: %{"n" => 1}
                 })

        assert {:ok, %{noop?: true, removed_revisions: 0, removed_changes: 0}} =
                 @adapter.compact_retention(adapter, %{})
      end

      test "stable_frontier advances floor to current sequence with no peers", %{adapter: adapter} do
        assert {:ok, _} =
                 @adapter.update_config(adapter, %{
                   "retention" => %{
                     "mode" => "stable_frontier",
                     "history_depth" => 0,
                     "peer_expiry_ms" => 86_400_000,
                     "schedule" => "disabled"
                   }
                 })

        for id <- ["a", "b"] do
          assert {:ok, _} =
                   @adapter.apply_local_mutation(adapter, %{
                     operation: :put,
                     document_id: id,
                     body: %{"n" => 1}
                   })
        end

        assert {:ok, %{current_sequence: 2}} = @adapter.identity(adapter)

        assert {:ok,
                %{
                  noop?: false,
                  old_floor: 0,
                  new_floor: 2,
                  removed_changes: 2,
                  old_compaction_epoch: 0,
                  new_compaction_epoch: 1
                }} = @adapter.compact_retention(adapter, %{})

        assert {:ok, %{results: [], last_sequence: 2}} =
                 @adapter.read_changes(adapter, %{since: 2, limit: 10})
      end

      test "active peer blocks the retention floor", %{adapter: adapter} do
        assert {:ok, %{database_uuid: uuid, history_epoch: epoch}} = @adapter.identity(adapter)

        assert {:ok, _} =
                 @adapter.update_config(adapter, %{
                   "retention" => %{
                     "mode" => "stable_frontier",
                     "history_depth" => 0,
                     "peer_expiry_ms" => 86_400_000,
                     "schedule" => "disabled"
                   }
                 })

        for id <- ["a", "b", "c"] do
          assert {:ok, _} =
                   @adapter.apply_local_mutation(adapter, %{
                     operation: :put,
                     document_id: id,
                     body: %{"n" => 1}
                   })
        end

        future = DateTime.utc_now() |> DateTime.add(60, :second) |> DateTime.to_iso8601()
        peer_uuid = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"

        assert {:ok, _} =
                 @adapter.put_peer_position_cas(adapter, %{
                   expected_version: 0,
                   value: %{
                     peer_database_uuid: peer_uuid,
                     peer_history_epoch: "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
                     source_database_uuid: uuid,
                     source_history_epoch: epoch,
                     safe_source_sequence: 2,
                     installed_source_compaction_epoch: 0,
                     last_seen_at: DateTime.utc_now() |> DateTime.to_iso8601(),
                     lease_expires_at: future,
                     status: :active
                   }
                 })

        assert {:ok, %{new_floor: 2, removed_changes: 2}} =
                 @adapter.compact_retention(adapter, %{})

        assert {:error, %ElixirDB.Error{code: :history_truncated}} =
                 @adapter.read_changes(adapter, %{since: 0, limit: 10})

        assert {:ok, %{results: [survivor], last_sequence: 3}} =
                 @adapter.read_changes(adapter, %{since: 2, limit: 10})

        assert is_map(survivor)
      end

      test "pending protection appears in live digests", %{adapter: adapter} do
        digest = String.duplicate("ab", 32)

        assert {:ok, %{digest: ^digest, expires_at: expires_at}} =
                 @adapter.protect_pending_blob(adapter, %{
                   digest: digest,
                   logical_size: 12
                 })

        assert is_binary(expires_at)

        assert {:ok, %{digests: [^digest], next_after_digest: nil}} =
                 @adapter.list_live_attachment_digests(adapter, %{})

        assert {:ok, %{digest: ^digest, logical_size: 12}} =
                 @adapter.resolve_blob_metadata(adapter, %{digest: digest})
      end

      test "integrity passes on a fresh database", %{adapter: adapter} do
        assert {:ok, report} = @adapter.integrity_check(adapter, %{})
        assert report.ok == true
        assert is_map(Map.get(report, :backend_details) || %{})
      end

      test "boundary page round-trip preserves source-qualified install", %{adapter: adapter} do
        assert {:ok, %{database_uuid: uuid, history_epoch: epoch}} = @adapter.identity(adapter)

        assert {:ok, _} =
                 @adapter.update_config(adapter, %{
                   "retention" => %{
                     "mode" => "stable_frontier",
                     "history_depth" => 0,
                     "peer_expiry_ms" => 86_400_000,
                     "schedule" => "disabled"
                   }
                 })

        assert {:ok, %{revision: root}} =
                 @adapter.apply_local_mutation(adapter, %{
                   operation: :put,
                   document_id: "doc",
                   body: %{"n" => 1}
                 })

        assert {:ok, _} =
                 @adapter.apply_local_mutation(adapter, %{
                   operation: :put,
                   document_id: "doc",
                   if_revision: root,
                   body: %{"n" => 2}
                 })

        assert {:ok, _} = @adapter.compact_retention(adapter, %{})

        assert {:ok, page} =
                 @adapter.read_boundary_pages(adapter, %{limit: 10})

        assert page.source_database_uuid == uuid
        assert page.source_history_epoch == epoch
        assert is_binary(page.boundary_digest)
        assert is_list(page.boundaries)
      end
    end
  end
end
