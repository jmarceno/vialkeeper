defmodule ElixirDB.StorageAdapter.RetentionContractTest do
  @moduledoc """
  Dual-backend contract coverage for retention compaction, boundary keys,
  integrity after trim, peer regression, and attachment metadata invariants.
  """

  use ExUnit.Case, async: true

  alias ElixirDB.Domain.BoundaryPage
  alias ElixirDB.Storage.AdapterCase

  @adapters [ElixirDB.Storage.SQLite.Adapter, ElixirDB.Storage.Memory.Adapter]
  @digest_a String.duplicate("a", 64)
  @digest_b String.duplicate("b", 64)
  @digest_c String.duplicate("c", 64)

  @retention_config %{
    "retention" => %{
      "mode" => "stable_frontier",
      "history_depth" => 0,
      "peer_expiry_ms" => 86_400_000,
      "schedule" => "disabled"
    }
  }

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
        assert {:ok, _} = @adapter.update_config(adapter, @retention_config)

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
        assert {:ok, _} = @adapter.update_config(adapter, @retention_config)

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

      test "peer position regression is rejected", %{adapter: adapter} do
        assert {:ok, %{database_uuid: uuid, history_epoch: epoch}} = @adapter.identity(adapter)
        future = DateTime.utc_now() |> DateTime.add(60, :second) |> DateTime.to_iso8601()
        now = DateTime.utc_now() |> DateTime.to_iso8601()
        peer_uuid = "cccccccc-cccc-4ccc-8ccc-cccccccccccc"

        base = %{
          peer_database_uuid: peer_uuid,
          peer_history_epoch: "dddddddd-dddd-4ddd-8ddd-dddddddddddd",
          source_database_uuid: uuid,
          source_history_epoch: epoch,
          safe_source_sequence: 5,
          installed_source_compaction_epoch: 1,
          last_seen_at: now,
          lease_expires_at: future,
          status: :active
        }

        assert {:ok, _} =
                 @adapter.put_peer_position_cas(adapter, %{
                   expected_version: 0,
                   value: base
                 })

        assert {:error, %ElixirDB.Error{code: :rebase_required}} =
                 @adapter.put_peer_position_cas(adapter, %{
                   expected_version: 1,
                   value: %{base | safe_source_sequence: 3}
                 })
      end

      test "compaction trims ancestors while integrity allows dangling parents", %{
        adapter: adapter
      } do
        assert {:ok, _} = @adapter.update_config(adapter, @retention_config)

        assert {:ok, %{revision: root}} =
                 @adapter.apply_local_mutation(adapter, %{
                   operation: :put,
                   document_id: "doc",
                   body: %{"n" => 1}
                 })

        assert {:ok, %{revision: winner}} =
                 @adapter.apply_local_mutation(adapter, %{
                   operation: :put,
                   document_id: "doc",
                   if_revision: root,
                   body: %{"n" => 2}
                 })

        assert {:ok, _} = @adapter.compact_retention(adapter, %{})

        assert {:ok, %{body: %{"n" => 2}}} =
                 @adapter.get_document(adapter, %{document_id: "doc"})

        assert {:ok, _} =
                 @adapter.get_revision(adapter, %{document_id: "doc", revision_id: winner})

        assert {:error, %ElixirDB.Error{code: :revision_not_found}} =
                 @adapter.get_revision(adapter, %{document_id: "doc", revision_id: root})

        assert {:ok, report} = @adapter.integrity_check(adapter, %{})
        assert report.ok == true
      end

      test "live attachment digests survive compaction of trimmed ancestors", %{adapter: adapter} do
        assert {:ok, _} = @adapter.update_config(adapter, @retention_config)

        assert {:ok, _} =
                 @adapter.protect_pending_blob(adapter, %{digest: @digest_a, logical_size: 4})

        assert {:ok, %{revision: root}} =
                 @adapter.apply_local_mutation(adapter, %{
                   operation: :put,
                   document_id: "doc",
                   body: %{"n" => 1},
                   attachments: %{
                     "a.bin" => %{
                       digest: @digest_a,
                       length: 4,
                       content_type: "application/octet-stream"
                     }
                   }
                 })

        assert {:ok, _} =
                 @adapter.apply_local_mutation(adapter, %{
                   operation: :put,
                   document_id: "doc",
                   if_revision: root,
                   body: %{"n" => 2}
                 })

        assert {:ok, _} = @adapter.compact_retention(adapter, %{})

        assert {:ok, %{digests: digests}} =
                 @adapter.list_live_attachment_digests(adapter, %{})

        assert @digest_a in digests

        assert {:ok, %{logical_size: 4}} =
                 @adapter.resolve_blob_metadata(adapter, %{digest: @digest_a})
      end

      test "source-qualified NUL boundary keys tolerate document ids with : and /", %{
        adapter: adapter
      } do
        assert {:ok, %{database_uuid: uuid, history_epoch: epoch}} = @adapter.identity(adapter)
        assert {:ok, _} = @adapter.update_config(adapter, @retention_config)
        document_id = "ns:team/docs"

        assert {:ok, %{revision: root}} =
                 @adapter.apply_local_mutation(adapter, %{
                   operation: :put,
                   document_id: document_id,
                   body: %{"n" => 1}
                 })

        assert {:ok, _} =
                 @adapter.apply_local_mutation(adapter, %{
                   operation: :put,
                   document_id: document_id,
                   if_revision: root,
                   body: %{"n" => 2}
                 })

        assert {:ok, _} = @adapter.compact_retention(adapter, %{})

        assert {:ok, page} = @adapter.read_boundary_pages(adapter, %{limit: 10})
        assert page.source_database_uuid == uuid
        assert page.source_history_epoch == epoch

        boundary = Enum.find(page.boundaries, &(&1.document_id == document_id))
        assert boundary != nil

        key = BoundaryPage.record_key(uuid, document_id, boundary.history_id)
        assert BoundaryPage.parse_record_key(key) == {uuid, document_id, boundary.history_id}
        refute String.contains?(document_id, <<0>>)
        assert String.contains?(key, <<0>>)
      end

      test "pending protection, live digest paging, cleanup, and union", %{adapter: adapter} do
        assert {:ok, %{digest: @digest_a, expires_at: expires}} =
                 @adapter.protect_pending_blob(adapter, %{digest: @digest_a, logical_size: 1})

        assert is_binary(expires)

        assert {:ok, _} =
                 @adapter.protect_pending_blob(adapter, %{digest: @digest_b, logical_size: 2})

        assert {:ok, _} =
                 @adapter.protect_pending_blob(adapter, %{digest: @digest_c, logical_size: 3})

        assert {:ok, %{revision: _}} =
                 @adapter.apply_local_mutation(adapter, %{
                   operation: :put,
                   document_id: "doc",
                   body: %{"n" => 1},
                   attachments: %{
                     "a.bin" => %{
                       digest: @digest_a,
                       length: 1,
                       content_type: "application/octet-stream"
                     }
                   }
                 })

        assert {:ok, %{digests: first_page, next_after_digest: cursor}} =
                 @adapter.list_live_attachment_digests(adapter, %{limit: 2})

        assert match?([_, _], first_page)
        assert cursor == List.last(first_page)

        assert {:ok, %{digests: rest, next_after_digest: nil}} =
                 @adapter.list_live_attachment_digests(adapter, %{
                   after_digest: cursor,
                   limit: 2
                 })

        assert Enum.sort([@digest_a, @digest_b, @digest_c]) == Enum.sort(first_page ++ rest)

        future = DateTime.utc_now() |> DateTime.add(200_000, :second) |> DateTime.to_iso8601()

        assert {:ok, %{removed: removed}} =
                 @adapter.cleanup_expired_pending_blobs(adapter, %{now: future})

        assert removed >= 1

        assert {:ok, %{digests: after_cleanup}} =
                 @adapter.list_live_attachment_digests(adapter, %{})

        assert @digest_a in after_cleanup
        refute @digest_b in after_cleanup
        refute @digest_c in after_cleanup
      end

      test "integrity passes on a fresh database", %{adapter: adapter} do
        assert {:ok, report} = @adapter.integrity_check(adapter, %{})
        assert report.ok == true
        assert is_map(Map.get(report, :backend_details) || %{})
      end

      test "boundary page round-trip preserves source-qualified install", %{adapter: adapter} do
        assert {:ok, %{database_uuid: uuid, history_epoch: epoch}} = @adapter.identity(adapter)
        assert {:ok, _} = @adapter.update_config(adapter, @retention_config)

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
