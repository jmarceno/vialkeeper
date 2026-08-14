defmodule ElixirDB.StorageAdapter.CompactionTest do
  use ElixirDB.Storage.AdapterCase, adapter: ElixirDB.Storage.SQLite.Adapter

  @moduletag :sqlite_physical

  alias ElixirDB.Storage.PortFault
  alias ElixirDB.Storage.SQLite.{Connection, DocumentFacts}
  alias ElixirDB.TestRevisionId, as: Id

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

  test "compaction preserves the materialized winner", %{adapter: adapter, path: path} do
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

    assert {:ok, _} = @adapter.get_revision(adapter, %{document_id: "doc", revision_id: winner})

    assert {:error, %ElixirDB.Error{code: :revision_not_found}} =
             @adapter.get_revision(adapter, %{document_id: "doc", revision_id: root})

    assert revision_count(path) == 1
  end

  test "compaction removes changes at or below the floor without rewriting survivors", %{
    adapter: adapter
  } do
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

    assert {:ok, %{results: survivor}} =
             @adapter.read_changes(adapter, %{since: 2, limit: 10})

    assert {:ok, _} = @adapter.compact_retention(adapter, %{})

    assert {:error, %ElixirDB.Error{code: :history_truncated}} =
             @adapter.read_changes(adapter, %{since: 0, limit: 10})

    assert {:ok, %{results: ^survivor, last_sequence: 3}} =
             @adapter.read_changes(adapter, %{since: 2, limit: 10})
  end

  test "history_depth zero keeps only the winner revision", %{adapter: adapter, path: path} do
    assert {:ok, _} =
             @adapter.update_config(adapter, %{
               "retention" => %{
                 "mode" => "stable_frontier",
                 "history_depth" => 0,
                 "peer_expiry_ms" => 86_400_000,
                 "schedule" => "disabled"
               }
             })

    assert {:ok, %{revision: v1}} =
             @adapter.apply_local_mutation(adapter, %{
               operation: :put,
               document_id: "doc",
               body: %{"n" => 1}
             })

    assert {:ok, %{revision: v2}} =
             @adapter.apply_local_mutation(adapter, %{
               operation: :put,
               document_id: "doc",
               if_revision: v1,
               body: %{"n" => 2}
             })

    assert {:ok, %{revision: v3}} =
             @adapter.apply_local_mutation(adapter, %{
               operation: :put,
               document_id: "doc",
               if_revision: v2,
               body: %{"n" => 3}
             })

    assert {:ok, _} = @adapter.compact_retention(adapter, %{})

    assert revision_count(path) == 1
    assert {:ok, _} = @adapter.get_revision(adapter, %{document_id: "doc", revision_id: v3})
  end

  test "compaction removes eligible losing branches", %{adapter: adapter, path: path} do
    assert {:ok, _} =
             @adapter.update_config(adapter, %{
               "retention" => %{
                 "mode" => "stable_frontier",
                 "history_depth" => 0,
                 "peer_expiry_ms" => 86_400_000,
                 "schedule" => "disabled"
               }
             })

    {:ok, root} = Id.calculate("doc", nil, false, %{"n" => 0})
    {:ok, left} = Id.calculate("doc", root, false, %{"n" => 1})
    {:ok, right} = Id.calculate("doc", root, false, %{"n" => 2})

    assert {:ok, _} =
             @adapter.import_revision_chains(adapter, %{
               chains: [
                 %{
                   document_id: "doc",
                   leaf_revision: left,
                   revisions: [
                     wire("doc", root, nil, false, %{"n" => 0}),
                     wire("doc", left, root, false, %{"n" => 1})
                   ]
                 },
                 %{
                   document_id: "doc",
                   leaf_revision: right,
                   revisions: [
                     wire("doc", root, nil, false, %{"n" => 0}),
                     wire("doc", right, root, false, %{"n" => 2})
                   ]
                 }
               ]
             })

    assert revision_count(path) == 3
    assert {:ok, _} = @adapter.compact_retention(adapter, %{})
    assert revision_count(path) == 1
  end

  test "injected mid-transaction fault rolls back compaction", %{adapter: adapter, path: path} do
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

    assert {:ok, %{revision: _winner}} =
             @adapter.apply_local_mutation(adapter, %{
               operation: :put,
               document_id: "doc",
               if_revision: root,
               body: %{"n" => 2}
             })

    fault_adapter =
      PortFault.inject(
        adapter,
        :compact_retention_mid_tx,
        {:once, ElixirDB.Error.internal_error("injected compaction fault")}
      )

    assert {:error, %ElixirDB.Error{code: :internal_error}} =
             PortFault.compact_retention(fault_adapter, %{})

    assert revision_count(path) == 2
    assert {:ok, _} = @adapter.get_revision(adapter, %{document_id: "doc", revision_id: root})

    assert {:ok, %{retention_floor_sequence: 0}} = @adapter.identity(adapter)
    assert {:ok, %{results: [_, _]}} = @adapter.read_changes(adapter, %{since: 0, limit: 10})
  end

  test "compaction metadata query errors are returned", %{adapter: adapter} do
    assert {:ok, _} =
             @adapter.apply_local_mutation(adapter, %{
               operation: :put,
               document_id: "doc",
               body: %{"n" => 1}
             })

    assert :ok =
             Connection.execute(
               adapter.conn,
               "ALTER TABLE revisions RENAME TO revisions_unavailable"
             )

    context = @adapter.to_context(adapter)

    assert {:error, %ElixirDB.Error{code: :internal_error}} =
             DocumentFacts.list_compaction_documents(context, 1)
  end

  defp revision_count(path) do
    {:ok, conn} = Connection.open(path, mode: [:readonly])

    try do
      {:ok, [[count]]} = Connection.query(conn, "SELECT COUNT(*) FROM revisions")
      count
    after
      Connection.close(conn)
    end
  end
end
