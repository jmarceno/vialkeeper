defmodule ElixirDB.Retention.ReviewerFixesTest do
  use ExUnit.Case, async: false

  @moduletag :integration

  alias ElixirDB.Config
  alias ElixirDB.Documents
  alias ElixirDB.Domain.{BoundaryPage, RetentionBoundary}
  alias ElixirDB.MapAccess
  alias ElixirDB.Replication
  alias ElixirDB.Replication.CheckpointReconciler
  alias ElixirDB.Replication.LocalEndpoint
  alias ElixirDB.Runtime.DatabaseCatalog
  alias ElixirDB.Storage.AdapterCase
  alias ElixirDB.Storage.SQLite.Adapter
  alias ElixirDB.Storage.SQLite.RetentionRecords
  alias ElixirDB.TempDatabase
  alias ElixirDB.TestServer

  import AdapterCase, only: [wire_revision: 6]

  @retention_config %{
    "retention" => %{
      "mode" => "stable_frontier",
      "history_depth" => 0,
      "peer_expiry_ms" => 86_400_000,
      "schedule" => "disabled"
    }
  }

  describe "cross-epoch boundary install" do
    test "installs boundary page from source onto target with different history epochs" do
      {:ok, source} = open_db("reviewer-boundary-src")
      {:ok, target} = open_db("reviewer-boundary-tgt")

      assert {:ok, _} =
               DatabaseCatalog.command(
                 source.database_uuid,
                 {:command, :update_config, @retention_config}
               )

      assert {:ok, _} =
               Documents.put(source.database_uuid, %{id: "doc", body: %{"n" => 1}})

      assert {:ok, _} =
               DatabaseCatalog.command(source.database_uuid, {:command, :compact_retention, %{}})

      {:ok, source_identity} =
        DatabaseCatalog.command(source.database_uuid, {:command, :identity, %{}})

      {:ok, target_identity} =
        DatabaseCatalog.command(target.database_uuid, {:command, :identity, %{}})

      refute source_identity.history_epoch == target_identity.history_epoch

      {:ok, page} =
        DatabaseCatalog.command(source.database_uuid, {:command, :read_boundary_pages, %{}})

      install_page =
        page
        |> Map.from_struct()
        |> Map.put(:source_database_uuid, source.database_uuid)
        |> Map.new(fn {k, v} -> {Atom.to_string(k), v} end)

      assert {:ok, _} =
               DatabaseCatalog.command(
                 target.database_uuid,
                 {:command, :install_boundary_pages, install_page}
               )
    end
  end

  describe "complete-set boundary digest" do
    test "multi-page install verifies digest across all pages" do
      {:ok, db} = open_db("reviewer-digest")

      assert {:ok, _} =
               DatabaseCatalog.command(
                 db.database_uuid,
                 {:command, :update_config, @retention_config}
               )

      for id <- ["a", "b", "c"] do
        assert {:ok, %{revision: root}} =
                 Documents.put(db.database_uuid, %{id: id, body: %{"n" => 1}})

        assert {:ok, _} =
                 Documents.put(db.database_uuid, %{
                   id: id,
                   if_revision: root,
                   body: %{"n" => 2}
                 })
      end

      assert {:ok, _} =
               DatabaseCatalog.command(db.database_uuid, {:command, :compact_retention, %{}})

      {:ok, page1} =
        DatabaseCatalog.command(db.database_uuid, {:command, :read_boundary_pages, %{"limit" => 1}})

      {:ok, page2} =
        DatabaseCatalog.command(
          db.database_uuid,
          {:command, :read_boundary_pages,
           %{
             "limit" => 1,
             "cursor" => page1.next_page
           }}
        )

      assert page1.boundary_digest == page2.boundary_digest
      assert page1.next_page != nil
      assert page2.next_page == nil
    end

    test "replaces stale boundaries only after the complete set verifies" do
      {:ok, adapter} = open_adapter("reviewer-boundary-replace")

      source_uuid = "source-boundary-replace"
      source_epoch = "source-history-replace"
      install_id = "%boundary_install_#{System.unique_integer([:positive])}"

      stale = RetentionBoundary.active("stale%_document", "stale-history", 1, [])
      current_a = RetentionBoundary.active("current-a", "current-a-history", 1, [])
      current_b = RetentionBoundary.active("current-b", "current-b-history", 1, [])
      digest = BoundaryPage.digest_for([current_a, current_b])

      assert {:ok, _} =
               Adapter.install_boundary_pages(adapter, %{
                 source_database_uuid: source_uuid,
                 source_history_epoch: source_epoch,
                 compaction_epoch: 1,
                 boundary_digest: BoundaryPage.digest_for([stale]),
                 next_page: nil,
                 boundaries: [stale],
                 install_id: nil,
                 replace: false
               })

      assert {:ok, _} =
               Adapter.install_boundary_pages(adapter, %{
                 source_database_uuid: source_uuid,
                 source_history_epoch: source_epoch,
                 compaction_epoch: 1,
                 boundary_digest: digest,
                 next_page: "next-page",
                 boundaries: [current_a],
                 install_id: install_id,
                 replace: true
               })

      assert {:ok, _} =
               Adapter.install_boundary_pages(adapter, %{
                 source_database_uuid: source_uuid,
                 source_history_epoch: source_epoch,
                 compaction_epoch: 1,
                 boundary_digest: digest,
                 next_page: nil,
                 boundaries: [current_b],
                 install_id: install_id,
                 replace: false
               })

      assert {:ok, boundaries} =
               RetentionRecords.list_boundaries(adapter.conn, source_database_uuid: source_uuid)

      assert Enum.map(boundaries, & &1.boundary.document_id) == ["current-a", "current-b"]
    end
  end

  describe "retired branch roots" do
    test "compaction records retired_branch_roots and import of losing revision is no-op" do
      {:ok, adapter} = open_adapter("reviewer-branch")

      assert {:ok, %{revision: root}} =
               Adapter.apply_local_mutation(adapter, %{
                 operation: :put,
                 document_id: "doc",
                 body: %{"n" => 1}
               })

      assert {:ok, %{revision: winner}} =
               Adapter.apply_local_mutation(adapter, %{
                 operation: :put,
                 document_id: "doc",
                 if_revision: root,
                 body: %{"n" => 2}
               })

      assert {:ok, _} = Adapter.update_config(adapter, @retention_config)
      assert {:ok, _} = Adapter.compact_retention(adapter, %{})

      {:ok, %{database_uuid: uuid}} = Adapter.identity(adapter)

      {:ok, stored_boundaries} =
        RetentionRecords.list_boundaries(
          adapter.conn,
          source_database_uuid: uuid
        )

      history_id =
        stored_boundaries
        |> Enum.find(fn %{boundary: boundary} -> boundary.document_id == "doc" end)
        |> then(fn %{boundary: boundary} -> boundary.history_id end)

      assert {:error, %ElixirDB.Error{code: :revision_not_found}} =
               Adapter.get_revision(adapter, %{document_id: "doc", revision_id: root})

      losing_chain = [
        %{
          document_id: "doc",
          history_id: history_id,
          leaf_revision: root,
          truncated: false,
          revisions: [
            wire_revision("doc", root, nil, false, %{"n" => 1}, history_id)
          ]
        }
      ]

      assert {:ok, %{revisions_inserted: 0}} =
               Adapter.import_revision_chains(adapter, %{chains: losing_chain})

      assert {:ok, %{body: %{"n" => 2}}} =
               Adapter.get_document(adapter, %{document_id: "doc"})

      refute winner == root
    end
  end

  describe "diff history_id" do
    test "diff carries history_id and fresh history is not compacted" do
      {:ok, adapter} = open_adapter("reviewer-diff")

      {:ok, %{revision: _rev}} =
        Adapter.apply_local_mutation(adapter, %{
          operation: :put,
          document_id: "fresh",
          body: %{"n" => 1}
        })

      {:ok, %{database_uuid: uuid}} = Adapter.identity(adapter)

      fake_revision = "9-fake#{String.duplicate("0", 60)}"

      {:ok, %{documents: [result]}} =
        Adapter.diff_revisions(adapter, %{
          source_database_uuid: uuid,
          documents: [
            %{
              document_id: "fresh",
              leaf_revisions: [%{revision: fake_revision, history_id: "unknown-history"}]
            }
          ]
        })

      assert result.missing_revisions == [
               %{"revision" => fake_revision, "history_id" => "unknown-history"}
             ]

      assert result.compacted_revisions == []
    end
  end

  describe "HTTP retention routes" do
    test "boundary install with full page fields returns 200" do
      server = TestServer.start_supervised!()
      {:ok, db} = open_db("reviewer-http-install")

      {:ok, %{status: 200, body: page}} =
        Req.post(
          server.base_url <> "/v1/databases/#{db.database_uuid}/replication/boundaries",
          json: %{}
        )

      install_body =
        Map.merge(page["data"], %{
          "source_database_uuid" => db.database_uuid,
          "boundaries" => page["data"]["boundaries"]
        })

      assert {:ok, %{status: 200}} =
               Req.post(
                 server.base_url <>
                   "/v1/databases/#{db.database_uuid}/replication/boundaries/install",
                 json: install_body
               )
    end

    test "peer put/get with peer_database_uuid path works" do
      server = TestServer.start_supervised!()
      {:ok, db} = open_db("reviewer-http-peer")

      {:ok, %{database_uuid: uuid, history_epoch: epoch}} =
        DatabaseCatalog.command(db.database_uuid, {:command, :identity, %{}})

      peer_uuid = "cccccccc-cccc-4ccc-8ccc-cccccccccccc"
      future = DateTime.utc_now() |> DateTime.add(60, :second) |> DateTime.to_iso8601()

      body = %{
        "expected_version" => 0,
        "peer_database_uuid" => peer_uuid,
        "peer_history_epoch" => "dddddddd-dddd-4ddd-8ddd-dddddddddddd",
        "source_database_uuid" => uuid,
        "source_history_epoch" => epoch,
        "safe_source_sequence" => 0,
        "installed_source_compaction_epoch" => 0,
        "last_seen_at" => DateTime.utc_now() |> DateTime.to_iso8601(),
        "lease_expires_at" => future,
        "status" => "active"
      }

      assert {:ok, %{status: 200}} =
               Req.put(
                 server.base_url <>
                   "/v1/databases/#{db.database_uuid}/replication/peers/#{peer_uuid}",
                 json: body
               )

      assert {:ok, %{status: 200, body: fetched}} =
               Req.get(
                 server.base_url <>
                   "/v1/databases/#{db.database_uuid}/replication/peers/#{peer_uuid}"
               )

      assert fetched["data"]["value"]["peer_database_uuid"] == peer_uuid
    end

    test "peer put with wrong source_database_uuid rejected" do
      server = TestServer.start_supervised!()
      {:ok, db} = open_db("reviewer-http-peer-bad")

      peer_uuid = "eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee"
      future = DateTime.utc_now() |> DateTime.add(60, :second) |> DateTime.to_iso8601()

      body = %{
        "expected_version" => 0,
        "peer_database_uuid" => peer_uuid,
        "peer_history_epoch" => "ffffffff-ffff-4fff-8fff-ffffffffffff",
        "source_database_uuid" => "00000000-0000-4000-8000-000000000099",
        "source_history_epoch" => "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
        "safe_source_sequence" => 0,
        "installed_source_compaction_epoch" => 0,
        "last_seen_at" => DateTime.utc_now() |> DateTime.to_iso8601(),
        "lease_expires_at" => future,
        "status" => "active"
      }

      assert {:ok, %{status: 400}} =
               Req.put(
                 server.base_url <>
                   "/v1/databases/#{db.database_uuid}/replication/peers/#{peer_uuid}",
                 json: body
               )
    end
  end

  describe "peer epoch replacement" do
    test "epoch change after bootstrap is accepted" do
      {:ok, db} = open_db("reviewer-peer-epoch")

      {:ok, %{database_uuid: uuid, history_epoch: epoch}} =
        DatabaseCatalog.command(db.database_uuid, {:command, :identity, %{}})

      peer_uuid = "11111111-1111-4111-8111-111111111111"
      future = DateTime.utc_now() |> DateTime.add(60, :second) |> DateTime.to_iso8601()
      now = DateTime.utc_now() |> DateTime.to_iso8601()

      base = %{
        peer_database_uuid: peer_uuid,
        peer_history_epoch: "22222222-2222-4222-8222-222222222222",
        source_database_uuid: uuid,
        source_history_epoch: epoch,
        safe_source_sequence: 5,
        installed_source_compaction_epoch: 1,
        last_seen_at: now,
        lease_expires_at: future,
        status: :active
      }

      assert {:ok, _} =
               DatabaseCatalog.command(
                 db.database_uuid,
                 {:command, :put_peer_position_cas,
                  %{
                    expected_version: 0,
                    value: base
                  }}
               )

      replacement =
        Map.merge(base, %{
          source_history_epoch: "33333333-3333-4333-8333-333333333333",
          safe_source_sequence: 0,
          installed_source_compaction_epoch: 0,
          status: :bootstrap_required
        })

      assert {:ok, _} =
               DatabaseCatalog.command(
                 db.database_uuid,
                 {:command, :put_peer_position_cas,
                  %{
                    expected_version: 1,
                    value: replacement
                  }}
               )
    end

    test "rejects an active peer incarnation change until bootstrap is reported" do
      {:ok, db} = open_db("reviewer-peer-incarnation")

      {:ok, %{database_uuid: uuid, history_epoch: epoch}} =
        DatabaseCatalog.command(db.database_uuid, {:command, :identity, %{}})

      peer_uuid = "44444444-4444-4444-8444-444444444444"
      future = DateTime.utc_now() |> DateTime.add(60, :second) |> DateTime.to_iso8601()
      now = DateTime.utc_now() |> DateTime.to_iso8601()

      base = %{
        peer_database_uuid: peer_uuid,
        peer_history_epoch: "55555555-5555-4555-8555-555555555555",
        source_database_uuid: uuid,
        source_history_epoch: epoch,
        safe_source_sequence: 0,
        installed_source_compaction_epoch: 0,
        last_seen_at: now,
        lease_expires_at: future,
        status: :active
      }

      assert {:ok, _} =
               DatabaseCatalog.command(
                 db.database_uuid,
                 {:command, :put_peer_position_cas, %{expected_version: 0, value: base}}
               )

      incarnation = %{base | peer_history_epoch: "66666666-6666-4666-8666-666666666666"}

      assert {:error, %ElixirDB.Error{code: :rebase_required}} =
               DatabaseCatalog.command(
                 db.database_uuid,
                 {:command, :put_peer_position_cas, %{expected_version: 1, value: incarnation}}
               )

      assert {:ok, _} =
               DatabaseCatalog.command(
                 db.database_uuid,
                 {:command, :put_peer_position_cas,
                  %{expected_version: 1, bootstrap_completed: true, value: incarnation}}
               )
    end
  end

  describe "random history ids" do
    test "two masters creating same doc id get different history_ids" do
      {:ok, a} = open_adapter("reviewer-hist-a")
      {:ok, b} = open_adapter("reviewer-hist-b")

      assert {:ok, %{revision: rev_a}} =
               Adapter.apply_local_mutation(a, %{
                 operation: :put,
                 document_id: "shared",
                 body: %{"n" => 1}
               })

      assert {:ok, %{revision: rev_b}} =
               Adapter.apply_local_mutation(b, %{
                 operation: :put,
                 document_id: "shared",
                 body: %{"n" => 1}
               })

      assert revision_history_id(a, "shared", rev_a) != revision_history_id(b, "shared", rev_b)
    end
  end

  describe "checkpoint epoch" do
    test "checkpoint without source_history_epoch rejected on HTTP" do
      server = TestServer.start_supervised!()
      {:ok, db} = open_db("reviewer-checkpoint")

      body = %{
        "expected_checkpoint_version" => 0,
        "version" => 1,
        "checkpoint_version" => 1,
        "replication_id" => "repl-test",
        "session_id" => "session-test",
        "source_sequence" => 0,
        "history" => [],
        "source_compaction_epoch" => 0,
        "safe_source_sequence" => 0,
        "installed_source_compaction_epoch" => 0
      }

      assert {:ok, %{status: 400}} =
               Req.put(
                 server.base_url <>
                   "/v1/databases/#{db.database_uuid}/replication/checkpoints/repl-test",
                 json: body
               )
    end

    test "reconcile forces bootstrap when checkpoint epoch missing" do
      source = %{"history_epoch" => "epoch-src"}
      target = %{}

      result =
        CheckpointReconciler.reconcile(%{}, target, source)

      assert result.bootstrap_required
      assert result.reason == :epoch_mismatch
    end
  end

  describe "bootstrap validation" do
    test "bootstrap limit 0 returns 400 not 500" do
      {:ok, adapter} = open_adapter("reviewer-bootstrap")

      assert {:error, %ElixirDB.Error{code: :invalid_request}} =
               Adapter.get_revision_chains(adapter, %{bootstrap: true, limit: 0})
    end

    test "purge manifests cannot name a different source database" do
      {:ok, adapter} = open_adapter("reviewer-purge-source-check")
      boundary = RetentionBoundary.retired("purge-check", "purge-history", [])

      assert {:error, %ElixirDB.Error{code: :invalid_request}} =
               Adapter.import_revision_chains(adapter, %{
                 chains: [],
                 source_database_uuid: "expected-source",
                 purged_boundaries: [
                   RetentionRecords.encode_boundary(
                     boundary,
                     "different-source",
                     "different-history",
                     1
                   )
                 ]
               })
    end
  end

  describe "all-deleted history removal" do
    test "fully removes history including tombstone" do
      {:ok, adapter} = open_adapter("reviewer-all-deleted")

      assert {:ok, _} =
               Adapter.update_config(adapter, @retention_config)

      assert {:ok, %{revision: root}} =
               Adapter.apply_local_mutation(adapter, %{
                 operation: :put,
                 document_id: "gone",
                 body: %{"n" => 1}
               })

      assert {:ok, _} =
               Adapter.apply_local_mutation(adapter, %{
                 operation: :delete,
                 document_id: "gone",
                 if_revision: root
               })

      assert {:ok, _} = Adapter.compact_retention(adapter, %{})

      assert {:error, %ElixirDB.Error{code: :document_not_found}} =
               Adapter.get_document(adapter, %{document_id: "gone"})

      assert {:error, %ElixirDB.Error{code: :revision_not_found}} =
               Adapter.get_revision(adapter, %{document_id: "gone", revision_id: root})
    end

    test "allows a fresh root after the old history is fully purged" do
      {:ok, adapter} = open_adapter("reviewer-recreate")

      assert {:ok, _} = Adapter.update_config(adapter, @retention_config)

      assert {:ok, %{revision: root}} =
               Adapter.apply_local_mutation(adapter, %{
                 operation: :put,
                 document_id: "recreated",
                 body: %{"version" => 1}
               })

      assert {:ok, _} =
               Adapter.apply_local_mutation(adapter, %{
                 operation: :delete,
                 document_id: "recreated",
                 if_revision: root
               })

      assert {:ok, _} = Adapter.compact_retention(adapter, %{})

      assert {:ok, %{revision: replacement}} =
               Adapter.apply_local_mutation(adapter, %{
                 operation: :put,
                 document_id: "recreated",
                 body: %{"version" => 2}
               })

      assert replacement != root

      assert {:ok, %{body: %{"version" => 2}}} =
               Adapter.get_document(adapter, %{document_id: "recreated"})
    end
  end

  describe "bootstrap purge manifests" do
    test "remove a stale target document when the source retained only its purge boundary" do
      {:ok, source} = open_db("reviewer-purge-source")
      {:ok, target} = open_db("reviewer-purge-target")

      assert {:ok, _} =
               DatabaseCatalog.command(
                 source.database_uuid,
                 {:command, :update_config, @retention_config}
               )

      assert {:ok, %{revision: initial_revision}} =
               Documents.put(source.database_uuid, %{id: "purged", body: %{"v" => 1}})

      assert {:ok, %{status: :completed}} =
               Replication.one_shot(source.database_uuid, target.database_uuid)

      assert {:ok, %{body: %{"v" => 1}}} =
               Documents.get(target.database_uuid, %{id: "purged"})

      assert {:ok, _} =
               Documents.delete(source.database_uuid, %{
                 id: "purged",
                 if_revision: initial_revision
               })

      assert {:ok, %{current_sequence: 2}} =
               DatabaseCatalog.command(source.database_uuid, {:command, :identity, %{}})

      assert {:ok, _} = update_peer_safe(source.database_uuid, target.database_uuid, 2)

      assert {:ok, %{new_floor: 2}} =
               DatabaseCatalog.command(source.database_uuid, {:command, :compact_retention, %{}})

      assert {:error, %ElixirDB.Error{code: :document_not_found}} =
               Documents.get(source.database_uuid, %{id: "purged"})

      assert {:ok, %{status: :completed}} =
               Replication.one_shot(source.database_uuid, target.database_uuid)

      assert {:error, %ElixirDB.Error{code: :document_not_found}} =
               Documents.get(target.database_uuid, %{id: "purged"})
    end
  end

  describe "active compaction refresh" do
    test "installs newer boundaries before acknowledging a caught-up peer" do
      {:ok, source} = open_db("reviewer-refresh-source")
      {:ok, target} = open_db("reviewer-refresh-target")

      assert {:ok, _} =
               DatabaseCatalog.command(
                 source.database_uuid,
                 {:command, :update_config, @retention_config}
               )

      assert {:ok, %{revision: root}} =
               Documents.put(source.database_uuid, %{id: "refresh", body: %{"v" => 1}})

      assert {:ok, _} =
               Documents.put(source.database_uuid, %{
                 id: "refresh",
                 if_revision: root,
                 body: %{"v" => 2}
               })

      assert {:ok, %{status: :completed}} =
               Replication.one_shot(source.database_uuid, target.database_uuid)

      assert {:ok, %{new_floor: 2, new_compaction_epoch: 1}} =
               DatabaseCatalog.command(source.database_uuid, {:command, :compact_retention, %{}})

      assert {:ok, %{status: :completed}} =
               Replication.one_shot(source.database_uuid, target.database_uuid)

      assert {:ok, %{body: %{"v" => 2}}} =
               Documents.get(target.database_uuid, %{id: "refresh"})

      assert {:ok, %{value: state}} =
               DatabaseCatalog.command(
                 target.database_uuid,
                 {:command, :get_local_record, "retention_boundary_state", source.database_uuid}
               )

      assert MapAccess.get(state, :compaction_epoch) == 1

      assert {:ok, %{value: peer}} =
               DatabaseCatalog.command(
                 source.database_uuid,
                 {:command, :get_local_record, "peer_ledger", target.database_uuid}
               )

      assert MapAccess.get(peer, :installed_source_compaction_epoch) == 1
    end
  end

  describe "safe report pending causal" do
    test "pending is peer-scoped and clears after reverse acknowledgment" do
      {:ok, a} = open_db("reviewer-safe-a")
      {:ok, b} = open_db("reviewer-safe-b")

      assert {:ok, _} =
               DatabaseCatalog.command(
                 a.database_uuid,
                 {:command, :update_config, @retention_config}
               )

      assert {:ok, _} =
               Documents.put(a.database_uuid, %{id: "seed", body: %{"n" => 1}})

      assert {:ok, %{status: :completed}} = Replication.one_shot(a.database_uuid, b.database_uuid)

      assert {:ok, _} =
               Documents.put(b.database_uuid, %{id: "local", body: %{"x" => 1}})

      {:ok, target} = LocalEndpoint.new(b.database_uuid)
      assert {:ok, true} = LocalEndpoint.has_local_origin_changes?(target)
      assert {:ok, true} = LocalEndpoint.has_local_origin_changes?(target, a.database_uuid)

      assert {:ok, %{status: :completed}} = Replication.one_shot(b.database_uuid, a.database_uuid)

      assert {:ok, false} = LocalEndpoint.has_local_origin_changes?(target, a.database_uuid)
    end
  end

  defp open_db(prefix) do
    path = "#{prefix}-#{System.unique_integer([:positive])}.elixirdb"
    root = Config.database_root()
    TempDatabase.cleanup(Path.join(root, path))
    DatabaseCatalog.create(path)
  end

  defp open_adapter(prefix) do
    {:ok, bundle_path} = TempDatabase.create(prefix: prefix)
    path = TempDatabase.sqlite_path(bundle_path)
    {:ok, adapter} = Adapter.create(path, %{})

    on_exit(fn ->
      Adapter.close(adapter)
      TempDatabase.cleanup(bundle_path)
    end)

    {:ok, adapter}
  end

  defp revision_history_id(adapter, document_id, revision_id) do
    alias ElixirDB.Storage.SQLite.{Documents, Revisions}

    {:ok, doc} = Documents.find(adapter.conn, document_id)
    {:ok, revision} = Revisions.find(adapter.conn, doc.doc_key, revision_id)
    revision.history_id
  end

  defp update_peer_safe(source_uuid, peer_uuid, safe_sequence) do
    {:ok, identity} = DatabaseCatalog.command(source_uuid, {:command, :identity, %{}})

    {:ok, record} =
      DatabaseCatalog.command(source_uuid, {:command, :get_local_record, "peer_ledger", peer_uuid})

    version = MapAccess.get(record, :version, 0)
    value = MapAccess.get(record, :value, %{})
    epoch = MapAccess.get(identity, :history_epoch)
    future = DateTime.utc_now() |> DateTime.add(60, :second) |> DateTime.to_iso8601()

    DatabaseCatalog.command(
      source_uuid,
      {:command, :put_peer_position_cas,
       %{
         expected_version: version,
         value:
           Map.merge(value, %{
             "peer_database_uuid" => peer_uuid,
             "source_database_uuid" => source_uuid,
             "source_history_epoch" => epoch,
             "safe_source_sequence" => safe_sequence,
             "last_seen_at" => DateTime.utc_now() |> DateTime.to_iso8601(),
             "lease_expires_at" => future,
             "status" => "active"
           })
       }}
    )
  end

  setup _tags do
    on_exit(fn ->
      case DatabaseCatalog.list() do
        {:ok, entries} ->
          for %{database_uuid: uuid} <- entries do
            _ = DatabaseCatalog.close(uuid)
            _ = DatabaseCatalog.unregister(uuid)
          end

        _ ->
          :ok
      end
    end)

    :ok
  end
end
