defmodule ElixirDB.Runtime.AdmissionClassificationTest do
  @moduledoc """
  Proves trusted operation origins acquire the intended service class and do not
  silently fall back to foreground.
  """
  use ExUnit.Case, async: false

  @moduletag :integration

  alias ElixirDB.Documents
  alias ElixirDB.Eventual
  alias ElixirDB.Query.Subscriptions
  alias ElixirDB.Replication.LocalEndpoint

  alias ElixirDB.Runtime.{
    AttachmentCoordinator,
    DatabaseCatalog,
    RetentionScheduler
  }

  alias ElixirDB.TestServer
  alias ElixirDB.TestSupport.AdmissionClassProbe

  @forbidden_trusted [:foreground]
  @forbidden_foreground [:subscription, :replication, :maintenance]
  @subscription_trusted_ops [
    :identity,
    :read_changes,
    :get_revisions_batch,
    :execute_subscription_snapshot
  ]

  setup tags do
    if tags[:no_module_setup] do
      :ok
    else
      rel = "admission-class-#{System.unique_integer([:positive])}.elixirdb"
      root = ElixirDB.Config.database_root()
      abs = Path.join(root, rel)
      ElixirDB.TempDatabase.cleanup(abs)

      assert {:ok, identity} = DatabaseCatalog.create(rel)
      uuid = identity.database_uuid
      assert {:ok, _} = DatabaseCatalog.open(uuid)

      on_exit(fn ->
        AdmissionClassProbe.uninstall()
        _ = DatabaseCatalog.close(uuid)
        _ = DatabaseCatalog.unregister(uuid)
        ElixirDB.TempDatabase.cleanup(abs)
      end)

      {:ok, uuid: uuid}
    end
  end

  defp await_attachment_gc_idle(uuid) do
    Eventual.eventually(
      fn ->
        case AttachmentCoordinator.status(uuid) do
          %{gc_barrier: false, gc_active: false, gc_queued: false, gc_scheduled: false} -> true
          _ -> false
        end
      end,
      timeout: 5_000,
      message: "attachment GC did not become idle"
    )
  end

  defp with_probe(fun) do
    AdmissionClassProbe.uninstall()
    ref = AdmissionClassProbe.install()

    try do
      fun.(ref)
    after
      AdmissionClassProbe.uninstall()
    end
  end

  describe "foreground origins" do
    setup do
      server = TestServer.start_supervised!()
      {:ok, server: server}
    end

    test "HTTP document put", %{uuid: uuid, server: server} do
      with_probe(fn probe ->
        AdmissionClassProbe.assert_only!(probe, :foreground, @forbidden_foreground, fn ->
          assert {:ok, %{status: 201}} =
                   Req.post(server.base_url <> "/v1/databases/#{uuid}/documents/put",
                     json: %{"id" => "doc", "body" => %{"kind" => "task"}}
                   )
        end)
      end)
    end

    test "HTTP ordinary query", %{uuid: uuid, server: server} do
      assert {:ok, %{status: 201}} =
               Req.post(server.base_url <> "/v1/databases/#{uuid}/documents/put",
                 json: %{"id" => "doc", "body" => %{"kind" => "task"}}
               )

      with_probe(fn probe ->
        AdmissionClassProbe.assert_only!(probe, :foreground, @forbidden_foreground, fn ->
          assert {:ok, %{status: 200}} =
                   Req.post(server.base_url <> "/v1/databases/#{uuid}/query",
                     json: %{"selector" => %{"/kind" => "task"}}
                   )
        end)
      end)
    end

    test "HTTP changes read", %{uuid: uuid, server: server} do
      with_probe(fn probe ->
        AdmissionClassProbe.assert_only!(probe, :foreground, @forbidden_foreground, fn ->
          assert {:ok, %{status: 200}} =
                   Req.post(server.base_url <> "/v1/databases/#{uuid}/changes",
                     json: %{"since" => 0, "limit" => 10, "wait_ms" => 0}
                   )
        end)
      end)
    end

    test "HTTP database info", %{uuid: uuid, server: server} do
      with_probe(fn probe ->
        AdmissionClassProbe.assert_op_only!(
          probe,
          :foreground,
          :identity,
          @forbidden_foreground,
          fn ->
            assert {:ok, %{status: 200}} =
                     Req.get(server.base_url <> "/v1/databases/#{uuid}")
          end
        )
      end)
    end

    test "explicit HTTP compact", %{uuid: uuid, server: server} do
      assert {:ok, %{status: 201}} =
               Req.post(server.base_url <> "/v1/databases/#{uuid}/documents/put",
                 json: %{"id" => "doc", "body" => %{"n" => 1}}
               )

      with_probe(fn probe ->
        assert {:ok, %{status: 200}} =
                 Req.post(server.base_url <> "/v1/databases/#{uuid}/compact", json: %{})

        grants = AdmissionClassProbe.drain(probe, 200)

        AdmissionClassProbe.assert_trusted_ops!(
          grants,
          [:compact_retention],
          :foreground,
          @forbidden_foreground
        )
      end)
    end
  end

  describe "subscription origins" do
    setup %{uuid: uuid} do
      assert {:ok, _} =
               DatabaseCatalog.command(
                 uuid,
                 {:command, :put, %{document_id: "a", body: %{"type" => "task"}}}
               )

      assert {:ok, _} =
               DatabaseCatalog.command(
                 uuid,
                 {:command, :put, %{document_id: "b", body: %{"type" => "note"}}}
               )

      :ok
    end

    test "initial live-query snapshot", %{uuid: uuid} do
      with_probe(fn probe ->
        AdmissionClassProbe.assert_only!(probe, :subscription, @forbidden_trusted, fn ->
          assert {:ok, pid} =
                   Subscriptions.open(uuid, %{"query" => %{"selector" => %{"/type" => "task"}}})

          assert {:ok, %{type: :snapshot}} = Subscriptions.next(pid, 5_000)
          assert {:ok, %{type: :caught_up}} = Subscriptions.next(pid, 5_000)
          Subscriptions.close(pid)
        end)
      end)
    end

    test "subscription hub changes read and revision batch", %{uuid: uuid} do
      assert {:ok, pid} =
               Subscriptions.open(uuid, %{"query" => %{"selector" => %{"/type" => "task"}}})

      assert {:ok, %{type: :snapshot}} = Subscriptions.next(pid, 5_000)
      assert {:ok, %{type: :caught_up}} = Subscriptions.next(pid, 5_000)

      assert {:ok, _} =
               DatabaseCatalog.command(
                 uuid,
                 {:command, :put, %{document_id: "c", body: %{"type" => "task"}}}
               )

      with_probe(fn probe ->
        AdmissionClassProbe.assert_only!(probe, :subscription, @forbidden_trusted, fn ->
          assert Eventual.eventually(
                   fn ->
                     case Subscriptions.next(pid, 5_000) do
                       {:ok, %{type: :upsert}} -> true
                       _ -> false
                     end
                   end,
                   timeout: 5_000
                 )
        end)
      end)

      Subscriptions.close(pid)
    end

    @tag :slow
    test "reset snapshot", %{uuid: uuid} do
      assert {:ok, _} =
               DatabaseCatalog.command(
                 uuid,
                 {:command, :update_config,
                  %{
                    "retention" => %{
                      "mode" => "stable_frontier",
                      "history_depth" => 0,
                      "peer_expiry_ms" => 86_400_000,
                      "schedule" => "disabled"
                    }
                  }}
               )

      assert {:ok, _} =
               DatabaseCatalog.command(
                 uuid,
                 {:command, :put, %{document_id: "keep", body: %{"title" => "keep"}}}
               )

      assert {:ok, pid} =
               Subscriptions.open(uuid, %{"query" => %{"selector" => %{"/title" => "keep"}}})

      for _ <- 1..2 do
        assert {:ok, _} = Subscriptions.next(pid, 5_000)
      end

      [{hub, _}] =
        Registry.lookup(ElixirDB.Runtime.DatabaseRegistry, {:query_subscription_hub, uuid})

      :sys.suspend(hub)

      assert {:ok, _} =
               DatabaseCatalog.command(
                 uuid,
                 {:command, :put, %{document_id: "late", body: %{"title" => "late"}}}
               )

      assert {:ok, _} =
               DatabaseCatalog.command(uuid, {:command, :compact_retention, %{}})

      with_probe(fn probe ->
        :sys.resume(hub)

        assert Eventual.eventually(
                 fn ->
                   case Subscriptions.next(pid, 5_000) do
                     {:ok, %{type: :reset}} -> true
                     _ -> false
                   end
                 end,
                 timeout: 5_000
               )

        grants = AdmissionClassProbe.drain(probe, 200)

        AdmissionClassProbe.assert_trusted_ops!(
          grants,
          @subscription_trusted_ops,
          :subscription,
          @forbidden_trusted
        )

        assert Enum.any?(grants, fn {class, op} ->
                 class == :subscription and op == :execute_subscription_snapshot
               end)
      end)

      Subscriptions.close(pid)
    end
  end

  describe "replication origins" do
    @retention_config %{
      "retention" => %{
        "mode" => "stable_frontier",
        "history_depth" => 0,
        "peer_expiry_ms" => 86_400_000,
        "schedule" => "disabled"
      }
    }

    setup %{uuid: uuid} do
      assert {:ok, endpoint} = LocalEndpoint.new(uuid)
      server = TestServer.start_supervised!()
      {:ok, endpoint: endpoint, server: server, uuid: uuid}
    end

    test "HTTP replication identity", %{server: server, uuid: uuid} do
      with_probe(fn probe ->
        AdmissionClassProbe.assert_op_only!(
          probe,
          :replication,
          :identity,
          @forbidden_trusted,
          fn ->
            assert {:ok, %{status: 200}} =
                     ElixirDB.TestReplicationWire.request(
                       :get,
                       server.base_url <> "/v1/databases/#{uuid}/replication/identity"
                     )
          end
        )
      end)
    end

    test "local endpoint identity", %{endpoint: endpoint} do
      with_probe(fn probe ->
        AdmissionClassProbe.assert_op_only!(
          probe,
          :replication,
          :identity,
          @forbidden_trusted,
          fn ->
            assert {:ok, identity} = LocalEndpoint.identity(endpoint)
            assert is_map(identity)
          end
        )
      end)
    end

    test "local endpoint changes", %{endpoint: endpoint} do
      with_probe(fn probe ->
        AdmissionClassProbe.assert_op_only!(
          probe,
          :replication,
          :read_changes,
          @forbidden_trusted,
          fn ->
            assert {:ok, _} =
                     LocalEndpoint.read_changes(endpoint, %{since: 0, limit: 10, wait_ms: 0})
          end
        )
      end)
    end

    test "diff/get/import revisions", %{endpoint: endpoint, uuid: uuid} do
      assert {:ok, %{revision: revision}} =
               Documents.put(uuid, %{id: "rep-doc", body: %{"n" => 1}})

      with_probe(fn probe ->
        AdmissionClassProbe.assert_only!(probe, :replication, @forbidden_trusted, fn ->
          assert {:ok, %{chains: chains}} =
                   LocalEndpoint.get_revision_chains(endpoint, %{
                     documents: [%{document_id: "rep-doc", leaf_revisions: [revision]}]
                   })

          assert is_list(chains)

          assert {:ok, diff} =
                   LocalEndpoint.diff_revisions(endpoint, %{
                     requests: [%{document_id: "rep-doc", revision_id: revision}]
                   })

          assert is_map(diff)

          assert {:ok, _imported} =
                   LocalEndpoint.import_revision_chains(endpoint, %{chains: chains})
        end)
      end)
    end

    test "checkpoint read and CAS", %{endpoint: endpoint} do
      replication_id = "rep_" <> Base.encode16(:crypto.strong_rand_bytes(4), case: :lower)
      {:ok, identity} = LocalEndpoint.identity(endpoint)

      checkpoint = %{
        "version" => 1,
        "replication_id" => replication_id,
        "checkpoint_version" => 1,
        "session_id" => ElixirDB.UUID.v4(),
        "source_sequence" => 0,
        "source_history_epoch" => identity.history_epoch,
        "source_compaction_epoch" => Map.get(identity, :compaction_epoch, 0),
        "safe_source_sequence" => 0,
        "installed_source_compaction_epoch" => 0,
        "history" => [],
        "expected_checkpoint_version" => 0
      }

      with_probe(fn probe ->
        AdmissionClassProbe.assert_only!(probe, :replication, @forbidden_trusted, fn ->
          assert {:ok, nil} = LocalEndpoint.get_checkpoint(endpoint, replication_id)
          assert {:ok, _} = LocalEndpoint.put_checkpoint(endpoint, replication_id, checkpoint)
        end)
      end)
    end

    test "durable commit confirmation", %{endpoint: endpoint} do
      with_probe(fn probe ->
        AdmissionClassProbe.assert_only!(probe, :replication, @forbidden_trusted, fn ->
          assert {:ok, %{"confirmed" => true}} =
                   LocalEndpoint.confirm_durable_commit(endpoint, %{imported: []})
        end)
      end)
    end

    test "peer position operations", %{endpoint: endpoint, uuid: uuid} do
      {:ok, identity} = LocalEndpoint.identity(endpoint)
      peer_uuid = "11111111-1111-4111-8111-111111111111"
      future = DateTime.utc_now() |> DateTime.add(60, :second) |> DateTime.to_iso8601()
      now = DateTime.utc_now() |> DateTime.to_iso8601()

      position = %{
        peer_database_uuid: peer_uuid,
        peer_history_epoch: "22222222-2222-4222-8222-222222222222",
        source_database_uuid: uuid,
        source_history_epoch: identity.history_epoch,
        safe_source_sequence: 0,
        installed_source_compaction_epoch: 0,
        last_seen_at: now,
        lease_expires_at: future,
        status: :active
      }

      with_probe(fn probe ->
        AdmissionClassProbe.assert_only!(probe, :replication, @forbidden_trusted, fn ->
          assert {:ok, peers} = LocalEndpoint.list_peer_positions(endpoint)
          assert is_list(peers)

          assert {:ok, _} =
                   LocalEndpoint.put_peer_position(endpoint, %{
                     expected_version: 0,
                     value: position
                   })
        end)
      end)
    end

    test "boundary read", %{endpoint: endpoint, uuid: uuid} do
      assert {:ok, _} =
               DatabaseCatalog.command(uuid, {:command, :update_config, @retention_config})

      with_probe(fn probe ->
        AdmissionClassProbe.assert_only!(probe, :replication, @forbidden_trusted, fn ->
          assert {:ok, _} = LocalEndpoint.read_boundary_pages(endpoint, %{})
        end)
      end)
    end

    test "boundary install", %{endpoint: endpoint, uuid: uuid} do
      assert {:ok, _} =
               DatabaseCatalog.command(uuid, {:command, :update_config, @retention_config})

      assert {:ok, _} =
               DatabaseCatalog.command(
                 uuid,
                 {:command, :put, %{document_id: "boundary-doc", body: %{"n" => 1}}}
               )

      assert {:ok, _} =
               DatabaseCatalog.command(uuid, {:command, :compact_retention, %{}})

      await_attachment_gc_idle(uuid)

      {:ok, page} = LocalEndpoint.read_boundary_pages(endpoint, %{})

      install_page =
        page
        |> Map.from_struct()
        |> Map.put(:source_database_uuid, uuid)
        |> Map.new(fn {k, v} -> {Atom.to_string(k), v} end)

      with_probe(fn probe ->
        AdmissionClassProbe.assert_only!(probe, :replication, @forbidden_trusted, fn ->
          assert {:ok, _} = LocalEndpoint.install_boundary_pages(endpoint, install_page)
        end)
      end)
    end

    test "replication attachment metadata around blob transfer", %{endpoint: endpoint} do
      bytes = "replication-classification-blob"
      digest = :crypto.hash(:sha256, bytes) |> Base.encode16(case: :lower)

      with_probe(fn probe ->
        AdmissionClassProbe.assert_only!(probe, :replication, @forbidden_trusted, fn ->
          assert {:ok, stream} = raw_blob_stream(digest, bytes)
          assert :ok = LocalEndpoint.put_blob_representation(endpoint, stream)
        end)
      end)
    end

    test "replication attachment diff and open metadata", %{endpoint: endpoint} do
      bytes = "replication-classification-open"
      digest = :crypto.hash(:sha256, bytes) |> Base.encode16(case: :lower)
      missing = :crypto.hash(:sha256, "missing") |> Base.encode16(case: :lower)

      assert {:ok, stream} = raw_blob_stream(digest, bytes)
      assert :ok = LocalEndpoint.put_blob_representation(endpoint, stream)

      with_probe(fn probe ->
        AdmissionClassProbe.assert_only!(probe, :replication, @forbidden_trusted, fn ->
          assert {:ok, [^missing]} = LocalEndpoint.diff_blobs(endpoint, [missing])
          assert {:ok, opened} = LocalEndpoint.open_blob_representation(endpoint, digest)
          assert opened.logical_digest == digest
        end)
      end)
    end
  end

  describe "maintenance origins" do
    @tag :no_module_setup
    test "RetentionScheduler schedule identity read" do
      rel = "admission-maint-schedule-#{System.unique_integer([:positive])}.elixirdb"
      root = ElixirDB.Config.database_root()
      abs = Path.join(root, rel)
      ElixirDB.TempDatabase.cleanup(abs)

      assert {:ok, identity} = DatabaseCatalog.create(rel)
      uuid = identity.database_uuid

      on_exit(fn ->
        AdmissionClassProbe.uninstall()
        _ = DatabaseCatalog.close(uuid)
        _ = DatabaseCatalog.unregister(uuid)
        ElixirDB.TempDatabase.cleanup(abs)
      end)

      assert {:ok, _} = DatabaseCatalog.open(uuid)

      assert {:ok, _} =
               DatabaseCatalog.command(
                 uuid,
                 {:command, :update_config,
                  %{
                    "retention" => %{
                      "mode" => "stable_frontier",
                      "history_depth" => 0,
                      "peer_expiry_ms" => 86_400_000,
                      "schedule" => 50
                    }
                  }}
               )

      assert [{scheduler_pid, _}] =
               Registry.lookup(
                 ElixirDB.Runtime.DatabaseRegistry,
                 {:retention_scheduler, uuid}
               )

      :sys.suspend(scheduler_pid)

      with_probe(fn probe ->
        assert :ok = RetentionScheduler.reschedule(uuid)
        :sys.resume(scheduler_pid)

        AdmissionClassProbe.assert_op_only!(
          probe,
          :maintenance,
          :identity,
          @forbidden_trusted,
          fn ->
            assert Eventual.eventually(
                     fn ->
                       case :sys.get_state(scheduler_pid) do
                         %{timer_ref: timer_ref} when is_reference(timer_ref) -> true
                         _ -> false
                       end
                     end,
                     timeout: 5_000,
                     message: "retention scheduler did not arm compaction timer"
                   )
          end
        )
      end)
    end

    test "RetentionScheduler automatic compact", %{uuid: uuid} do
      assert {:ok, _} =
               DatabaseCatalog.command(
                 uuid,
                 {:command, :update_config,
                  %{
                    "retention" => %{
                      "mode" => "stable_frontier",
                      "history_depth" => 0,
                      "peer_expiry_ms" => 86_400_000,
                      "schedule" => "disabled"
                    }
                  }}
               )

      for id <- ["a", "b"] do
        assert {:ok, _} =
                 DatabaseCatalog.command(
                   uuid,
                   {:command, :put, %{document_id: id, body: %{"n" => 1}}}
                 )
      end

      assert [{scheduler_pid, _}] =
               Registry.lookup(
                 ElixirDB.Runtime.DatabaseRegistry,
                 {:retention_scheduler, uuid}
               )

      :sys.suspend(scheduler_pid)

      with_probe(fn probe ->
        send(scheduler_pid, :scheduled_compact)
        :sys.resume(scheduler_pid)

        assert Eventual.eventually(
                 fn ->
                   case DatabaseCatalog.command(uuid, {:command, :retention_status, %{}}) do
                     {:ok, %{floor_sequence: floor}} when floor > 0 -> true
                     _ -> false
                   end
                 end,
                 timeout: 5_000,
                 message: "scheduled compaction did not advance the retention floor"
               )

        grants = AdmissionClassProbe.drain(probe, 200)

        AdmissionClassProbe.assert_trusted_ops!(
          grants,
          [:compact_retention],
          :maintenance,
          @forbidden_trusted
        )
      end)
    end
  end

  defp raw_blob_stream(digest, bytes) do
    ElixirDB.Replication.BlobRepresentationStream.new(%{
      logical_digest: digest,
      logical_length: byte_size(bytes),
      format_version: 1,
      encoding: :raw,
      payload_length: byte_size(bytes),
      payload_sha256: digest,
      body: [bytes]
    })
  end
end
