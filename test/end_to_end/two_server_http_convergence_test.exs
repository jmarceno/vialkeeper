defmodule VialKeeper.EndToEnd.TwoServerHttpConvergenceTest do
  @moduledoc """
  Two real Bandit servers, remote
  replication wire only, restart during continuous replication, resume through
  checkpoint reconciliation.

  Uses `VialKeeper.TestServer` + Req — not Plug.Test as fake servers.
  On outage: cancel/disable the continuous worker so resume must go through
  `JobManager.start` after Bandit restart (not an in-BEAM reconnect).
  """
  use ExUnit.Case, async: false

  @moduletag :integration

  alias VialKeeper.Attachments.FilesystemStore
  alias VialKeeper.EndToEnd.TwoServerHttpConvergenceTest.Barrier
  alias VialKeeper.MapAccess
  alias VialKeeper.Replication.Id
  alias VialKeeper.Replication.JobManager
  alias VialKeeper.Runtime.DatabaseCatalog
  alias VialKeeper.TestReplicationWire
  alias VialKeeper.TestServer

  @replay_header_names [
    "accept",
    "accept-encoding",
    "content-type",
    "content-encoding",
    "x-vialkeeper-uncompressed-length",
    "content-length"
  ]

  @tag :slow
  test "two Bandit servers converge over remote wire across mid-replication restart" do
    root = VialKeeper.Config.database_root()
    prefix = "e2e-two-http-#{System.unique_integer([:positive])}"
    a_path = prefix <> "-a.vialkeeper"
    b_path = prefix <> "-b.vialkeeper"

    for path <- [a_path, b_path] do
      VialKeeper.TempDatabase.cleanup(Path.join(root, path))
    end

    server_a = TestServer.start_supervised!()
    server_b = TestServer.start_supervised!()
    refute server_a.port == server_b.port
    port_a = server_a.port
    port_b = server_b.port

    a_uuid = create_database!(server_a, a_path)
    b_uuid = create_database!(server_b, b_path)

    on_exit(fn ->
      _ = maybe_disable_jobs(a_uuid)
      _ = DatabaseCatalog.close(a_uuid)
      _ = DatabaseCatalog.close(b_uuid)
      _ = DatabaseCatalog.unregister(a_uuid)
      _ = DatabaseCatalog.unregister(b_uuid)
      VialKeeper.TempDatabase.cleanup(Path.join(root, a_path))
      VialKeeper.TempDatabase.cleanup(Path.join(root, b_path))
    end)

    assert {:ok, %{"revision" => first_rev}} =
             put_document!(server_a, a_uuid, "seed", %{"n" => 1, "phase" => "pre-restart"})

    assert {:ok, %{"job_id" => job_id}} =
             put_replication_job!(server_a, a_uuid, %{
               "persist" => true,
               "mode" => "continuous",
               "direction" => "push",
               "enabled" => true,
               "wait_ms" => 100,
               "retry" => %{
                 "max_attempts" => 32,
                 "base_delay_ms" => 50,
                 "max_delay_ms" => 400,
                 "jitter_ms" => 10
               },
               "endpoint" => %{
                 "kind" => "remote",
                 "database_uuid" => b_uuid,
                 "base_url" => server_b.base_url
               }
             })

    wait_for_document!(server_b, b_uuid, "seed", first_rev, %{"n" => 1})

    VialKeeper.Eventual.eventually(
      fn ->
        case JobManager.get(a_uuid, job_id) do
          {:ok, %{state: state}} when state in [:waiting, :backoff, :handshake, :read_changes] ->
            true

          _ ->
            false
        end
      end,
      timeout: 15_000,
      message: "continuous job never entered an active replication state"
    )

    assert {:ok, %{"revision" => mid_rev}} =
             put_document!(server_a, a_uuid, "during", %{"n" => 2, "phase" => "before-stop"})

    wait_for_document!(server_b, b_uuid, "during", mid_rev, %{"n" => 2})

    # Architecture §21 step 9 — stop BOTH HTTP servers, then cancel/disable the
    # continuous worker so recovery cannot be a silent reconnect.
    assert :ok = TestServer.stop(server_a)
    assert :ok = TestServer.stop(server_b)

    assert {:error, _} = Req.get("http://127.0.0.1:#{port_a}/v1/databases", retry: false)
    assert {:error, _} = Req.get("http://127.0.0.1:#{port_b}/v1/databases", retry: false)

    assert {:ok, _} = JobManager.disable(a_uuid, job_id)

    VialKeeper.Eventual.eventually(
      fn ->
        case JobManager.get(a_uuid, job_id) do
          {:ok, %{state: :disabled}} -> true
          _ -> false
        end
      end,
      timeout: 10_000,
      message: "continuous job was not disabled after outage"
    )

    assert {:ok, %{revision: offline_rev}} =
             VialKeeper.Documents.put(a_uuid, %{
               id: "offline",
               body: %{"n" => 3, "phase" => "servers-down"}
             })

    # While the remote wire is down and the worker is disabled, B must not have
    # the offline write.
    assert {:error, %VialKeeper.Error{code: :document_not_found}} =
             VialKeeper.Documents.get(b_uuid, %{id: "offline"})

    server_a2 = TestServer.start_supervised!(port: port_a)
    server_b2 = TestServer.start_supervised!(port: port_b)
    assert server_a2.port == port_a
    assert server_b2.port == port_b

    # Explicit resume path: enable + start after Bandit restart.
    assert {:ok, _} = JobManager.enable(a_uuid, job_id)

    VialKeeper.Eventual.eventually(
      fn ->
        case JobManager.get(a_uuid, job_id) do
          {:ok, %{state: state}}
          when state in [
                 :waiting,
                 :backoff,
                 :handshake,
                 :read_changes,
                 :diff,
                 :transfer,
                 :import,
                 :checkpoint_target,
                 :checkpoint_source,
                 :idle
               ] ->
            true

          _ ->
            false
        end
      end,
      timeout: 15_000,
      message: "continuous job never restarted after JobManager.enable/start"
    )

    # Resume through checkpoint reconciliation — offline write arrives after restart.
    wait_for_document!(server_b2, b_uuid, "offline", offline_rev, %{"n" => 3})

    assert {:ok, %{"revision" => post_rev}} =
             put_document!(server_a2, a_uuid, "post-restart", %{
               "n" => 4,
               "phase" => "after-restart"
             })

    wait_for_document!(server_b2, b_uuid, "post-restart", post_rev, %{"n" => 4})

    assert {:ok, replication_id} =
             Id.calculate(a_uuid, b_uuid, "push", "continuous")

    final_sequence = source_sequence!(a_uuid)
    assert final_sequence >= 4

    VialKeeper.Eventual.eventually(
      fn ->
        case checkpoint_source_sequence(a_uuid, replication_id) do
          {:ok, seq} when seq == final_sequence ->
            case checkpoint_source_sequence(b_uuid, replication_id) do
              {:ok, ^final_sequence} -> true
              _ -> false
            end

          _ ->
            false
        end
      end,
      timeout: 15_000,
      message: "A and B checkpoints must equal final source sequence #{final_sequence}"
    )

    assert {:ok, ^final_sequence} = checkpoint_source_sequence(a_uuid, replication_id)
    assert {:ok, ^final_sequence} = checkpoint_source_sequence(b_uuid, replication_id)

    assert {:ok, %{"revision" => ^first_rev, "body" => %{"n" => 1}}} =
             get_document!(server_b2, b_uuid, "seed")

    assert {:ok, %{"revision" => ^mid_rev, "body" => %{"n" => 2}}} =
             get_document!(server_b2, b_uuid, "during")

    assert {:ok, %{"revision" => ^offline_rev, "body" => %{"n" => 3}}} =
             get_document!(server_b2, b_uuid, "offline")

    assert {:ok, %{"revision" => ^post_rev, "body" => %{"n" => 4}}} =
             get_document!(server_b2, b_uuid, "post-restart")

    assert_leaf_sets_equal!(a_uuid, b_uuid)

    assert {:ok, %{ok: true}} =
             DatabaseCatalog.command(b_uuid, {:command, :integrity_check, %{}})

    _ = JobManager.disable(a_uuid, job_id)
  end

  @tag :slow
  test "continuous push starts while target is offline and converges after recovery" do
    root = VialKeeper.Config.database_root()
    prefix = "e2e-offline-start-#{System.unique_integer([:positive])}"
    a_path = prefix <> "-a.vialkeeper"
    b_path = prefix <> "-b.vialkeeper"

    for path <- [a_path, b_path] do
      VialKeeper.TempDatabase.cleanup(Path.join(root, path))
    end

    server_a = TestServer.start_supervised!()
    holder_b = TestServer.start_supervised!()
    port_b = holder_b.port
    assert :ok = TestServer.stop(holder_b)

    a_uuid = create_database!(server_a, a_path)

    # Create B's database file while B is down by registering through A's process
    # space via local catalog — B will open the same registered path when its
    # server comes up. Use HTTP create on A only; B database is created locally
    # so the remote UUID is known before B listens.
    {:ok, b} = DatabaseCatalog.create(b_path)
    b_uuid = b.database_uuid

    on_exit(fn ->
      _ = maybe_disable_jobs(a_uuid)
      _ = DatabaseCatalog.close(a_uuid)
      _ = DatabaseCatalog.close(b_uuid)
      _ = DatabaseCatalog.unregister(a_uuid)
      _ = DatabaseCatalog.unregister(b_uuid)
      VialKeeper.TempDatabase.cleanup(Path.join(root, a_path))
      VialKeeper.TempDatabase.cleanup(Path.join(root, b_path))
    end)

    assert {:ok, %{"job_id" => job_id}} =
             put_replication_job!(server_a, a_uuid, %{
               "persist" => true,
               "mode" => "continuous",
               "direction" => "push",
               "enabled" => true,
               "wait_ms" => 100,
               "retry" => %{
                 "max_attempts" => 32,
                 "base_delay_ms" => 50,
                 "max_delay_ms" => 400,
                 "jitter_ms" => 10
               },
               "endpoint" => %{
                 "kind" => "remote",
                 "database_uuid" => b_uuid,
                 "base_url" => "http://127.0.0.1:#{port_b}"
               }
             })

    VialKeeper.Eventual.eventually(
      fn ->
        case JobManager.get(a_uuid, job_id) do
          {:ok, %{state: state}} when state in [:backoff, :handshake] -> true
          _ -> false
        end
      end,
      timeout: 15_000,
      message: "continuous job never entered backoff/handshake while target was down"
    )

    assert {:ok, %{"revision" => rev}} =
             put_document!(server_a, a_uuid, "offline-start", %{"n" => 1})

    server_b = TestServer.start_supervised!(port: port_b)
    assert server_b.port == port_b

    wait_for_document!(server_b, b_uuid, "offline-start", rev, %{"n" => 1})

    assert {:ok, replication_id} = Id.calculate(a_uuid, b_uuid, "push", "continuous")
    final_sequence = source_sequence!(a_uuid)

    VialKeeper.Eventual.eventually(
      fn ->
        case checkpoint_source_sequence(a_uuid, replication_id) do
          {:ok, seq} when seq == final_sequence ->
            case checkpoint_source_sequence(b_uuid, replication_id) do
              {:ok, ^final_sequence} -> true
              _ -> false
            end

          _ ->
            false
        end
      end,
      timeout: 15_000,
      message: "checkpoints did not converge after offline-start recovery"
    )

    assert {:ok, _} = JobManager.cancel(a_uuid, job_id)
  end

  @tag :slow
  test "continuous push resumes from checkpoint after mid-batch target drop" do
    root = VialKeeper.Config.database_root()
    prefix = "e2e-mid-batch-#{System.unique_integer([:positive])}"
    a_path = prefix <> "-a.vialkeeper"
    b_path = prefix <> "-b.vialkeeper"

    for path <- [a_path, b_path] do
      VialKeeper.TempDatabase.cleanup(Path.join(root, path))
    end

    server_a = TestServer.start_supervised!()
    server_b = TestServer.start_supervised!()
    port_b = server_b.port

    a_uuid = create_database!(server_a, a_path)
    b_uuid = create_database!(server_b, b_path)

    on_exit(fn ->
      _ = maybe_disable_jobs(a_uuid)
      _ = DatabaseCatalog.close(a_uuid)
      _ = DatabaseCatalog.close(b_uuid)
      _ = DatabaseCatalog.unregister(a_uuid)
      _ = DatabaseCatalog.unregister(b_uuid)
      VialKeeper.TempDatabase.cleanup(Path.join(root, a_path))
      VialKeeper.TempDatabase.cleanup(Path.join(root, b_path))
    end)

    assert {:ok, %{"job_id" => job_id}} =
             put_replication_job!(server_a, a_uuid, %{
               "persist" => true,
               "mode" => "continuous",
               "direction" => "push",
               "enabled" => true,
               "wait_ms" => 50,
               "retry" => %{
                 "max_attempts" => 32,
                 "base_delay_ms" => 50,
                 "max_delay_ms" => 400,
                 "jitter_ms" => 10
               },
               "endpoint" => %{
                 "kind" => "remote",
                 "database_uuid" => b_uuid,
                 "base_url" => server_b.base_url
               }
             })

    assert {:ok, %{"revision" => seed_rev}} =
             put_document!(server_a, a_uuid, "seed", %{"n" => 0})

    wait_for_document!(server_b, b_uuid, "seed", seed_rev, %{"n" => 0})

    assert {:ok, replication_id} = Id.calculate(a_uuid, b_uuid, "push", "continuous")

    VialKeeper.Eventual.eventually(
      fn ->
        case checkpoint_source_sequence(a_uuid, replication_id) do
          {:ok, seq} when is_integer(seq) and seq >= 1 ->
            case checkpoint_source_sequence(b_uuid, replication_id) do
              {:ok, ^seq} -> true
              _ -> false
            end

          _ ->
            false
        end
      end,
      timeout: 15_000,
      message: "baseline checkpoints never caught up"
    )

    assert {:ok, cp0} = checkpoint_source_sequence(a_uuid, replication_id)
    assert cp0 >= 1

    for i <- 1..50 do
      assert {:ok, _} =
               put_document!(server_a, a_uuid, "batch-#{i}", %{"n" => i})
    end

    assert :ok = TestServer.stop(server_b)
    assert {:error, _} = Req.get("http://127.0.0.1:#{port_b}/v1/databases", retry: false)

    assert {:ok, _} = JobManager.disable(a_uuid, job_id)

    VialKeeper.Eventual.eventually(
      fn ->
        case JobManager.get(a_uuid, job_id) do
          {:ok, %{state: :disabled}} -> true
          _ -> false
        end
      end,
      timeout: 10_000,
      message: "continuous job was not disabled after mid-batch drop"
    )

    for i <- 51..55 do
      assert {:ok, %{revision: _}} =
               VialKeeper.Documents.put(a_uuid, %{id: "batch-#{i}", body: %{"n" => i}})
    end

    server_b2 = TestServer.start_supervised!(port: port_b)
    assert server_b2.port == port_b
    assert {:ok, _} = JobManager.enable(a_uuid, job_id)

    for i <- 1..55 do
      VialKeeper.Eventual.eventually(
        fn ->
          case VialKeeper.Documents.get(b_uuid, %{id: "batch-#{i}"}) do
            {:ok, %{body: %{"n" => ^i}}} -> true
            _ -> false
          end
        end,
        timeout: 30_000,
        message: "document batch-#{i} did not arrive after mid-batch resume"
      )
    end

    final_sequence = source_sequence!(a_uuid)

    VialKeeper.Eventual.eventually(
      fn ->
        case checkpoint_source_sequence(a_uuid, replication_id) do
          {:ok, seq} when seq == final_sequence ->
            case checkpoint_source_sequence(b_uuid, replication_id) do
              {:ok, ^final_sequence} -> true
              _ -> false
            end

          _ ->
            false
        end
      end,
      timeout: 20_000,
      message: "checkpoints did not reach final source sequence after mid-batch resume"
    )

    assert {:ok, recovered} = checkpoint_source_sequence(a_uuid, replication_id)
    assert recovered > cp0
    assert recovered == final_sequence

    assert_leaf_sets_equal!(a_uuid, b_uuid)
    _ = JobManager.disable(a_uuid, job_id)
  end

  @tag :slow
  test "bounded remote pull overlaps chains and blobs with barriers" do
    root = VialKeeper.Config.database_root()
    prefix = "e2e-overlap-#{System.unique_integer([:positive])}"
    a_path = prefix <> "-a.vialkeeper"
    b_path = prefix <> "-b.vialkeeper"
    {:ok, barrier} = Barrier.start_link(self())
    # Equal logical lengths so the byte budget admits exactly two fixture blobs.
    blob_payload = String.duplicate("W", 64)

    for path <- [a_path, b_path] do
      VialKeeper.TempDatabase.cleanup(Path.join(root, path))
    end

    server_a =
      TestServer.start_supervised!(request_hook: &Barrier.hook(barrier, &1))

    server_b = TestServer.start_supervised!()
    a_uuid = create_database!(server_a, a_path)
    b_uuid = create_database!(server_b, b_path)

    on_exit(fn ->
      # Drain HTTP first so unwinding handlers cannot touch closed databases
      # or a stopped barrier agent.
      _ = maybe_disable_jobs(b_uuid)
      _ = TestServer.stop(server_a)
      _ = TestServer.stop(server_b)
      _ = DatabaseCatalog.close(a_uuid)
      _ = DatabaseCatalog.close(b_uuid)
      _ = DatabaseCatalog.unregister(a_uuid)
      _ = DatabaseCatalog.unregister(b_uuid)
      VialKeeper.TempDatabase.cleanup(Path.join(root, a_path))
      VialKeeper.TempDatabase.cleanup(Path.join(root, b_path))

      if Process.alive?(barrier) do
        Agent.stop(barrier)
      end
    end)

    shared_blob = upload_blob!(server_a, a_uuid, blob_payload <> "-one")
    blob_two = upload_blob!(server_a, a_uuid, blob_payload <> "-two")
    blob_three = upload_blob!(server_a, a_uuid, blob_payload <> "-six")
    blob_bytes = byte_size(blob_payload <> "-one")
    assert byte_size(blob_payload <> "-two") == blob_bytes
    assert byte_size(blob_payload <> "-six") == blob_bytes

    expected_blobs = %{
      "batch-1" => shared_blob,
      "batch-2" => blob_two,
      "batch-3" => shared_blob,
      "batch-4" => blob_three,
      "batch-5" => blob_two,
      "batch-6" => blob_three
    }

    assert {:ok, _} = put_document!(server_a, a_uuid, "baseline", %{"n" => 0})

    assert {:ok, %{"job_id" => job_id}} =
             put_replication_job!(server_b, b_uuid, %{
               "persist" => true,
               "mode" => "continuous",
               "direction" => "pull",
               "enabled" => false,
               "wait_ms" => 50,
               "max_concurrent_chain_fetches" => 3,
               # Count limit is higher than two so only the byte budget can admit
               # exactly two equal-sized fixture blobs at once.
               "max_concurrent_blob_transfers" => 4,
               "max_transfer_bytes_in_flight" => 2 * blob_bytes,
               "batch" => %{"documents" => 6, "bytes" => 16_000_000},
               "batch_documents" => 1,
               "retry" => %{
                 "max_attempts" => 8,
                 "base_delay_ms" => 20,
                 "max_delay_ms" => 100,
                 "jitter_ms" => 1
               },
               "endpoint" => %{
                 "kind" => "remote",
                 "database_uuid" => a_uuid,
                 "base_url" => server_a.base_url
               }
             })

    assert {:ok, initial_replication_id} = Id.calculate(a_uuid, b_uuid, "pull", "continuous")
    assert {:ok, _} = JobManager.enable(b_uuid, job_id)
    await_checkpoint(b_uuid, initial_replication_id, source_sequence!(a_uuid))
    assert {:ok, _} = JobManager.disable(b_uuid, job_id)

    batch_revisions =
      for {id, n, blob} <- [
            {"batch-1", 1, shared_blob},
            {"batch-2", 2, blob_two},
            {"batch-3", 3, shared_blob},
            {"batch-4", 4, blob_three},
            {"batch-5", 5, blob_two},
            {"batch-6", 6, blob_three}
          ],
          into: %{} do
        assert {:ok, %{"revision" => revision}} =
                 put_document!(
                   server_a,
                   a_uuid,
                   id,
                   %{"n" => n, "batch" => true},
                   %{
                     "attachment.bin" => %{
                       "blob" => blob,
                       "content_type" => "application/octet-stream"
                     }
                   }
                 )

        {id, revision}
      end

    assert {:ok, replication_id} = Id.calculate(a_uuid, b_uuid, "pull", "continuous")
    checkpoint_before_batch = checkpoint_source_sequence(b_uuid, replication_id)
    Barrier.reset(barrier)
    Barrier.activate(barrier)
    assert {:ok, _} = JobManager.enable(b_uuid, job_id)

    Barrier.await(barrier, :chains, 3)
    assert Barrier.count(barrier, :chains) == 3
    assert Barrier.count(barrier, :max_chains) == 3
    Barrier.release(barrier, :chains, 1)
    Barrier.await(barrier, :blobs, 1)

    # The first completed chain exposes its attachment before the other two
    # chain requests finish. This is an interaction assertion, not a timing
    # assertion: two chain request processes are still waiting here.
    assert Barrier.count(barrier, :released_chains) == 1
    assert match?([_, _ | _], Barrier.pids(barrier, :chains))
    assert Barrier.count(barrier, :blobs) == 1
    assert_document_missing!(server_b, b_uuid, "batch-6")

    Barrier.deactivate_chains(barrier)
    Barrier.release(barrier, :chains, 10)
    Barrier.await(barrier, :blobs, 2)
    assert Barrier.count(barrier, :blobs) == 2
    assert Barrier.count(barrier, :max_blobs) == 2
    assert Barrier.count(barrier, :blobs_total) == 2
    assert_document_missing!(server_b, b_uuid, "batch-1")
    assert_document_missing!(server_b, b_uuid, "batch-6")

    # Byte reservation (not a sleep / not the count limit): concurrency allows 4,
    # but the budget only fits two logical blob lengths. Releasing one reservation
    # lets the third distinct missing digest open while active stays at two.
    Barrier.release(barrier, :blobs, 1)
    Barrier.await_total(barrier, :blobs, 3)
    assert Barrier.count(barrier, :blobs) == 2
    assert Barrier.count(barrier, :max_blobs) == 2
    assert_document_missing!(server_b, b_uuid, "batch-6")

    assert {:ok, _} = JobManager.disable(b_uuid, job_id)
    await_job_state(b_uuid, job_id, :disabled)
    assert checkpoint_source_sequence(b_uuid, replication_id) == checkpoint_before_batch
    assert_document_missing!(server_b, b_uuid, "batch-6")

    Barrier.release(barrier, :chains, 10)
    Barrier.release(barrier, :blobs, 10)
    Barrier.await_idle(barrier)
    Barrier.reset(barrier)
    Barrier.activate(barrier)
    Barrier.deactivate_chains(barrier)
    Barrier.deactivate_blobs(barrier)
    assert {:ok, _} = JobManager.enable(b_uuid, job_id)

    wait_for_document!(server_b, b_uuid, "batch-6", batch_revisions["batch-6"], %{
      "n" => 6,
      "batch" => true
    })

    Barrier.await(barrier, :checkpoint_source, 1)
    assert {:ok, target_checkpoint} = checkpoint_source_sequence(b_uuid, replication_id)
    assert {:ok, source_checkpoint_before_put} = checkpoint_source_sequence(a_uuid, replication_id)
    assert {:ok, previous_target_checkpoint} = checkpoint_before_batch
    assert target_checkpoint > previous_target_checkpoint
    assert source_checkpoint_before_put < target_checkpoint
    Barrier.release(barrier, :checkpoint_source, 1)
    Barrier.deactivate(barrier)
    Barrier.await_idle(barrier)

    await_checkpoint(b_uuid, replication_id, source_sequence!(a_uuid))
    await_checkpoint(a_uuid, replication_id, source_sequence!(a_uuid))

    assert {:ok, %{ok: true}} =
             DatabaseCatalog.command(b_uuid, {:command, :integrity_check, %{}})

    source_sequence = source_sequence!(a_uuid)
    assert checkpoint_source_sequence(b_uuid, replication_id) == {:ok, source_sequence}
    assert checkpoint_source_sequence(a_uuid, replication_id) == {:ok, source_sequence}

    for id <- ["batch-1", "batch-2", "batch-3", "batch-4", "batch-5", "batch-6"] do
      assert {:ok, %{"body" => %{"batch" => true}, "attachments" => attachments}} =
               get_document!(server_b, b_uuid, id)

      assert %{"attachment.bin" => %{"blob" => blob}} = attachments
      assert blob == expected_blobs[id]
    end

    assert_leaf_sets_equal!(a_uuid, b_uuid)
    assert_batch_documents_equal!(server_a, server_b, a_uuid, b_uuid)

    # Retryable chain failure: observed 503, then convergence without duplicate
    # logical revisions.
    Barrier.fail_next_chain(barrier)

    assert {:ok, %{"revision" => retry_chain_revision}} =
             put_document!(server_a, a_uuid, "retry-chain", %{"n" => 7})

    wait_for_document!(server_b, b_uuid, "retry-chain", retry_chain_revision, %{"n" => 7})
    await_job_state(b_uuid, job_id, :idle)
    assert Barrier.count(barrier, :chain_failures) == 1

    assert revisions_for_document(
             changes_results!(a_uuid),
             "retry-chain"
           ) == [retry_chain_revision]

    assert revisions_for_document(
             changes_results!(b_uuid),
             "retry-chain"
           ) == [retry_chain_revision]

    # Retryable blob failure uses a brand-new digest so open_blob_representation is required.
    retry_blob = upload_blob!(server_a, a_uuid, blob_payload <> "-try")
    Barrier.fail_next_blob(barrier)

    assert {:ok, %{"revision" => retry_blob_revision}} =
             put_document!(
               server_a,
               a_uuid,
               "retry-blob",
               %{"n" => 8},
               %{
                 "attachment.bin" => %{
                   "blob" => retry_blob,
                   "content_type" => "application/octet-stream"
                 }
               }
             )

    wait_for_document!(server_b, b_uuid, "retry-blob", retry_blob_revision, %{"n" => 8})
    await_job_state(b_uuid, job_id, :idle)
    assert Barrier.count(barrier, :blob_failures) == 1

    assert revisions_for_document(
             changes_results!(a_uuid),
             "retry-blob"
           ) == [retry_blob_revision]

    assert revisions_for_document(
             changes_results!(b_uuid),
             "retry-blob"
           ) == [retry_blob_revision]

    # Already-durable blobs are not reopened when a later revision reuses them.
    blob_opens_before_reuse = Barrier.count(barrier, :blob_opens)

    assert {:ok, %{"revision" => reuse_revision}} =
             put_document!(
               server_a,
               a_uuid,
               "reuse-doc",
               %{"n" => 9},
               %{
                 "attachment.bin" => %{
                   "blob" => shared_blob,
                   "content_type" => "application/octet-stream"
                 }
               }
             )

    wait_for_document!(server_b, b_uuid, "reuse-doc", reuse_revision, %{"n" => 9})
    await_job_state(b_uuid, job_id, :idle)
    assert Barrier.count(barrier, :blob_opens) == blob_opens_before_reuse

    assert {:ok, %{ok: true}} =
             DatabaseCatalog.command(a_uuid, {:command, :integrity_check, %{}})

    assert {:ok, %{ok: true}} =
             DatabaseCatalog.command(b_uuid, {:command, :integrity_check, %{}})
  end

  @tag :slow
  test "replaying an imported revision batch is idempotent and preserves convergence" do
    {:ok, captures} = Agent.start_link(fn -> [] end)

    capture_import = fn request ->
      if request.method == "POST" and
           String.ends_with?(request.path, "/replication/revisions/put") do
        Agent.update(captures, &[request | &1])
      end
    end

    {server_a, server_b, a_uuid, b_uuid} =
      start_server_pair!("e2e-import-replay", request_body_hook: capture_import)

    revisions =
      for n <- 1..3, into: %{} do
        id = "replay-#{n}"
        assert {:ok, %{"revision" => revision}} = put_document!(server_a, a_uuid, id, %{"n" => n})
        {id, revision}
      end

    assert {:ok, %{"job_id" => job_id}} =
             put_replication_job!(server_a, a_uuid, %{
               "persist" => true,
               "mode" => "one_shot",
               "direction" => "push",
               "enabled" => true,
               "endpoint" => %{
                 "kind" => "remote",
                 "database_uuid" => b_uuid,
                 "base_url" => server_b.base_url
               }
             })

    start_job!(a_uuid, job_id)
    await_job_state(a_uuid, job_id, :completed)

    for {id, revision} <- revisions do
      wait_for_document!(server_b, b_uuid, id, revision, %{
        "n" => id |> String.replace_prefix("replay-", "") |> String.to_integer()
      })
    end

    assert {:ok, replication_id} = Id.calculate(a_uuid, b_uuid, "push", "one_shot")
    source_sequence = source_sequence!(a_uuid)
    await_checkpoint(a_uuid, replication_id, source_sequence)
    await_checkpoint(b_uuid, replication_id, source_sequence)
    assert_leaf_sets_equal!(a_uuid, b_uuid)

    assert [captured | _] = captures |> Agent.get(&Enum.reverse/1)
    replay_headers = replay_headers!(captured.headers, captured.body)
    observable_before = replay_observable_state!(server_b, a_uuid, b_uuid, replication_id)
    assert map_size(observable_before.target.documents) == 3

    assert {:ok, replay_response} =
             Req.post(server_b.base_url <> captured.path,
               body: captured.body,
               headers: replay_headers,
               decode_body: false,
               compressed: false,
               retry: false
             )

    replay_body =
      TestReplicationWire.decode_response(replay_response.headers, replay_response.body)

    assert replay_response.status == 200

    assert %{
             "data" => %{
               "documents_changed" => 0,
               "revisions_inserted" => 0
             }
           } = replay_body

    assert replay_observable_state!(server_b, a_uuid, b_uuid, replication_id) ==
             observable_before

    assert_leaf_sets_equal!(a_uuid, b_uuid)

    assert {:ok, %{"revision" => later_revision}} =
             put_document!(server_a, a_uuid, "after-replay", %{"n" => 4})

    assert {:ok, %{"job_id" => later_job_id}} =
             put_replication_job!(server_a, a_uuid, %{
               "persist" => true,
               "mode" => "one_shot",
               "direction" => "push",
               "enabled" => true,
               "endpoint" => %{
                 "kind" => "remote",
                 "database_uuid" => b_uuid,
                 "base_url" => server_b.base_url
               }
             })

    start_job!(a_uuid, later_job_id)
    await_job_state(a_uuid, later_job_id, :completed)
    wait_for_document!(server_b, b_uuid, "after-replay", later_revision, %{"n" => 4})
    assert b_uuid |> public_documents!(server_b) |> map_size() == 4
    assert_leaf_sets_equal!(a_uuid, b_uuid)
  end

  @tag :slow
  test "one-shot remote pull preserves raw and zstd representations byte for byte" do
    {server_a, server_b, a_uuid, b_uuid} = start_server_pair!("e2e-repr")

    raw_payload = :crypto.strong_rand_bytes(65_536)
    zstd_payload = String.duplicate("byte-stable-representation-", 8_192)

    raw_digest = upload_blob!(server_a, a_uuid, raw_payload)
    zstd_digest = upload_blob!(server_a, a_uuid, zstd_payload)
    assert blob_encoding!(a_uuid, raw_digest) == :raw
    assert blob_encoding!(a_uuid, zstd_digest) == :zstd

    # The target already holds a valid representation of the raw payload.
    assert upload_blob!(server_b, b_uuid, raw_payload) == raw_digest
    existing_bytes = blob_file_bytes!(b_uuid, raw_digest)
    existing_file_id = blob_file_id!(b_uuid, raw_digest)

    assert {:ok, %{"revision" => raw_revision}} =
             put_document!(server_a, a_uuid, "raw-doc", %{"kind" => "raw"}, %{
               "payload.bin" => %{
                 "blob" => raw_digest,
                 "content_type" => "application/octet-stream"
               }
             })

    assert {:ok, %{"revision" => zstd_revision}} =
             put_document!(server_a, a_uuid, "zstd-doc", %{"kind" => "zstd"}, %{
               "payload.bin" => %{
                 "blob" => zstd_digest,
                 "content_type" => "application/octet-stream"
               }
             })

    assert {:ok, %{"job_id" => job_id}} =
             put_replication_job!(server_b, b_uuid, %{
               "persist" => true,
               "mode" => "one_shot",
               "direction" => "pull",
               "enabled" => true,
               "endpoint" => %{
                 "kind" => "remote",
                 "database_uuid" => a_uuid,
                 "base_url" => server_a.base_url
               }
             })

    start_job!(b_uuid, job_id)
    await_job_state(b_uuid, job_id, :completed)

    wait_for_document!(server_b, b_uuid, "raw-doc", raw_revision, %{"kind" => "raw"})
    wait_for_document!(server_b, b_uuid, "zstd-doc", zstd_revision, %{"kind" => "zstd"})

    # Complete on-disk representation files (payload plus trailer) match.
    for digest <- [raw_digest, zstd_digest] do
      assert blob_file_bytes!(b_uuid, digest) == blob_file_bytes!(a_uuid, digest)
    end

    # The pre-existing valid target representation was kept, not replaced:
    # same inode, so the file was never reinstalled via rename.
    assert blob_file_bytes!(b_uuid, raw_digest) == existing_bytes
    assert blob_file_id!(b_uuid, raw_digest) == existing_file_id

    # Public download still returns the original logical bytes.
    assert download_attachment!(server_b, b_uuid, "raw-doc", "payload.bin") == raw_payload
    assert download_attachment!(server_b, b_uuid, "zstd-doc", "payload.bin") == zstd_payload

    assert {:ok, %{ok: true}} =
             DatabaseCatalog.command(b_uuid, {:command, :integrity_check, %{}})
  end

  @tag :slow
  test "source representation corruption fails pull without installing on the target" do
    {server_a, server_b, a_uuid, b_uuid} = start_server_pair!("e2e-corrupt")

    payload = String.duplicate("corrupted-representation-", 4_096)
    digest = upload_blob!(server_a, a_uuid, payload)

    assert {:ok, %{"revision" => revision}} =
             put_document!(server_a, a_uuid, "poisoned", %{"n" => 1}, %{
               "payload.bin" => %{
                 "blob" => digest,
                 "content_type" => "application/octet-stream"
               }
             })

    source_blob_path = blob_file_path!(a_uuid, digest)
    original = File.read!(source_blob_path)
    <<first, rest::binary>> = original
    File.write!(source_blob_path, <<Bitwise.bxor(first, 1), rest::binary>>)

    assert {:ok, replication_id} = Id.calculate(a_uuid, b_uuid, "pull", "one_shot")
    checkpoint_before = checkpoint_source_sequence(b_uuid, replication_id)

    assert {:ok, %{"job_id" => job_id}} =
             put_replication_job!(server_b, b_uuid, %{
               "persist" => true,
               "mode" => "one_shot",
               "direction" => "pull",
               "enabled" => true,
               "retry" => %{
                 "max_attempts" => 2,
                 "base_delay_ms" => 10,
                 "max_delay_ms" => 20,
                 "jitter_ms" => 1
               },
               "endpoint" => %{
                 "kind" => "remote",
                 "database_uuid" => a_uuid,
                 "base_url" => server_a.base_url
               }
             })

    start_job!(b_uuid, job_id)
    await_job_state(b_uuid, job_id, :failed)

    # No partial install: document, blob, checkpoint, and tmp dir untouched.
    assert_document_missing!(server_b, b_uuid, "poisoned")

    assert {:error, %VialKeeper.Error{code: :attachment_blob_not_found}} =
             VialKeeper.Attachments.open_blob_representation(b_uuid, digest)

    assert checkpoint_source_sequence(b_uuid, replication_id) == checkpoint_before
    {:ok, target_bundle} = DatabaseCatalog.bundle_root(b_uuid)
    assert Path.wildcard(Path.join([target_bundle, "tmp", "*"])) == []

    # No leaked guard on either side: the target stays writable and a clean
    # retry converges once the source representation is restored.
    assert {:ok, _} = put_document!(server_b, b_uuid, "target-writable", %{"ok" => true})
    File.write!(source_blob_path, original)

    assert {:ok, %{"job_id" => retry_job_id}} =
             put_replication_job!(server_b, b_uuid, %{
               "persist" => true,
               "mode" => "one_shot",
               "direction" => "pull",
               "enabled" => true,
               "endpoint" => %{
                 "kind" => "remote",
                 "database_uuid" => a_uuid,
                 "base_url" => server_a.base_url
               }
             })

    start_job!(b_uuid, retry_job_id)
    await_job_state(b_uuid, retry_job_id, :completed)
    wait_for_document!(server_b, b_uuid, "poisoned", revision, %{"n" => 1})
    assert blob_file_bytes!(b_uuid, digest) == original
  end

  defp start_server_pair!(prefix_base, server_b_opts \\ []) do
    root = VialKeeper.Config.database_root()
    prefix = "#{prefix_base}-#{System.unique_integer([:positive])}"
    a_path = prefix <> "-a.vialkeeper"
    b_path = prefix <> "-b.vialkeeper"

    for path <- [a_path, b_path] do
      VialKeeper.TempDatabase.cleanup(Path.join(root, path))
    end

    server_a = TestServer.start_supervised!()
    server_b = TestServer.start_supervised!(server_b_opts)
    a_uuid = create_database!(server_a, a_path)
    b_uuid = create_database!(server_b, b_path)

    on_exit(fn ->
      _ = maybe_disable_jobs(b_uuid)
      _ = TestServer.stop(server_a)
      _ = TestServer.stop(server_b)
      _ = DatabaseCatalog.close(a_uuid)
      _ = DatabaseCatalog.close(b_uuid)
      _ = DatabaseCatalog.unregister(a_uuid)
      _ = DatabaseCatalog.unregister(b_uuid)
      VialKeeper.TempDatabase.cleanup(Path.join(root, a_path))
      VialKeeper.TempDatabase.cleanup(Path.join(root, b_path))
    end)

    {server_a, server_b, a_uuid, b_uuid}
  end

  defp start_job!(uuid, job_id) do
    case JobManager.start(uuid, job_id) do
      {:ok, _} -> :ok
      {:error, %VialKeeper.Error{code: :replication_already_running}} -> :ok
      other -> flunk("job #{job_id} did not start: #{inspect(other)}")
    end
  end

  defp blob_file_path!(uuid, digest) do
    {:ok, bundle} = DatabaseCatalog.bundle_root(uuid)
    path = Path.join([bundle, "blobs", String.slice(digest, 0, 2), digest <> ".blob"])
    assert File.regular?(path), "expected representation file at #{path}"
    path
  end

  defp blob_file_bytes!(uuid, digest), do: File.read!(blob_file_path!(uuid, digest))

  defp blob_file_id!(uuid, digest) do
    path = blob_file_path!(uuid, digest)
    {:ok, info} = :file.read_file_info(String.to_charlist(path))

    # :file_info record layout: {:file_info, size, type, access, atime, mtime,
    # ctime, mode, links, major_device, minor_device, inode, uid, gid}
    {elem(info, 9), elem(info, 11)}
  end

  defp blob_encoding!(uuid, digest) do
    {:ok, bundle} = DatabaseCatalog.bundle_root(uuid)
    assert {:ok, stat} = FilesystemStore.stat(bundle, digest)
    stat.encoding
  end

  defp download_attachment!(server, uuid, document_id, name) do
    assert {:ok, %{status: 200, body: body}} =
             Req.post(server.base_url <> "/v1/databases/#{uuid}/attachments/get",
               json: %{"id" => document_id, "name" => name},
               decode_body: false,
               compressed: false
             )

    body
  end

  defp create_database!(server, path) do
    assert {:ok, %{status: 201, body: body}} =
             Req.post(server.base_url <> "/v1/databases", json: %{"path" => path})

    assert %{"data" => %{"database_uuid" => uuid}} = body
    uuid
  end

  defp put_document!(server, uuid, id, doc_body, attachments \\ %{}) do
    assert {:ok, %{status: status, body: body}} =
             Req.post(server.base_url <> "/v1/databases/#{uuid}/documents/put",
               json: %{"id" => id, "body" => doc_body, "attachments" => attachments}
             )

    assert status in [200, 201]
    assert %{"data" => data} = body
    {:ok, data}
  end

  defp upload_blob!(server, uuid, payload) do
    assert {:ok, %{status: 201, body: %{"data" => %{"blob" => digest}}}} =
             Req.post(server.base_url <> "/v1/databases/#{uuid}/attachments/upload",
               body: payload,
               headers: [{"content-type", "application/octet-stream"}]
             )

    digest
  end

  defp get_document!(server, uuid, id) do
    case Req.post(server.base_url <> "/v1/databases/#{uuid}/documents/get",
           json: %{"id" => id}
         ) do
      {:ok, %{status: 200, body: %{"data" => data}}} -> {:ok, data}
      other -> other
    end
  end

  defp assert_document_missing!(server, uuid, id) do
    assert {:ok, %{status: 404}} = get_document!(server, uuid, id)
  end

  defp changes_results!(uuid) do
    assert {:ok, %{results: results}} = VialKeeper.Changes.read(uuid, %{since: 0, limit: 200})
    results
  end

  defp put_replication_job!(server, uuid, definition) do
    assert {:ok, %{status: 201, body: body}} =
             Req.post(server.base_url <> "/v1/databases/#{uuid}/replications", json: definition)

    assert %{"data" => data} = body
    {:ok, data}
  end

  defp wait_for_document!(server, uuid, id, revision, body_subset) do
    VialKeeper.Eventual.eventually(
      fn -> document_matches?(server, uuid, id, revision, body_subset) end,
      timeout: 20_000,
      message: "document #{id} revision #{revision} did not appear on remote server"
    )
  end

  defp document_matches?(server, uuid, id, revision, body_subset) do
    case get_document!(server, uuid, id) do
      {:ok, %{"revision" => ^revision, "body" => body}} -> body_subset_matches?(body, body_subset)
      _ -> false
    end
  end

  defp body_subset_matches?(body, body_subset),
    do: Enum.all?(body_subset, fn {key, value} -> body[key] == value end)

  defp source_sequence!(uuid) do
    assert {:ok, identity} = DatabaseCatalog.command(uuid, {:command, :identity, %{}})
    MapAccess.get(identity, :current_sequence)
  end

  defp checkpoint_source_sequence(uuid, replication_id) do
    case DatabaseCatalog.command(
           uuid,
           {:command, :get_local_record, "checkpoints", replication_id}
         ) do
      {:ok, %{value: value}} when is_map(value) ->
        {:ok, MapAccess.get(value, :source_sequence)}

      {:ok, %{"value" => value}} when is_map(value) ->
        {:ok, MapAccess.get(value, :source_sequence)}

      other ->
        other
    end
  end

  defp replay_observable_state!(server, source_uuid, target_uuid, replication_id) do
    target_changes = changes_results!(target_uuid)

    %{
      source_checkpoint: checkpoint_record!(source_uuid, replication_id),
      target: %{
        documents: public_documents!(target_uuid, server, target_changes),
        current_sequence: source_sequence!(target_uuid),
        leaves: leaf_map(target_changes),
        checkpoint: checkpoint_record!(target_uuid, replication_id)
      }
    }
  end

  defp checkpoint_record!(uuid, replication_id) do
    assert {:ok, %{version: record_version, value: value} = record} =
             DatabaseCatalog.command(
               uuid,
               {:command, :get_local_record, "checkpoints", replication_id}
             )

    assert is_integer(record_version) and record_version > 0
    assert MapAccess.get(value, :replication_id) == replication_id
    assert is_integer(MapAccess.get(value, :checkpoint_version))
    assert is_integer(MapAccess.get(value, :source_sequence))
    assert is_list(MapAccess.get(value, :history))
    assert is_binary(MapAccess.get(value, :source_history_epoch))
    assert is_integer(MapAccess.get(value, :source_compaction_epoch))
    assert is_integer(MapAccess.get(value, :safe_source_sequence))
    assert is_integer(MapAccess.get(value, :installed_source_compaction_epoch))
    record
  end

  defp public_documents!(uuid, server, changes \\ nil) do
    changes = changes || changes_results!(uuid)

    changes
    |> Enum.map(&MapAccess.get(&1, :document_id))
    |> Enum.sort()
    |> Map.new(fn id ->
      assert {:ok, %{status: 200, body: %{"data" => document}}} =
               Req.post(server.base_url <> "/v1/databases/#{uuid}/documents/get",
                 json: %{"id" => id, "include_conflicts" => true}
               )

      assert %{
               "id" => ^id,
               "revision" => revision,
               "deleted" => deleted,
               "body" => body,
               "sequence" => sequence,
               "attachments" => attachments,
               "conflicts" => conflicts
             } = document

      assert is_binary(revision)
      assert is_boolean(deleted)
      assert is_nil(body) or is_map(body)
      assert is_integer(sequence)
      assert is_map(attachments)
      assert is_list(conflicts)
      {id, document}
    end)
  end

  defp replay_headers!(headers, body) do
    replay_headers =
      Enum.map(@replay_header_names, fn name ->
        assert [value] =
                 for(
                   {key, value} <- headers,
                   String.downcase(to_string(key)) == name,
                   do: value
                 )

        {name, value}
      end)

    assert {"content-encoding", "zstd"} in replay_headers
    assert {"accept-encoding", "zstd"} in replay_headers

    assert {"content-length", Integer.to_string(byte_size(body))} in replay_headers
    assert byte_size(body) > 0
    replay_headers
  end

  defp assert_leaf_sets_equal!(source_uuid, target_uuid) do
    assert {:ok, %{results: source_changes}} =
             VialKeeper.Changes.read(source_uuid, %{since: 0, limit: 200})

    assert {:ok, %{results: target_changes}} =
             VialKeeper.Changes.read(target_uuid, %{since: 0, limit: 200})

    source_leaves = leaf_map(source_changes)
    target_leaves = leaf_map(target_changes)

    assert target_leaves == source_leaves,
           "leaf sets differ: source=#{inspect(source_leaves)} target=#{inspect(target_leaves)}"
  end

  defp assert_batch_documents_equal!(source_server, target_server, source_uuid, target_uuid) do
    for id <- ["batch-1", "batch-2", "batch-3", "batch-4", "batch-5", "batch-6"] do
      assert {:ok, source_document} = get_document!(source_server, source_uuid, id)
      assert {:ok, target_document} = get_document!(target_server, target_uuid, id)
      assert target_document == source_document
    end
  end

  defp revisions_for_document(changes, document_id) do
    changes
    |> Enum.filter(&(MapAccess.get(&1, :document_id) == document_id))
    |> Enum.flat_map(fn change ->
      MapAccess.get(change, :leaf_revisions, [])
      |> Enum.map(&MapAccess.get(&1, :revision))
    end)
  end

  defp leaf_map(changes) do
    Map.new(changes, fn change ->
      leaves =
        MapAccess.get(change, :leaf_revisions, [])
        |> Enum.map(&MapAccess.get(&1, :revision))
        |> Enum.reject(&is_nil/1)
        |> MapSet.new()

      {MapAccess.get(change, :document_id), leaves}
    end)
  end

  defp maybe_disable_jobs(uuid) do
    case JobManager.list(uuid) do
      {:ok, jobs} ->
        Enum.each(jobs, fn job ->
          _ = JobManager.disable(uuid, job.job_id)
        end)

      _ ->
        :ok
    end
  end

  defp await_job_state(uuid, job_id, expected) do
    VialKeeper.Eventual.eventually(
      fn ->
        case JobManager.get(uuid, job_id) do
          {:ok, %{state: state}} when expected == :idle and state in [:idle, :waiting] -> true
          {:ok, %{state: ^expected}} -> true
          _ -> false
        end
      end,
      timeout: 20_000,
      message: "job #{job_id} did not reach #{expected}"
    )
  end

  defp await_checkpoint(uuid, replication_id, expected) do
    VialKeeper.Eventual.eventually(
      fn -> checkpoint_source_sequence(uuid, replication_id) == {:ok, expected} end,
      timeout: 20_000,
      message: "checkpoint did not reach #{expected}"
    )
  end

  defmodule Barrier do
    @moduledoc false

    def start_link(_owner),
      do:
        Agent.start_link(fn ->
          %{skip_bootstrap_chain: true, skip_bootstrap_blob: true}
        end)

    def hook(agent, conn) do
      case Process.alive?(agent) do
        false -> conn
        true -> hook_alive(agent, conn)
      end
    catch
      :exit, {:noproc, _} -> conn
      :exit, {{:noproc, _}, _} -> conn
    end

    defp hook_alive(agent, conn) do
      kind = classify(conn)
      :ok = maybe_count_blob_open(agent, kind)
      dispatch(agent, conn, kind)
    end

    defp maybe_count_blob_open(agent, :blobs) do
      Agent.update(agent, fn state -> Map.update(state, :blob_opens, 1, &(&1 + 1)) end)
    end

    defp maybe_count_blob_open(_agent, _kind), do: :ok

    defp dispatch(_agent, conn, :other), do: conn

    defp dispatch(agent, conn, kind) do
      cond do
        kind == :chains and Agent.get_and_update(agent, &fail_chain_once/1) == :fail ->
          retryable_failure(conn)

        kind == :blobs and Agent.get_and_update(agent, &fail_blob_once/1) == :fail ->
          retryable_failure(conn)

        Agent.get(agent, &Map.get(&1, :enabled, false)) and
            Agent.get(agent, &Map.get(&1, :"#{kind}_enabled", true)) ->
          dispatch_enabled(agent, conn, kind)

        true ->
          conn
      end
    end

    defp dispatch_enabled(agent, conn, :chains) do
      if Agent.get_and_update(agent, &skip_bootstrap_chain/1) == :skip do
        conn
      else
        wait(agent, :chains)
        {:after, conn, fn -> ack(agent) end}
      end
    end

    defp dispatch_enabled(agent, conn, :blobs) do
      if Agent.get_and_update(agent, &skip_bootstrap_blob/1) == :skip do
        conn
      else
        wait(agent, :blobs)
        {:after, conn, fn -> ack(agent) end}
      end
    end

    defp dispatch_enabled(agent, conn, :checkpoint_source) do
      wait(agent, :checkpoint_source)
      {:after, conn, fn -> ack(agent) end}
    end

    defp retryable_failure(conn) do
      body =
        Jason.encode!(%{
          "error" => %{
            "code" => "database_unavailable",
            "message" => "injected retryable failure",
            "retryable" => true
          }
        })

      {:halt,
       conn
       |> Plug.Conn.put_resp_content_type("application/json")
       |> Plug.Conn.send_resp(503, body)}
    end

    def await(agent, kind, count) do
      VialKeeper.Eventual.eventually(
        fn -> Agent.get(agent, &Map.get(&1, kind, 0)) >= count end,
        timeout: 20_000,
        interval: 5,
        message: "barrier #{kind} did not reach #{count}"
      )
    end

    def await_total(agent, kind, count) do
      VialKeeper.Eventual.eventually(
        fn -> Agent.get(agent, &Map.get(&1, :"#{kind}_total", 0)) >= count end,
        timeout: 20_000,
        interval: 5,
        message: "barrier #{kind} total did not reach #{count}"
      )
    end

    def await_idle(agent) do
      VialKeeper.Eventual.eventually(
        fn ->
          Agent.get(agent, fn state ->
            Enum.empty?(Map.get(state, :chains_waiters, [])) and
              Enum.empty?(Map.get(state, :blobs_waiters, [])) and
              Enum.empty?(Map.get(state, :checkpoint_source_waiters, [])) and
              Enum.empty?(Map.get(state, :pending_acks, [])) and
              Map.get(state, :chains, 0) == 0 and
              Map.get(state, :blobs, 0) == 0 and
              Map.get(state, :checkpoint_source, 0) == 0
          end)
        end,
        timeout: 20_000,
        interval: 5,
        message: "barrier handlers did not acknowledge release"
      )
    end

    def release(agent, kind, count) do
      pids =
        Agent.get_and_update(agent, fn state ->
          {pids, waiters} = Enum.split(Map.get(state, :"#{kind}_waiters", []), count)

          active = max(Map.get(state, kind, 0) - length(pids), 0)

          state =
            state
            |> Map.put(:"#{kind}_waiters", waiters)
            |> Map.put(kind, active)
            |> Map.update(:pending_acks, pids, &(&1 ++ pids))
            |> Map.put(
              :released_chains,
              Map.get(state, :released_chains, 0) +
                if(kind == :chains, do: length(pids), else: 0)
            )

          {pids, state}
        end)

      Enum.each(pids, &send(&1, {:release, kind}))
    end

    def count(agent, key), do: Agent.get(agent, &Map.get(&1, key, 0))

    def pids(agent, kind), do: Agent.get(agent, &Map.get(&1, :"#{kind}_waiters", []))

    def activate(agent) do
      Agent.update(agent, fn state ->
        state
        |> Map.put(:enabled, true)
        |> Map.put(:chains_enabled, true)
        |> Map.put(:blobs_enabled, true)
        |> Map.put(:skip_bootstrap_chain, false)
        |> Map.put(:skip_bootstrap_blob, false)
      end)
    end

    def deactivate_chains(agent), do: Agent.update(agent, &Map.put(&1, :chains_enabled, false))
    def deactivate_blobs(agent), do: Agent.update(agent, &Map.put(&1, :blobs_enabled, false))

    def deactivate(agent) do
      releases =
        Agent.get_and_update(agent, fn state ->
          releases =
            tagged_waiters(state, :chains) ++
              tagged_waiters(state, :blobs) ++
              tagged_waiters(state, :checkpoint_source)

          pids = Enum.map(releases, fn {pid, _kind} -> pid end)

          state =
            state
            |> Map.put(:enabled, false)
            |> Map.put(:chains_enabled, false)
            |> Map.put(:blobs_enabled, false)
            |> Map.put(:chains_waiters, [])
            |> Map.put(:blobs_waiters, [])
            |> Map.put(:checkpoint_source_waiters, [])
            |> Map.put(:chains, 0)
            |> Map.put(:blobs, 0)
            |> Map.put(:checkpoint_source, 0)
            |> Map.update(:pending_acks, pids, &(&1 ++ pids))

          {releases, state}
        end)

      Enum.each(releases, fn {pid, kind} -> send(pid, {:release, kind}) end)
    end

    def fail_next_blob(agent), do: Agent.update(agent, &Map.put(&1, :fail_blob, true))
    def fail_next_chain(agent), do: Agent.update(agent, &Map.put(&1, :fail_chain, true))

    def reset(agent) do
      Agent.update(agent, fn state ->
        pending = Map.get(state, :pending_acks, [])

        unless Enum.empty?(pending) do
          raise "barrier reset with pending acknowledgements: #{inspect(pending)}"
        end

        state
        |> Map.drop([
          :chains_waiters,
          :blobs_waiters,
          :checkpoint_source_waiters,
          :chains,
          :blobs,
          :checkpoint_source,
          :chains_total,
          :blobs_total,
          :checkpoint_source_total,
          :max_chains,
          :max_blobs,
          :pending_acks
        ])
        |> Map.put(:released_chains, 0)
        |> Map.put(:pending_acks, [])
      end)
    end

    defp tagged_waiters(state, kind),
      do: Enum.map(Map.get(state, :"#{kind}_waiters", []), &{&1, kind})

    defp wait(agent, kind) do
      caller = self()

      Agent.update(agent, fn state ->
        total_key = :"#{kind}_total"
        seen = Map.get(state, total_key, 0) + 1
        active = Map.get(state, kind, 0) + 1
        waiters = Map.get(state, :"#{kind}_waiters", []) ++ [caller]

        state
        |> Map.put(total_key, seen)
        |> Map.put(kind, active)
        |> Map.update(:"max_#{kind}", active, &max(&1, active))
        |> Map.put(:"#{kind}_waiters", waiters)
      end)

      receive do
        {:release, ^kind} -> :ok
      end
    end

    defp ack(agent) do
      caller = self()

      if Process.alive?(agent) do
        try do
          Agent.update(agent, fn state ->
            Map.update(state, :pending_acks, [], &List.delete(&1, caller))
          end)
        catch
          :exit, {:noproc, _} -> :ok
          :exit, {{:noproc, _}, _} -> :ok
        end
      end

      :ok
    end

    defp fail_blob_once(state) do
      if Map.get(state, :fail_blob, false) do
        {:fail,
         state
         |> Map.put(:fail_blob, false)
         |> Map.update(:blob_failures, 1, &(&1 + 1))}
      else
        {nil, state}
      end
    end

    defp fail_chain_once(state) do
      if Map.get(state, :fail_chain, false) do
        {:fail,
         state
         |> Map.put(:fail_chain, false)
         |> Map.update(:chain_failures, 1, &(&1 + 1))}
      else
        {nil, state}
      end
    end

    defp skip_bootstrap_chain(%{skip_bootstrap_chain: true} = state),
      do: {:skip, Map.put(state, :skip_bootstrap_chain, false)}

    defp skip_bootstrap_chain(state), do: {nil, state}

    defp skip_bootstrap_blob(%{skip_bootstrap_blob: true} = state),
      do: {:skip, Map.put(state, :skip_bootstrap_blob, false)}

    defp skip_bootstrap_blob(state), do: {nil, state}

    defp classify(%Plug.Conn{method: "POST", request_path: path}) do
      if String.ends_with?(path, "/replication/revisions/get"), do: :chains, else: :other
    end

    defp classify(%Plug.Conn{method: "GET", request_path: path}) do
      if String.contains?(path, "/replication/blobs/"), do: :blobs, else: :other
    end

    defp classify(%Plug.Conn{method: "PUT", request_path: path}) do
      if String.contains?(path, "/replication/checkpoints/"),
        do: :checkpoint_source,
        else: :other
    end

    defp classify(_conn), do: :other
  end
end
