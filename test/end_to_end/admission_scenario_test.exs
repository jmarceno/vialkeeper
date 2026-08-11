defmodule ElixirDB.EndToEnd.AdmissionScenarioTest do
  @moduledoc """
  Uses real Bandit HTTP, real `.elixirdb` bundles, live query subscriptions,
  background retention scheduling, and bounded concurrent local replication.
  Grant ordering and occupancy are proven with deterministic instrumentation,
  not wall-clock timing.

  Split into tagged slow tests so agents can iterate on one proof without
  replaying the full scenario. CI must run all tags (`mix test --include slow`
  on this file, or the project slow gate). Never skip, weaken, re-tag, or
  quarantine these proofs.

  Focused iteration (do **not** also pass `--include slow` with `--only`, or
  every slow test in the file matches):

      mix test test/end_to_end/admission_scenario_test.exs --only admission_e2e_scheduling
      mix test test/end_to_end/admission_scenario_test.exs --only admission_e2e_isolation
      mix test test/end_to_end/admission_scenario_test.exs --only admission_e2e_composition

  Tags (also all `@tag :slow`):

  - `:admission_e2e_scheduling` — fairness, reservations, kill, and timeout races
  - `:admission_e2e_isolation` — disconnect cleanup, streaming, and A/B independence
  - `:admission_e2e_composition` — retention, replication, sustained load, close, and correctness
  """
  use ExUnit.Case, async: false

  @moduletag :integration

  import ExUnit.Assertions

  alias ElixirDB.Attachments
  alias ElixirDB.Eventual
  alias ElixirDB.Query.Subscriptions
  alias ElixirDB.Replication.{BlobStream, JobManager, LocalEndpoint}

  alias ElixirDB.Runtime.{
    AdmissionPolicy,
    AttachmentCoordinator,
    DatabaseAdmission,
    DatabaseCatalog,
    RetentionScheduler
  }

  alias ElixirDB.TestServer
  alias ElixirDB.TestSupport.{AdmissionClassProbe, AdmissionScenario}
  alias ElixirDB.View.Manager

  @admission_limit 12
  @blob_payload "admission-e2e-attachment-payload"

  setup do
    previous_limits = Application.get_env(:elixir_db, :host_limits)
    previous_policy = Application.get_env(:elixir_db, :admission_policy)

    limits = Keyword.put(previous_limits || [], :admission_limit, @admission_limit)
    policy = AdmissionPolicy.default_keyword()

    Application.put_env(:elixir_db, :host_limits, limits)
    Application.put_env(:elixir_db, :admission_policy, policy)

    on_exit(fn ->
      AdmissionClassProbe.uninstall()
      AdmissionScenario.uninstall_test_hook()
      Application.delete_env(:elixir_db, :admitted_command_sync)
      Application.delete_env(:elixir_db, :admitted_command_owner_body_sync)
      Application.delete_env(:elixir_db, :subscription_execute_snapshot_sync)
      Application.delete_env(:elixir_db, :subscription_hub_pause_reads)
      Application.delete_env(:elixir_db, :admission_test_hook)
      Application.put_env(:elixir_db, :host_limits, previous_limits)
      Application.put_env(:elixir_db, :admission_policy, previous_policy)
    end)

    :ok
  end

  @tag :slow
  @tag admission_e2e_scheduling: true
  @tag timeout: 90_000
  test "scheduling: fairness, reservations, kill, and timeout races" do
    ctx = bootstrap_seeded_pair!()
    %{a_uuid: a_uuid, probe_ref: probe_ref, hook_ref: hook_ref} = ctx

    AdmissionScenario.await_stats(a_uuid, &(&1.total_occupancy == 0), timeout: 30_000)

    AdmissionScenario.assert_sustained_fairness!(a_uuid, probe_ref, @admission_limit,
      backlog_per_class: 2,
      job_id: nil,
      subscription_pids: [],
      hook_ref: hook_ref
    )

    AdmissionScenario.assert_reservation_pressure!(a_uuid, @admission_limit)

    AdmissionScenario.assert_killed_queued_never_granted!(a_uuid, probe_ref, hook_ref)
    AdmissionScenario.assert_timeout_race_clean!(a_uuid, hook_ref)
  end

  @tag :slow
  @tag admission_e2e_isolation: true
  @tag timeout: 90_000
  test "isolation: disconnect cleanup, streaming permits, and A/B independence" do
    ctx = bootstrap_seeded_pair!()
    %{server: server, a_uuid: a_uuid, b_uuid: b_uuid, blob: blob, hook_ref: hook_ref} = ctx

    seed_database_mirror!(server, b_uuid, blob)

    assert_subscription_disconnect_cleanup!(a_uuid, hook_ref)
    assert_active_subscription_disconnect_cleanup!(a_uuid, hook_ref)

    assert_streaming_non_retaining!(a_uuid, b_uuid, blob)
    assert_database_independence!(a_uuid, b_uuid, server)
  end

  @tag :slow
  @tag admission_e2e_composition: true
  @tag timeout: 120_000
  test "composition: retention, replication, sustained load, close, and correctness" do
    ctx = bootstrap_seeded_pair!()

    %{
      server: server,
      a_uuid: a_uuid,
      b_uuid: b_uuid,
      blob: blob,
      probe_ref: probe_ref,
      hook_ref: hook_ref
    } = ctx

    {sub_a, sub_b} = open_live_subscriptions!(a_uuid)
    prove_retention_maintenance!(a_uuid, probe_ref)
    job_id = start_continuous_replication!(a_uuid, b_uuid)

    await_replication_doc(b_uuid, "task-open")
    await_replication_doc(b_uuid, "attached")

    Subscriptions.close(sub_a)
    Subscriptions.close(sub_b)

    assert {:ok, %{state: :disabled}} = JobManager.disable(a_uuid, job_id)
    AdmissionScenario.await_stats(a_uuid, &(&1.total_occupancy == 0), timeout: 30_000)

    # Real-path fairness while replication is disabled and no live subscriptions
    # are open — avoids contaminating the grant sample.
    AdmissionScenario.begin_peak_occupancy_tracking(a_uuid)
    _ = AdmissionScenario.drain_grants(probe_ref, 0)

    AdmissionScenario.assert_real_path_continuous_fairness!(
      a_uuid,
      probe_ref,
      @admission_limit,
      backlog_per_class: 2,
      hook_ref: hook_ref
    )

    assert {:ok, _} = JobManager.enable(a_uuid, job_id)
    await_replication_active(a_uuid, job_id)

    assert {:ok, sub_a} =
             Subscriptions.open(
               a_uuid,
               %{"query" => %{"selector" => %{"/type" => "task"}}, "heartbeat_ms" => 100},
               self()
             )

    assert {:ok, sub_b} =
             Subscriptions.open(
               a_uuid,
               %{"query" => %{"selector" => %{"/status" => "open"}}, "heartbeat_ms" => 100},
               self()
             )

    task_membership = snapshot_membership!(sub_a)

    assert task_membership ==
             MapSet.new(["task-open", "task-done", "attached"])

    open_membership = snapshot_membership!(sub_b)
    assert open_membership == MapSet.new(["task-open", "attached"])
    assert 2 = Subscriptions.count(a_uuid)

    AdmissionScenario.begin_peak_occupancy_tracking(a_uuid)
    AdmissionScenario.begin_peak_occupancy_tracking(b_uuid)
    _ = AdmissionScenario.drain_grants(probe_ref, 0)

    class_pressure =
      Task.async(fn ->
        for _ <- 1..24 do
          assert {:ok, _} =
                   DatabaseCatalog.command_as(
                     a_uuid,
                     :subscription,
                     {:command, :identity, %{}}
                   )

          assert {:ok, _} =
                   DatabaseAdmission.execute(
                     a_uuid,
                     :maintenance,
                     {:command, :identity, %{}}
                   )
        end

        :pressure_done
      end)

    sustained =
      Task.async(fn ->
        for index <- 1..16 do
          assert %{status: 201} =
                   put_document!(server, a_uuid, "sustained-#{index}", %{
                     "type" => "task",
                     "status" => "open",
                     "n" => index
                   })

          assert %{status: 200, body: body} =
                   query!(server, a_uuid, %{"selector" => %{"/type" => "task"}})

          assert is_list(body["data"]["documents"])
        end

        :sustained_done
      end)

    assert :sustained_done = Task.await(sustained, 60_000)
    assert :pressure_done = Task.await(class_pressure, 60_000)

    # Capture grants from the sustained all-class phase before waiting on
    # post-phase replication catch-up, which would contaminate the sample.
    grants_during_sustained = AdmissionScenario.drain_grants(probe_ref, 500)

    await_replication_doc(b_uuid, "sustained-16")

    replication_grants =
      Enum.filter(grants_during_sustained, fn {class, _op} -> class == :replication end)

    subscription_grants =
      Enum.filter(grants_during_sustained, fn {class, _op} -> class == :subscription end)

    maintenance_grants =
      Enum.filter(grants_during_sustained, fn {class, _op} -> class == :maintenance end)

    foreground_grants =
      Enum.filter(grants_during_sustained, fn {class, _op} -> class == :foreground end)

    refute replication_grants == [],
           "continuous replication did not produce replication-class grants during sustained load"

    refute subscription_grants == [],
           "subscription work did not produce subscription-class grants during sustained load"

    refute maintenance_grants == [],
           "maintenance work did not produce maintenance-class grants during sustained load"

    refute foreground_grants == [],
           "foreground HTTP work did not produce foreground grants during sustained load"

    peak_a = AdmissionScenario.peak_occupancy(a_uuid)
    peak_b = AdmissionScenario.peak_occupancy(b_uuid)
    AdmissionScenario.assert_max_occupancy!(peak_a, @admission_limit)
    AdmissionScenario.assert_max_occupancy!(peak_b, @admission_limit)

    assert_close_drains_admission!(a_uuid, job_id, sub_a, sub_b)

    assert {:ok, _} = DatabaseCatalog.open(a_uuid)
    assert :ok = Manager.await_resumed(a_uuid)
    AdmissionScenario.await_stats(a_uuid, &(&1.total_occupancy == 0), timeout: 30_000)

    assert {:ok, stats} = DatabaseAdmission.stats(a_uuid)
    assert stats.total_occupancy == 0
    assert stats.queued_foreground == 0
    assert stats.queued_subscription == 0
    assert stats.queued_replication == 0
    assert stats.queued_maintenance == 0

    refute match?({:ok, true}, DatabaseAdmission.closing?(a_uuid))

    assert 0 = Subscriptions.count(a_uuid)

    assert {:ok, sub_after} =
             Subscriptions.open(
               a_uuid,
               %{"query" => %{"selector" => %{"/type" => "task"}}, "heartbeat_ms" => 100},
               self()
             )

    assert 1 = Subscriptions.count(a_uuid)
    assert_correctness!(server, a_uuid, b_uuid, job_id, blob, sub_after)
  end

  defp bootstrap_seeded_pair! do
    server = TestServer.start_supervised!()
    root = ElixirDB.Config.database_root()
    prefix = "admission-e2e-#{System.unique_integer([:positive])}"
    a_path = prefix <> "-a.elixirdb"
    b_path = prefix <> "-b.elixirdb"

    for path <- [a_path, b_path] do
      ElixirDB.TempDatabase.cleanup(Path.join(root, path))
    end

    a_uuid = create_database!(server, a_path)
    b_uuid = create_database!(server, b_path)

    on_exit(fn ->
      _ = maybe_disable_jobs(a_uuid)
      _ = DatabaseCatalog.close(a_uuid)
      _ = DatabaseCatalog.close(b_uuid)
      _ = DatabaseCatalog.unregister(a_uuid)
      _ = DatabaseCatalog.unregister(b_uuid)

      for path <- [a_path, b_path] do
        ElixirDB.TempDatabase.cleanup(Path.join(root, path))
      end
    end)

    blob = upload_attachment!(server, a_uuid, @blob_payload)
    seed_database!(server, a_uuid, blob)
    create_index!(server, a_uuid)

    assert {:ok, _} =
             DatabaseCatalog.command(
               a_uuid,
               {:command, :update_config,
                %{
                  "subscriptions" => %{"max_buffered_events" => 8},
                  "retention" => %{
                    "mode" => "stable_frontier",
                    "history_depth" => 0,
                    "peer_expiry_ms" => 86_400_000,
                    "schedule" => "disabled"
                  }
                }}
             )

    probe_ref = AdmissionScenario.install_probe()
    hook_ref = AdmissionScenario.install_test_hook()
    :ok = AdmissionScenario.begin_peak_occupancy_tracking(a_uuid)

    %{
      server: server,
      a_uuid: a_uuid,
      b_uuid: b_uuid,
      a_path: a_path,
      b_path: b_path,
      blob: blob,
      probe_ref: probe_ref,
      hook_ref: hook_ref
    }
  end

  defp seed_database!(server, uuid, blob) do
    seed_docs = [
      {"task-open", %{"type" => "task", "status" => "open", "title" => "seed", "priority" => 3}},
      {"task-done", %{"type" => "task", "status" => "done", "title" => "done", "priority" => 1}},
      {"note", %{"type" => "note", "title" => "note"}}
    ]

    for {id, body} <- seed_docs do
      assert %{status: 201} = put_document!(server, uuid, id, body)
    end

    assert %{status: 201} =
             put_document!(
               server,
               uuid,
               "attached",
               %{"type" => "task", "status" => "open", "title" => "with-blob"},
               %{"file.bin" => %{"blob" => blob, "content_type" => "application/octet-stream"}}
             )

    :ok
  end

  defp seed_database_mirror!(server, uuid, _blob) do
    # Isolation proofs need B populated without continuous replication; upload
    # the payload into B's attachment store so document puts resolve locally.
    mirrored_blob = upload_attachment!(server, uuid, @blob_payload)
    seed_database!(server, uuid, mirrored_blob)
    create_index!(server, uuid)
    :ok
  end

  defp open_live_subscriptions!(a_uuid) do
    assert {:ok, sub_a} =
             Subscriptions.open(
               a_uuid,
               %{"query" => %{"selector" => %{"/type" => "task"}}, "heartbeat_ms" => 100},
               self()
             )

    assert {:ok, sub_b} =
             Subscriptions.open(
               a_uuid,
               %{"query" => %{"selector" => %{"/status" => "open"}}, "heartbeat_ms" => 100},
               self()
             )

    assert {:ok, %{type: :snapshot, document: %{id: _}}} = Subscriptions.next(sub_a, 5_000)
    assert {:ok, %{type: :caught_up}} = drain_to_caught_up(sub_a)
    assert {:ok, %{type: :snapshot, document: %{id: _}}} = Subscriptions.next(sub_b, 5_000)
    assert {:ok, %{type: :caught_up}} = drain_to_caught_up(sub_b)

    {sub_a, sub_b}
  end

  defp prove_retention_maintenance!(a_uuid, probe_ref) do
    assert [{scheduler_pid, _}] =
             Registry.lookup(
               ElixirDB.Runtime.DatabaseRegistry,
               {:retention_scheduler, a_uuid}
             )

    :sys.suspend(scheduler_pid)

    assert {:ok, _} =
             DatabaseCatalog.command(
               a_uuid,
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

    :ok = RetentionScheduler.reschedule(a_uuid)
    _ = AdmissionScenario.drain_grants(probe_ref, 0)
    send(scheduler_pid, :scheduled_compact)
    :sys.resume(scheduler_pid)

    assert Eventual.eventually(
             fn ->
               grants = AdmissionScenario.drain_grants(probe_ref, 100)

               Enum.any?(grants, fn {class, op} ->
                 class == :maintenance and op == :compact_retention
               end)
             end,
             timeout: 10_000,
             message: "RetentionScheduler did not grant :maintenance compact_retention on A"
           )

    assert {:ok, %{floor_sequence: floor}} =
             DatabaseCatalog.command(a_uuid, {:command, :retention_status, %{}})

    assert is_integer(floor) and floor > 0,
           "retention maintenance did not advance floor_sequence"

    assert {:ok, _} =
             DatabaseCatalog.command(
               a_uuid,
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

    await_attachment_gc_idle(a_uuid)
    :ok
  end

  defp start_continuous_replication!(a_uuid, b_uuid) do
    assert {:ok, %{job_id: job_id}} =
             JobManager.put(a_uuid, %{
               "persist" => true,
               "mode" => "continuous",
               "direction" => "push",
               "enabled" => true,
               "wait_ms" => 50,
               "max_concurrent_chain_fetches" => 2,
               "max_concurrent_blob_transfers" => 2,
               "max_transfer_bytes_in_flight" => 1_048_576,
               "retry" => %{
                 "max_attempts" => 8,
                 "base_delay_ms" => 20,
                 "max_delay_ms" => 200,
                 "jitter_ms" => 1
               },
               "endpoint" => %{"kind" => "local", "database_uuid" => b_uuid}
             })

    await_replication_active(a_uuid, job_id)
    job_id
  end

  defp assert_subscription_disconnect_cleanup!(uuid, hook_ref) do
    flush_admission_mailbox!(hook_ref)

    parent = self()
    gate_ref = make_ref()
    :ok = Application.put_env(:elixir_db, :admitted_command_sync, {parent, gate_ref, uuid})

    blocker =
      Task.async(fn ->
        DatabaseAdmission.execute_owner(uuid, :foreground, fn -> :block end)
      end)

    assert_receive {^gate_ref, :before_begin, blocker_exec}, 2_000

    client =
      spawn(fn ->
        _result =
          Subscriptions.open(
            uuid,
            %{"query" => %{"selector" => %{"/type" => "task"}}, "heartbeat_ms" => 100},
            self()
          )
      end)

    assert_receive {^hook_ref, :enqueued, request_ref, :subscription, :identity, _caller}, 5_000

    refute_receive {^hook_ref, :granted, ^request_ref, _, _}, 0

    Process.exit(client, :kill)
    client_ref = Process.monitor(client)
    assert_receive {:DOWN, ^client_ref, :process, ^client, _}, 2_000

    AdmissionScenario.await_stats(
      uuid,
      &(&1.queued_subscription == 0 and &1.total_occupancy <= @admission_limit)
    )

    refute_receive {^hook_ref, :granted, ^request_ref, _, _}, 0

    send(blocker_exec, {:go, gate_ref})
    Application.delete_env(:elixir_db, :admitted_command_sync)
    Task.await(blocker, 5_000)
  end

  defp assert_active_subscription_disconnect_cleanup!(uuid, hook_ref) do
    flush_admission_mailbox!(hook_ref)

    parent = self()
    snapshot_gate = make_ref()
    before_gate = make_ref()
    body_gate = make_ref()

    :ok =
      Application.put_env(
        :elixir_db,
        :subscription_execute_snapshot_sync,
        {parent, snapshot_gate, uuid}
      )

    client =
      spawn(fn ->
        receive do
          :stop -> :ok
        end
      end)

    open_task =
      Task.async(fn ->
        Subscriptions.open(
          uuid,
          %{"query" => %{"selector" => %{"/type" => "task"}}, "heartbeat_ms" => 100},
          client
        )
      end)

    assert_receive {^snapshot_gate, :execute_snapshot_ready, sub}, 10_000
    assert {:ok, ^sub} = Task.await(open_task, 5_000)
    assert Process.alive?(sub)

    # Pause further hub reads, then drain any in-flight admission so snapshot is alone.
    :ok = Application.put_env(:elixir_db, :subscription_hub_pause_reads, uuid)

    AdmissionScenario.await_stats(uuid, &(&1.total_occupancy == 0 and &1.active_class == nil),
      timeout: 15_000
    )

    flush_admission_mailbox!(hook_ref)

    :ok =
      Application.put_env(
        :elixir_db,
        :admitted_command_sync,
        {parent, before_gate, uuid, :execute_subscription_snapshot}
      )

    :ok =
      Application.put_env(
        :elixir_db,
        :admitted_command_owner_body_sync,
        {parent, body_gate, uuid, :execute_subscription_snapshot}
      )

    send(sub, {:go, snapshot_gate})
    Application.delete_env(:elixir_db, :subscription_execute_snapshot_sync)

    snap_executor =
      await_subscription_snapshot_at_owner_body!(hook_ref, before_gate, body_gate, sub)

    Process.exit(client, :kill)
    client_mon = Process.monitor(client)
    assert_receive {:DOWN, ^client_mon, :process, ^client, _}, 2_000

    assert {:ok, stats_mid} = DatabaseAdmission.stats(uuid)
    assert stats_mid.total_occupancy == 1
    assert stats_mid.active_class == :subscription

    send(snap_executor, {:go, body_gate})
    Application.delete_env(:elixir_db, :admitted_command_owner_body_sync)
    Application.delete_env(:elixir_db, :subscription_hub_pause_reads)

    AdmissionScenario.await_stats(uuid, &(&1.total_occupancy == 0 and &1.active_class == nil),
      timeout: 10_000
    )

    Eventual.eventually(
      fn -> not Process.alive?(sub) and Subscriptions.count(uuid) == 0 end,
      timeout: 5_000,
      message: "live subscription was not cleaned after client disconnect"
    )
  end

  defp await_subscription_snapshot_at_owner_body!(hook_ref, before_gate, body_gate, sub) do
    receive do
      {^hook_ref, :enqueued, snap_ref, :subscription, :execute_subscription_snapshot, ^sub} ->
        assert_receive {^hook_ref, :granted, ^snap_ref, :subscription,
                        :execute_subscription_snapshot},
                       10_000

        assert_receive {^before_gate, :before_begin, snap_executor}, 5_000
        send(snap_executor, {:go, before_gate})
        Application.delete_env(:elixir_db, :admitted_command_sync)
        assert_receive {^body_gate, :owner_body, ^snap_executor}, 5_000
        snap_executor

      {^hook_ref, :enqueued, _request_ref, :subscription, probe_op, _caller}
      when probe_op != :execute_subscription_snapshot ->
        # Competing hub reads are not gated; let them complete and keep waiting.
        await_subscription_snapshot_at_owner_body!(hook_ref, before_gate, body_gate, sub)

      {^hook_ref, :granted, _request_ref, :subscription, probe_op}
      when probe_op != :execute_subscription_snapshot ->
        await_subscription_snapshot_at_owner_body!(hook_ref, before_gate, body_gate, sub)

      {_probe_ref, :admission_grant, :subscription, probe_op}
      when probe_op != :execute_subscription_snapshot ->
        await_subscription_snapshot_at_owner_body!(hook_ref, before_gate, body_gate, sub)

      {^hook_ref, :enqueued, _request_ref, :subscription, :execute_subscription_snapshot,
       other_caller} ->
        flunk(
          "expected execute_subscription_snapshot caller #{inspect(sub)}, got #{inspect(other_caller)}"
        )
    after
      10_000 ->
        flunk("timed out waiting for live subscription snapshot admission")
    end
  end

  defp flush_admission_mailbox!(hook_ref) do
    receive do
      {^hook_ref, _, _, _, _} ->
        flush_admission_mailbox!(hook_ref)

      {^hook_ref, _, _, _, _, _} ->
        flush_admission_mailbox!(hook_ref)

      {_probe_ref, :admission_grant, _, _} ->
        flush_admission_mailbox!(hook_ref)
    after
      0 -> :ok
    end
  end

  defp assert_streaming_non_retaining!(a_uuid, b_uuid, blob) do
    AdmissionScenario.with_suspended_replication_workers!(a_uuid, fn ->
      AdmissionScenario.await_stats(a_uuid, &(&1.active_class == nil), timeout: 15_000)
      assert_slow_attachment_upload_non_retaining!(a_uuid)
      AdmissionScenario.await_stats(a_uuid, &(&1.active_class == nil), timeout: 15_000)
      assert_slow_attachment_download_non_retaining!(a_uuid, blob)
      AdmissionScenario.await_stats(a_uuid, &(&1.active_class == nil), timeout: 15_000)
      assert_slow_replication_blob_transfer_non_retaining!(a_uuid, b_uuid)
      AdmissionScenario.await_stats(a_uuid, &(&1.active_class == nil), timeout: 15_000)
      assert_concurrent_replication_transfers_non_retaining!(a_uuid, b_uuid)
    end)
  end

  defp assert_slow_attachment_upload_non_retaining!(uuid) do
    parent = self()
    gate = make_ref()

    source = fn ->
      {:ok, "slow-",
       fn ->
         send(parent, {:upload_blocked, gate})

         receive do
           {:release, ^gate} -> {:ok, "attachment", fn -> :done end}
         after
           5_000 -> {:error, :timeout}
         end
       end}
    end

    upload = Task.async(fn -> Attachments.upload_stream(uuid, source) end)
    assert_receive {:upload_blocked, ^gate}, 2_000

    assert {:ok, 0} = DatabaseAdmission.active_count(uuid)

    assert {:ok, _} =
             DatabaseCatalog.command(
               uuid,
               {:command, :put, %{document_id: "during-upload", body: %{"type" => "note"}}}
             )

    send(upload.pid, {:release, gate})
    assert {:ok, _} = Task.await(upload, 10_000)
    AdmissionScenario.await_stats(uuid, &(&1.total_occupancy == 0))
  end

  defp assert_slow_attachment_download_non_retaining!(uuid, blob) do
    parent = self()
    gate = make_ref()

    assert {:ok, put} =
             ElixirDB.Documents.put(uuid, %{
               "id" => "download-doc",
               "body" => %{},
               "attachments" => %{
                 "file.bin" => %{"blob" => blob, "content_type" => "application/octet-stream"}
               }
             })

    download =
      Task.async(fn ->
        assert {:ok, stream} =
                 Attachments.open_stream(uuid, %{
                   "id" => "download-doc",
                   "revision" => put.revision,
                   "name" => "file.bin"
                 })

        {[first], rest} = Enum.split(stream.body, 1)
        send(parent, {:download_blocked, gate, first})

        receive do
          {:release, ^gate} -> Enum.into(rest, <<>>)
        after
          5_000 -> flunk("download was not released")
        end
      end)

    assert_receive {:download_blocked, ^gate, first_chunk}, 2_000
    assert is_binary(first_chunk)

    assert {:ok, 0} = DatabaseAdmission.active_count(uuid)

    assert {:ok, _} =
             DatabaseCatalog.command(
               uuid,
               {:command, :put, %{document_id: "during-download", body: %{"type" => "note"}}}
             )

    send(download.pid, {:release, gate})
    remainder = Task.await(download, 10_000)
    assert first_chunk <> remainder == @blob_payload
    AdmissionScenario.await_stats(uuid, &(&1.total_occupancy == 0))
  end

  defp assert_slow_replication_blob_transfer_non_retaining!(source_uuid, target_uuid) do
    {:ok, source_endpoint} = LocalEndpoint.new(source_uuid)
    {:ok, target_endpoint} = LocalEndpoint.new(target_uuid)

    payload = :binary.copy("replication-transfer-", 16_384)
    digest = :crypto.hash(:sha256, payload) |> Base.encode16(case: :lower)
    parent = self()
    gate = make_ref()

    assert {:ok, %{blob: ^digest}} = Attachments.upload_stream(source_uuid, [payload])

    transfer =
      Task.async(fn ->
        assert {:ok, source_stream} = LocalEndpoint.open_blob(source_endpoint, digest)
        gated_body = gated_blob_body(source_stream.body, parent, gate, :blob_transfer_blocked)
        assert {:ok, target_stream} = BlobStream.new(digest, byte_size(payload), gated_body)
        LocalEndpoint.put_blob(target_endpoint, target_stream)
      end)

    assert_receive {:blob_transfer_blocked, ^gate}, 2_000

    assert {:ok, 0} = DatabaseAdmission.active_count(source_uuid)
    assert {:ok, 0} = DatabaseAdmission.active_count(target_uuid)

    assert {:ok, _} =
             DatabaseCatalog.command(
               source_uuid,
               {:command, :put, %{document_id: "during-repl-transfer", body: %{"type" => "note"}}}
             )

    send(transfer.pid, {:release, gate})
    assert :ok = Task.await(transfer, 10_000)
  end

  defp assert_concurrent_replication_transfers_non_retaining!(source_uuid, target_uuid) do
    {:ok, source_endpoint} = LocalEndpoint.new(source_uuid)
    {:ok, target_endpoint} = LocalEndpoint.new(target_uuid)

    parent = self()
    gate = make_ref()
    transfer_count = 3

    digests =
      for index <- 1..transfer_count do
        payload = :binary.copy("concurrent-repl-#{index}-", 8_192)
        digest = :crypto.hash(:sha256, payload) |> Base.encode16(case: :lower)
        assert {:ok, %{blob: ^digest}} = Attachments.upload_stream(source_uuid, [payload])
        {digest, byte_size(payload)}
      end

    transfers =
      Enum.map(digests, fn {digest, size} ->
        Task.async(fn ->
          assert {:ok, source_stream} = LocalEndpoint.open_blob(source_endpoint, digest)

          gated_body =
            gated_blob_body(source_stream.body, parent, gate, {:blob_transfer_blocked, digest})

          assert {:ok, target_stream} = BlobStream.new(digest, size, gated_body)
          LocalEndpoint.put_blob(target_endpoint, target_stream)
        end)
      end)

    for _ <- 1..transfer_count do
      assert_receive {:blob_transfer_blocked, ^gate, _digest}, 2_000
    end

    for uuid <- [source_uuid, target_uuid] do
      assert {:ok, 0} = DatabaseAdmission.active_count(uuid)
    end

    for transfer <- transfers do
      send(transfer.pid, {:release, gate})
    end

    for transfer <- transfers do
      assert :ok = Task.await(transfer, 10_000)
    end
  end

  defp gated_blob_body(body, parent, gate, :blob_transfer_blocked) do
    Stream.transform(body, false, fn chunk, blocked? ->
      gate_blob_chunk(chunk, blocked?, parent, gate, fn ->
        send(parent, {:blob_transfer_blocked, gate})
      end)
    end)
  end

  defp gated_blob_body(body, parent, gate, {:blob_transfer_blocked, digest}) do
    Stream.transform(body, false, fn chunk, blocked? ->
      gate_blob_chunk(chunk, blocked?, parent, gate, fn ->
        send(parent, {:blob_transfer_blocked, gate, digest})
      end)
    end)
  end

  defp gate_blob_chunk(chunk, true, _parent, _gate, _notify), do: {[chunk], true}

  defp gate_blob_chunk(chunk, false, _parent, gate, notify) do
    notify.()

    receive do
      {:release, ^gate} -> {[chunk], true}
    after
      5_000 -> flunk("blob transfer was not released")
    end
  end

  defp assert_database_independence!(a_uuid, b_uuid, server) do
    parent = self()
    a_gate = make_ref()
    b_gate = make_ref()
    :ok = Application.put_env(:elixir_db, :admitted_command_sync, {parent, a_gate, a_uuid})

    a_blocker =
      Task.async(fn ->
        DatabaseCatalog.command(a_uuid, {:command, :identity, %{}})
      end)

    assert_receive {^a_gate, :before_begin, a_executor}, 2_000

    a_waiters =
      for class <- [:subscription, :replication, :maintenance, :foreground] do
        Task.async(fn ->
          DatabaseCatalog.command_as(a_uuid, class, {:command, :identity, %{}})
        end)
      end

    AdmissionScenario.await_stats(
      a_uuid,
      fn stats ->
        stats.queued_subscription >= 1 and stats.queued_replication >= 1 and
          stats.queued_maintenance >= 1 and stats.queued_foreground >= 1
      end
    )

    # B remains independent while A is blocked.
    assert %{status: 200, body: body} =
             Req.post!(
               server.base_url <> "/v1/databases/#{b_uuid}/documents/get",
               json: %{"id" => "task-open"},
               receive_timeout: 5_000
             )

    assert body["data"]["body"]["title"] == "seed"

    # Force a non-vacuous B occupancy peak, then drain.
    AdmissionScenario.begin_peak_occupancy_tracking(b_uuid)
    :ok = Application.put_env(:elixir_db, :admitted_command_sync, {parent, b_gate, b_uuid})

    b_blocker =
      Task.async(fn ->
        DatabaseCatalog.command(b_uuid, {:command, :identity, %{}})
      end)

    assert_receive {^b_gate, :before_begin, b_executor}, 5_000

    b_waiter =
      Task.async(fn ->
        DatabaseCatalog.command(b_uuid, {:command, :identity, %{}})
      end)

    AdmissionScenario.await_stats(b_uuid, &(&1.total_occupancy >= 1 and &1.queued_foreground >= 1),
      timeout: 5_000
    )

    peak_b = AdmissionScenario.peak_occupancy(b_uuid)
    assert peak_b >= 1
    AdmissionScenario.assert_max_occupancy!(peak_b, @admission_limit)

    send(b_executor, {:go, b_gate})
    Application.delete_env(:elixir_db, :admitted_command_sync)
    assert {:ok, _} = Task.await(b_blocker, 10_000)
    assert {:ok, _} = Task.await(b_waiter, 10_000)
    AdmissionScenario.await_stats(b_uuid, &(&1.total_occupancy == 0))

    send(a_executor, {:go, a_gate})
    assert {:ok, _} = Task.await(a_blocker, 10_000)

    for task <- a_waiters do
      assert {:ok, _} = Task.await(task, 10_000)
    end

    AdmissionScenario.await_stats(a_uuid, &(&1.total_occupancy == 0))
  end

  defp assert_close_drains_admission!(uuid, job_id, sub_a, sub_b) do
    Application.delete_env(:elixir_db, :admitted_command_sync)
    Application.delete_env(:elixir_db, :admitted_command_owner_body_sync)
    await_replication_active(uuid, job_id)

    # Active replication blocks close; callers must disable or cancel the job first.
    assert {:error, %ElixirDB.Error{code: :database_not_closable}} =
             DatabaseCatalog.close(uuid)

    assert {:ok, false} = DatabaseAdmission.closing?(uuid)
    assert {:ok, %{state: :disabled}} = JobManager.disable(uuid, job_id)

    Eventual.eventually(
      fn -> not JobManager.active?(uuid) end,
      timeout: 30_000,
      message: "replication job did not drain after disable"
    )

    AdmissionScenario.await_stats(uuid, &(&1.total_occupancy == 0), timeout: 30_000)

    parent = self()

    active =
      Task.async(fn ->
        DatabaseAdmission.execute_owner(uuid, :foreground, fn ->
          send(parent, {:close_blocked, self()})
          receive(do: (:finish -> :done))
        end)
      end)

    assert_receive {:close_blocked, executor_pid}, 5_000

    queued =
      Task.async(fn ->
        DatabaseAdmission.execute_owner(uuid, :foreground, fn -> :queued end)
      end)

    replication_during_close =
      Task.async(fn ->
        DatabaseCatalog.command_as(uuid, :replication, {:command, :identity, %{}})
      end)

    AdmissionScenario.await_stats(
      uuid,
      &(&1.queued_foreground >= 1 and &1.queued_replication >= 1),
      timeout: 10_000
    )

    closer = Task.async(fn -> DatabaseCatalog.close(uuid) end)

    Eventual.eventually(
      fn ->
        case DatabaseAdmission.closing?(uuid) do
          {:ok, true} -> true
          _ -> false
        end
      end,
      timeout: 10_000,
      message: "admission did not enter closing state"
    )

    assert {:error, %ElixirDB.Error{code: :database_closed}} =
             DatabaseAdmission.execute_owner(uuid, :foreground, fn -> :new_after_close end, 1_000)

    assert {:error, %ElixirDB.Error{code: :database_closed}} = Task.await(queued, 5_000)

    assert {:error, %ElixirDB.Error{code: :database_closed}} =
             Task.await(replication_during_close, 5_000)

    send(executor_pid, :finish)
    assert :done = Task.await(active, 10_000)
    assert :ok = Task.await(closer, 15_000)

    assert match?({:error, _}, DatabaseAdmission.stats(uuid)) or
             match?({:ok, %{total_occupancy: 0}}, DatabaseAdmission.stats(uuid))

    assert match?({:closed, _}, Subscriptions.next(sub_a, 1_000)) or
             match?({:error, _}, Subscriptions.next(sub_a, 1_000))

    assert match?({:closed, _}, Subscriptions.next(sub_b, 1_000)) or
             match?({:error, _}, Subscriptions.next(sub_b, 1_000))
  end

  defp assert_correctness!(server, a_uuid, b_uuid, _job_id, _blob, sub_pid) do
    assert {:ok, %{revision: _}} = ElixirDB.Documents.get(a_uuid, %{id: "task-open"})
    assert {:ok, %{revision: _}} = ElixirDB.Documents.get(b_uuid, %{id: "task-open"})
    assert {:ok, %{revision: attached_revision}} = ElixirDB.Documents.get(b_uuid, %{id: "attached"})

    assert {:ok, stream} =
             Attachments.open_stream(b_uuid, %{
               "id" => "attached",
               "revision" => attached_revision,
               "name" => "file.bin"
             })

    downloaded = Enum.into(stream.body, <<>>)
    assert downloaded == @blob_payload

    assert %{status: 200, body: query_body} =
             query!(server, a_uuid, %{"selector" => %{"/type" => "task"}, "limit" => 40})

    query_ids =
      query_body["data"]["documents"]
      |> Enum.map(& &1["id"])
      |> MapSet.new()

    expected_task_ids =
      MapSet.new(
        ["task-open", "task-done", "attached"] ++
          Enum.map(1..16, &"sustained-#{&1}")
      )

    assert MapSet.subset?(expected_task_ids, query_ids)

    membership = snapshot_membership!(sub_pid)
    assert membership == expected_task_ids

    assert %{status: 201} =
             put_document!(server, a_uuid, "membership-open", %{
               "type" => "task",
               "status" => "open",
               "title" => "membership"
             })

    assert_receive_subscription_upsert!(sub_pid, "membership-open")

    assert {:ok, %{ok: true}} =
             DatabaseCatalog.command(a_uuid, {:command, :integrity_check, %{}})

    assert {:ok, %{ok: true}} =
             DatabaseCatalog.command(b_uuid, {:command, :integrity_check, %{}})

    assert {:ok, %{floor_sequence: floor}} =
             DatabaseCatalog.command(a_uuid, {:command, :retention_status, %{}})

    assert is_integer(floor) and floor > 0

    assert {:ok, %{status: :completed}} = ElixirDB.Replication.one_shot(a_uuid, b_uuid)

    assert {:ok, %{revision: _}} = ElixirDB.Documents.get(b_uuid, %{id: "sustained-16"})
    Subscriptions.close(sub_pid)
  end

  defp snapshot_membership!(pid) do
    snapshot_membership_loop(pid, MapSet.new())
  end

  defp snapshot_membership_loop(pid, acc) do
    case Subscriptions.next(pid, 5_000) do
      {:ok, %{type: :snapshot, document: %{id: id}}} ->
        snapshot_membership_loop(pid, MapSet.put(acc, id))

      {:ok, %{type: :caught_up}} ->
        acc

      {:ok, %{type: :heartbeat}} ->
        snapshot_membership_loop(pid, acc)

      other ->
        flunk("expected subscription snapshot/caught_up, got #{inspect(other)}")
    end
  end

  defp assert_receive_subscription_upsert!(pid, id) do
    case Subscriptions.next(pid, 5_000) do
      {:ok, %{type: type, document: %{id: ^id}}} when type in [:upsert, :snapshot] ->
        :ok

      {:ok, %{type: :caught_up}} ->
        assert_receive_subscription_upsert!(pid, id)

      {:ok, %{type: type, document: %{id: _other}}} when type in [:upsert, :snapshot, :remove] ->
        assert_receive_subscription_upsert!(pid, id)

      other ->
        flunk("expected upsert for #{id}, got #{inspect(other)}")
    end
  end

  defp drain_to_caught_up(pid) do
    case Subscriptions.next(pid, 5_000) do
      {:ok, %{type: :caught_up} = event} -> {:ok, event}
      {:ok, _} -> drain_to_caught_up(pid)
      other -> other
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
      timeout: 10_000,
      message: "attachment GC did not become idle"
    )
  end

  defp await_replication_active(uuid, job_id) do
    Eventual.eventually(
      fn ->
        case JobManager.get(uuid, job_id) do
          {:ok, %{state: state}}
          when state in [:waiting, :backoff, :handshake, :read_changes, :idle, :transfer] ->
            true

          _ ->
            false
        end
      end,
      timeout: 10_000,
      message: "continuous replication job did not become active"
    )
  end

  defp await_replication_doc(uuid, id) do
    Eventual.eventually(
      fn ->
        case ElixirDB.Documents.get(uuid, %{id: id}) do
          {:ok, _} -> true
          _ -> false
        end
      end,
      timeout: 30_000,
      message: "document #{id} did not replicate to #{uuid}"
    )
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

  defp create_database!(server, path) do
    {:ok, resp} = Req.post(server.base_url <> "/v1/databases", json: %{"path" => path})
    assert resp.status == 201
    resp.body["data"]["database_uuid"]
  end

  defp create_index!(server, uuid) do
    {:ok, resp} =
      Req.post(server.base_url <> "/v1/databases/#{uuid}/indexes",
        json: %{
          "name" => "by-type",
          "type" => "structured",
          "fields" => [%{"path" => "/type", "type" => "string", "direction" => "asc"}]
        }
      )

    assert resp.status == 201
    resp.body["data"]["index_id"]
  end

  defp put_document!(server, uuid, id, body, attachments \\ nil) do
    payload = %{"id" => id, "body" => body}
    payload = if attachments, do: Map.put(payload, "attachments", attachments), else: payload

    {:ok, resp} =
      Req.post(server.base_url <> "/v1/databases/#{uuid}/documents/put", json: payload)

    %{status: resp.status, body: resp.body}
  end

  defp query!(server, uuid, request) do
    {:ok, resp} =
      Req.post(server.base_url <> "/v1/databases/#{uuid}/query", json: request)

    %{status: resp.status, body: resp.body}
  end

  defp upload_attachment!(server, uuid, bytes) do
    {:ok, resp} =
      Req.post(server.base_url <> "/v1/databases/#{uuid}/attachments/upload",
        body: bytes,
        headers: [{"content-type", "application/octet-stream"}]
      )

    assert resp.status == 201
    resp.body["data"]["blob"]
  end
end
