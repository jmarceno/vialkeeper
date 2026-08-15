defmodule VialKeeper.Replication.CompactUnderLagTest do
  @moduledoc """
  Exercises continuous replication while retention compaction runs under
  deterministic endpoint delay and bounded transfer pressure.
  """

  use VialKeeper.Observability.OtelCase, async: false

  @moduletag :integration

  alias VialKeeper.ChaosEndpoint
  alias VialKeeper.Documents
  alias VialKeeper.Eventual
  alias VialKeeper.MapAccess
  alias VialKeeper.Observability.TestMetricExporter
  alias VialKeeper.Replication.Id
  alias VialKeeper.Replication.LocalEndpoint
  alias VialKeeper.Replication.Worker
  alias VialKeeper.Runtime.AttachmentCoordinator
  alias VialKeeper.Runtime.DatabaseCatalog

  @chaos_weights %{pass: 40, delay: 60}
  @delay_ms 2..5
  @queue_limit 100
  @retention_config %{
    "retention" => %{
      "mode" => "stable_frontier",
      "history_depth" => 0,
      "peer_expiry_ms" => 86_400_000,
      "schedule" => "disabled"
    }
  }

  setup do
    prefix = "compact-under-lag-#{System.unique_integer([:positive])}"
    source_path = prefix <> "-source.vialkeeper"
    target_path = prefix <> "-target.vialkeeper"
    root = VialKeeper.Config.database_root()

    for path <- [source_path, target_path] do
      VialKeeper.TempDatabase.cleanup(Path.join(root, path))
    end

    assert {:ok, source_identity} = DatabaseCatalog.create(source_path)
    assert {:ok, target_identity} = DatabaseCatalog.create(target_path)

    assert {:ok, _} =
             DatabaseCatalog.command(
               source_identity.database_uuid,
               {:command, :update_config, @retention_config}
             )

    on_exit(fn ->
      for {identity, path} <- [
            {source_identity, source_path},
            {target_identity, target_path}
          ] do
        _ = DatabaseCatalog.close(identity.database_uuid)
        _ = DatabaseCatalog.unregister(identity.database_uuid)
        VialKeeper.TempDatabase.cleanup(Path.join(root, path))
      end
    end)

    {:ok, source_uuid: source_identity.database_uuid, target_uuid: target_identity.database_uuid}
  end

  test "compaction during a blocked lagging transfer converges directly or through bootstrap",
       context do
    %{source_uuid: source_uuid, target_uuid: target_uuid} = context
    initial = write_documents(source_uuid, "initial", 4)
    {source, target} = delayed_endpoints(source_uuid, target_uuid, 101)
    replication_id = replication_id!(source_uuid, target_uuid)
    seed_checkpoints!(source_uuid, target_uuid, replication_id)

    {initial_worker, initial_monitor} =
      start_continuous_worker(source, target, replication_id, batch: 100, batch_documents: 1)

    await_convergence(initial_worker, source_uuid, target_uuid, replication_id, initial)
    initial_floor = current_sequence(source_uuid)
    assert initial_floor > 0
    await_peer_safe(source_uuid, target_uuid, initial_floor)
    stop_worker(initial_worker, initial_monitor)

    updated = update_documents(source_uuid, initial)

    {history_worker, history_monitor} =
      start_continuous_worker(source, target, replication_id, batch: 100, batch_documents: 1)

    await_convergence(history_worker, source_uuid, target_uuid, replication_id, updated)
    safe_floor = current_sequence(source_uuid)
    assert safe_floor > initial_floor
    await_peer_safe(source_uuid, target_uuid, safe_floor)
    stop_worker(history_worker, history_monitor)

    backlog = write_documents(source_uuid, "backlog", 8)
    before_compaction = source_identity(source_uuid)
    parent = self()
    barrier = make_ref()
    {:ok, phases} = Agent.start_link(fn -> [] end)
    {:ok, blocked?} = Agent.start_link(fn -> false end)

    phase_hook = fn phase, phase_context ->
      Agent.update(phases, &[phase_record(phase, phase_context) | &1])

      if phase == :before_chain_fetch and claim_first_barrier(blocked?) do
        send(parent, {:lag_barrier, barrier, self()})

        receive do
          {:release_lag_barrier, ^barrier} -> :ok
        after
          15_000 -> {:error, VialKeeper.Error.internal_error("lag barrier was not released")}
        end
      else
        :ok
      end
    end

    {worker, monitor} =
      start_continuous_worker(source, target, replication_id,
        batch: 100,
        batch_documents: 1,
        phase_hook: phase_hook,
        state_notify: self()
      )

    assert_receive {:lag_barrier, ^barrier, transfer_task}, 10_000
    assert Process.alive?(worker)
    assert {:transfer, _data} = :sys.get_state(worker)

    lagging_sequence = checkpoint_sequence(target_uuid, replication_id)
    source_sequence = current_sequence(source_uuid)
    assert lagging_sequence == safe_floor
    assert lagging_sequence < source_sequence

    writer = Task.async(fn -> write_documents(source_uuid, "during-compact", 8) end)

    compactor =
      Task.async(fn ->
        Enum.map(1..4, fn _operation ->
          DatabaseCatalog.command(source_uuid, {:command, :compact_retention, %{}})
        end)
      end)

    concurrent_writes = Task.await(writer, 10_000)
    compactions = Task.await(compactor, 10_000)

    assert Enum.all?(compactions, &match?({:ok, %{new_floor: _}}, &1))
    applied = applied_compaction!(compactions)
    assert applied.old_floor == 0
    assert applied.new_floor == safe_floor
    assert applied.new_floor > 0
    assert applied.new_compaction_epoch > applied.old_compaction_epoch
    assert applied.new_compaction_epoch > MapAccess.get(before_compaction, :compaction_epoch, 0)
    assert applied.removed_changes > 0
    assert applied.removed_revisions > 0

    compacted_identity = source_identity(source_uuid)
    assert MapAccess.get(compacted_identity, :retention_floor_sequence) == applied.new_floor
    assert MapAccess.get(compacted_identity, :compaction_epoch) == applied.new_compaction_epoch
    assert is_binary(MapAccess.get(compacted_identity, :retention_boundary_digest))
    assert_compaction_boundary(source_uuid, compacted_identity)

    assert {:error, %VialKeeper.Error{code: :history_truncated}} =
             DatabaseCatalog.command(
               source_uuid,
               {:command, :read_changes, %{since: 0, limit: 100}}
             )

    await_attachment_gc(source_uuid)
    send(transfer_task, {:release_lag_barrier, barrier})
    expected = updated |> Map.merge(backlog) |> Map.merge(concurrent_writes)

    await_convergence(worker, source_uuid, target_uuid, replication_id, expected, phases)

    records = phases |> Agent.get(&Enum.reverse/1)
    assert Enum.any?(records, &match?({:before_chain_fetch, _}, &1))
    assert_observed_terminal_path(records)
    assert queue_length(worker) < @queue_limit
    assert Process.alive?(worker)
    refute_received {:DOWN, ^monitor, :process, ^worker, _reason}
    assert_delay_only_fired(source, target)

    refute :failed in collect_worker_states()
    stop_worker(worker, monitor)
  end

  test "exported transfer peaks bound bytes and concurrency without an in-flight gauge", context do
    %{source_uuid: source_uuid, target_uuid: target_uuid} = context
    initial = write_documents_with_attachments(source_uuid, "bounded", 8)
    {source, target} = delayed_endpoints(source_uuid, target_uuid, 303)
    replication_id = replication_id!(source_uuid, target_uuid)
    seed_checkpoints!(source_uuid, target_uuid, replication_id)
    parent = self()
    barrier = make_ref()
    barrier_count = 8
    max_chain_concurrency = 2
    max_blob_concurrency = 2
    max_transfer_bytes = 16_384
    {:ok, barrier_ordinal} = Agent.start_link(fn -> 0 end)

    phase_hook = fn
      :before_chain_fetch, _phase_context ->
        ordinal =
          Agent.get_and_update(barrier_ordinal, fn current -> {current + 1, current + 1} end)

        if ordinal <= barrier_count do
          send(parent, {:pressure_barrier, barrier, ordinal, self()})

          receive do
            {:release_pressure_barrier, ^barrier, ^ordinal} -> :ok
          after
            15_000 -> {:error, VialKeeper.Error.internal_error("pressure barrier was not released")}
          end
        else
          :ok
        end

      _phase, _phase_context ->
        :ok
    end

    {worker, monitor} =
      start_continuous_worker(source, target, replication_id,
        batch: 4,
        batch_documents: 1,
        max_concurrent_chain_fetches: max_chain_concurrency,
        max_concurrent_blob_transfers: max_blob_concurrency,
        max_transfer_bytes_in_flight: max_transfer_bytes,
        phase_hook: phase_hook
      )

    {lag_writes, queue_samples} =
      Enum.reduce(1..barrier_count, {%{}, []}, fn operation, {writes, samples} ->
        assert_receive {:pressure_barrier, ^barrier, ordinal, transfer_task}, 10_000
        assert ordinal in 1..barrier_count
        assert Process.alive?(worker)

        write = write_documents(source_uuid, "sustained-lag-#{operation}", 1)
        queue_sample = queue_length(worker)
        send(transfer_task, {:release_pressure_barrier, barrier, ordinal})

        {Map.merge(writes, write), [queue_sample | samples]}
      end)

    expected = Map.merge(initial, lag_writes)
    await_convergence(worker, source_uuid, target_uuid, replication_id, expected)

    queue_samples = [queue_length(worker) | queue_samples]
    assert Enum.all?(queue_samples, &(&1 < @queue_limit))

    datapoints =
      Eventual.eventually(
        fn ->
          points =
            TestMetricExporter.datapoints_matching(
              "vial_keeper.replication.transfer.duration",
              %{:"replication.id" => replication_id}
            )

          if points == [], do: false, else: {:ok, points}
        end,
        timeout: 8_000,
        message: "bounded transfer peak metrics were not exported"
      )

    assert Enum.all?(datapoints, fn datapoint ->
             metric_bound?(datapoint, :max_chain_concurrency_observed, max_chain_concurrency) and
               metric_bound?(datapoint, :max_blob_concurrency_observed, max_blob_concurrency) and
               metric_bound?(datapoint, :peak_reserved_transfer_bytes, max_transfer_bytes)
           end)

    assert Enum.any?(
             datapoints,
             &(TestMetricExporter.datapoint_attr(&1, :peak_reserved_transfer_bytes) > 0)
           )

    assert Enum.any?(
             datapoints,
             &(TestMetricExporter.datapoint_attr(&1, :max_chain_concurrency_observed) > 0)
           )

    assert_delay_only_fired(source, target)
    assert Process.alive?(worker)
    refute_received {:DOWN, ^monitor, :process, ^worker, _reason}
    stop_worker(worker, monitor)
  end

  defp delayed_endpoints(source_uuid, target_uuid, seed) do
    assert {:ok, source} = LocalEndpoint.new(source_uuid)
    assert {:ok, target} = LocalEndpoint.new(target_uuid)

    {
      ChaosEndpoint.wrap(source,
        seed: seed,
        weights: @chaos_weights,
        delay_ms: @delay_ms
      ),
      ChaosEndpoint.wrap(target,
        seed: seed + 1,
        weights: @chaos_weights,
        delay_ms: @delay_ms
      )
    }
  end

  defp replication_id!(source_uuid, target_uuid) do
    assert {:ok, replication_id} =
             Id.calculate(source_uuid, target_uuid, "push", "continuous")

    replication_id
  end

  defp seed_checkpoints!(source_uuid, target_uuid, replication_id) do
    :ok =
      VialKeeper.TestReplicationCheckpoint.seed_matching_checkpoints!(
        source_uuid,
        target_uuid,
        replication_id
      )
  end

  defp await_peer_safe(source_uuid, peer_uuid, expected_sequence) do
    Eventual.eventually(
      fn -> peer_safe_sequence(source_uuid, peer_uuid) == expected_sequence end,
      timeout: 5_000,
      message: "initial replication did not establish a positive peer safe floor"
    )
  end

  defp peer_safe_sequence(source_uuid, peer_uuid) do
    case DatabaseCatalog.command(
           source_uuid,
           {:command, :get_local_record, "peer_ledger", peer_uuid}
         ) do
      {:ok, record} when is_map(record) ->
        record |> MapAccess.get(:value, %{}) |> MapAccess.get(:safe_source_sequence)

      _ ->
        nil
    end
  end

  defp applied_compaction!(compactions) do
    case Enum.find_value(compactions, fn
           {:ok, %{noop?: false} = result} -> result
           _ -> nil
         end) do
      nil -> flunk("retention compaction never performed positive-floor work")
      result -> result
    end
  end

  defp assert_compaction_boundary(source_uuid, identity) do
    assert {:ok, endpoint} = LocalEndpoint.new(source_uuid)
    assert {:ok, page} = LocalEndpoint.read_boundary_pages(endpoint, %{})
    assert MapAccess.get(page, :compaction_epoch) == MapAccess.get(identity, :compaction_epoch)

    assert MapAccess.get(page, :boundary_digest) ==
             MapAccess.get(identity, :retention_boundary_digest)
  end

  defp start_continuous_worker(source, target, replication_id, options) do
    worker_options =
      options
      |> Map.new()
      |> Map.merge(%{
        source: source,
        target: target,
        replication_id: replication_id,
        mode: "continuous",
        direction: "push",
        wait_ms: 5,
        retry: %{base_delay_ms: 1, max_delay_ms: 5, jitter_ms: 0, max_attempts: 16}
      })

    assert {:ok, worker} = Worker.start_link(worker_options)
    monitor = Process.monitor(worker)
    :gen_statem.cast(worker, :start)
    {worker, monitor}
  end

  defp write_documents(uuid, prefix, count) do
    Map.new(1..count, fn ordinal ->
      id = "#{prefix}-#{ordinal}"
      body = %{"prefix" => prefix, "ordinal" => ordinal}
      revision = put_document_eventually(uuid, id, body)
      {id, %{revision: revision, body: body}}
    end)
  end

  defp update_documents(uuid, documents) do
    Map.new(documents, fn {id, %{revision: previous}} ->
      body = %{"updated" => true, "document_id" => id}

      assert {:ok, %{revision: revision}} =
               Documents.put(uuid, %{id: id, if_revision: previous, body: body})

      {id, %{revision: revision, body: body}}
    end)
  end

  defp put_document_eventually(uuid, id, body) do
    result =
      Eventual.eventually(
        fn -> retryable_put(uuid, id, body) end,
        timeout: 5_000,
        message: "source write remained blocked by compaction"
      )

    assert {:ok, %{revision: revision}} = result
    revision
  end

  defp retryable_put(uuid, id, body) do
    case Documents.put(uuid, %{id: id, body: body}) do
      {:error, %VialKeeper.Error{retryable: true}} -> false
      result -> {:ok, result}
    end
  end

  defp write_documents_with_attachments(uuid, prefix, count) do
    Map.new(1..count, fn ordinal ->
      payload = String.duplicate("payload-#{ordinal}-", 256)
      assert {:ok, %{blob: digest}} = VialKeeper.Attachments.upload_stream(uuid, [payload])
      id = "#{prefix}-#{ordinal}"
      body = %{"prefix" => prefix, "ordinal" => ordinal}

      assert {:ok, %{revision: revision}} =
               Documents.put(uuid, %{
                 id: id,
                 body: body,
                 attachments: %{
                   "payload.bin" => %{
                     blob: digest,
                     content_type: "application/octet-stream"
                   }
                 }
               })

      {id, %{revision: revision, body: body}}
    end)
  end

  defp await_convergence(
         worker,
         source_uuid,
         target_uuid,
         replication_id,
         expected,
         phases \\ nil
       ) do
    condition = fn ->
      Process.alive?(worker) and
        documents_converged?(source_uuid, target_uuid, expected) and
        checkpoints_aligned?(source_uuid, target_uuid, replication_id)
    end

    case Eventual.await(condition, timeout: 20_000) do
      :ok ->
        :ok

      :timeout ->
        flunk(
          "continuous replication did not converge: " <>
            inspect(%{
              worker_alive: Process.alive?(worker),
              worker_state: worker_state(worker),
              source_sequence: current_sequence(source_uuid),
              source_checkpoint: checkpoint_value(source_uuid, replication_id),
              target_checkpoint: checkpoint_value(target_uuid, replication_id),
              missing_documents: missing_documents(source_uuid, target_uuid, expected),
              phases: if(phases, do: Agent.get(phases, &Enum.reverse/1), else: nil)
            })
        )
    end
  end

  defp await_attachment_gc(uuid) do
    Eventual.eventually(
      fn ->
        case AttachmentCoordinator.status(uuid) do
          %{gc_barrier: false, gc_active: false, gc_queued: false, gc_scheduled: false} -> true
          _ -> false
        end
      end,
      timeout: 5_000,
      message: "attachment GC after compaction did not become idle"
    )
  end

  defp worker_state(worker) do
    if Process.alive?(worker), do: :sys.get_state(worker), else: :stopped
  catch
    :exit, _reason -> :stopped
  end

  defp missing_documents(source_uuid, target_uuid, expected) do
    Enum.reject(expected, fn {id, document} ->
      documents_converged?(source_uuid, target_uuid, %{id => document})
    end)
    |> Enum.map(&elem(&1, 0))
  end

  defp documents_converged?(source_uuid, target_uuid, expected) do
    expected_leaves =
      Map.new(expected, fn {id, document} -> {id, MapSet.new([document.revision])} end)

    documents_match? =
      Enum.all?(expected, fn {id, %{revision: revision, body: body}} ->
        with {:ok, source_document} <- Documents.get(source_uuid, %{id: id}),
             {:ok, target_document} <- Documents.get(target_uuid, %{id: id}) do
          MapAccess.get(source_document, :revision) == revision and
            MapAccess.get(target_document, :revision) == revision and
            MapAccess.get(source_document, :body) == body and
            MapAccess.get(target_document, :body) == body
        else
          _ -> false
        end
      end)

    documents_match? and target_leaf_map(target_uuid) == expected_leaves
  end

  defp target_leaf_map(uuid) do
    case VialKeeper.Changes.read(uuid, %{since: 0, limit: 500}) do
      {:ok, %{results: changes}} ->
        Map.new(changes, fn change ->
          leaves =
            change
            |> MapAccess.get(:leaf_revisions, [])
            |> Enum.map(&MapAccess.get(&1, :revision))
            |> Enum.reject(&is_nil/1)
            |> MapSet.new()

          {MapAccess.get(change, :document_id), leaves}
        end)

      _ ->
        %{}
    end
  end

  defp checkpoints_aligned?(source_uuid, target_uuid, replication_id) do
    source_checkpoint = checkpoint_value(source_uuid, replication_id)
    target_checkpoint = checkpoint_value(target_uuid, replication_id)
    sequence = current_sequence(source_uuid)
    keys = checkpoint_alignment_keys()

    is_map(source_checkpoint) and is_map(target_checkpoint) and
      MapAccess.get(source_checkpoint, :source_sequence) == sequence and
      Enum.all?(keys, fn key ->
        MapAccess.get(source_checkpoint, key) == MapAccess.get(target_checkpoint, key)
      end)
  end

  defp checkpoint_alignment_keys do
    [
      :source_sequence,
      :source_history_epoch,
      :source_compaction_epoch,
      :safe_source_sequence,
      :installed_source_compaction_epoch
    ]
  end

  defp checkpoint_sequence(uuid, replication_id) do
    uuid
    |> checkpoint_value(replication_id)
    |> MapAccess.get(:source_sequence, 0)
  end

  defp checkpoint_value(uuid, replication_id) do
    case DatabaseCatalog.command(
           uuid,
           {:command, :get_local_record, "checkpoints", replication_id}
         ) do
      {:ok, record} when is_map(record) -> MapAccess.get(record, :value)
      _ -> nil
    end
  end

  defp current_sequence(uuid) do
    uuid |> source_identity() |> MapAccess.get(:current_sequence)
  end

  defp source_identity(uuid) do
    assert {:ok, identity} = DatabaseCatalog.command(uuid, {:command, :identity, %{}})
    identity
  end

  defp claim_first_barrier(agent) do
    Agent.get_and_update(agent, fn
      false -> {true, true}
      true -> {false, true}
    end)
  end

  defp phase_record(phase, context)
       when phase in [:after_handshake, :after_waiting, :after_read_changes] do
    {phase,
     %{
       since: MapAccess.get(context, :since),
       terminal: MapAccess.get(context, :terminal),
       bootstrap_required: MapAccess.get(context, :bootstrap_required),
       reconcile_reason: MapAccess.get(context, :reconcile_reason)
     }}
  end

  defp phase_record(phase, _context), do: {phase, nil}

  defp bootstrap_reconcile_record?({phase, %{bootstrap_required: true, reconcile_reason: reason}})
       when phase in [:after_handshake, :after_waiting, :after_read_changes] and
              reason in [:ok, :epoch_mismatch, :below_floor, :no_common_history],
       do: true

  defp bootstrap_reconcile_record?(_record), do: false

  defp assert_observed_terminal_path(records) do
    if Enum.any?(records, &match?({:bootstrap, _}, &1)) do
      assert Enum.any?(records, &bootstrap_reconcile_record?/1)
      assert Enum.any?(records, &match?({:after_bootstrap, _}, &1))
    else
      for phase <- [:after_transfer, :after_import, :after_checkpoint_source, :after_report_peer] do
        assert Enum.any?(records, &match?({^phase, _}, &1)),
               "direct convergence did not record #{phase}"
      end
    end
  end

  defp metric_bound?(datapoint, attribute, configured_max) do
    value = TestMetricExporter.datapoint_attr(datapoint, attribute)
    is_integer(value) and value >= 0 and value <= configured_max
  end

  defp queue_length(worker) do
    case Process.info(worker, :message_queue_len) do
      {:message_queue_len, length} -> length
      nil -> @queue_limit
    end
  end

  defp assert_delay_only_fired(source, target) do
    stats = [ChaosEndpoint.stats(source), ChaosEndpoint.stats(target)]

    assert Enum.sum(Enum.map(stats, & &1.delay)) > 0

    assert Enum.all?(stats, fn counts ->
             counts.error == 0 and counts.duplicate == 0 and counts.reorder == 0
           end)
  end

  defp stop_worker(worker, monitor) do
    :gen_statem.cast(worker, :cancel)
    assert_receive {:DOWN, ^monitor, :process, ^worker, :normal}, 10_000
  end

  defp collect_worker_states(acc \\ []) do
    receive do
      {:replication_worker_state, state} -> collect_worker_states([state | acc])
    after
      0 -> Enum.reverse(acc)
    end
  end
end
