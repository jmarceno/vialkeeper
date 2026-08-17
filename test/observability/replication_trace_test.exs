defmodule VialKeeper.Observability.ReplicationTraceTest do
  @moduledoc """
  Replication trace continuity.

    * A one-shot local replication emits one `vial_keeper.replication.batch`
      span wrapping the cycle, with both `vial_keeper.replication.checkpoint`
      spans as children sharing the same trace_id (§6.2).
    * The batch duration histogram records the real `revisions_written`.
    * A one-shot REMOTE replication propagates the trace via W3C traceparent:
      the wire `vial_keeper.http.request` spans on the target server share the
      worker's trace_id (§6.2, §9 two-server criterion — both servers run in
      this VM, so both span sets land in the same in-memory exporter).
  """

  use VialKeeper.Observability.OtelCase, async: false

  @moduletag :integration

  alias VialKeeper.Eventual
  alias VialKeeper.Observability.{TestExporter, TestMetricExporter}
  alias VialKeeper.Replication.Id
  alias VialKeeper.Replication.JobManager
  alias VialKeeper.Replication.LocalEndpoint
  alias VialKeeper.Replication.RemoteEndpoint
  alias VialKeeper.Replication.Worker
  alias VialKeeper.Runtime.DatabaseCatalog
  alias VialKeeper.TestServer

  setup do
    root = VialKeeper.Config.database_root()
    prefix = "obs-repl-#{System.unique_integer([:positive])}"
    a_path = prefix <> "-a.vialkeeper"
    b_path = prefix <> "-b.vialkeeper"

    for path <- [a_path, b_path] do
      VialKeeper.TempDatabase.cleanup(Path.join(root, path))
    end

    {:ok, a} = DatabaseCatalog.create(a_path)
    {:ok, b} = DatabaseCatalog.create(b_path)

    on_exit(fn ->
      for {identity, path} <- [{a, a_path}, {b, b_path}] do
        _ = disable_jobs(identity.database_uuid)
        _ = DatabaseCatalog.close(identity.database_uuid)
        _ = DatabaseCatalog.unregister(identity.database_uuid)
        VialKeeper.TempDatabase.cleanup(Path.join(root, path))
      end
    end)

    [a: a, b: b]
  end

  defp disable_jobs(uuid) do
    case JobManager.list(uuid) do
      {:ok, jobs} ->
        Enum.each(jobs, fn job ->
          _ = JobManager.disable(uuid, job.job_id)
        end)

      _ ->
        :ok
    end
  end

  test "local one-shot replication: batch and checkpoint spans share one trace", %{a: a, b: b} do
    assert {:ok, _} = VialKeeper.Documents.put(a.database_uuid, %{id: "doc", body: %{"n" => 1}})

    {:ok, source} = LocalEndpoint.new(a.database_uuid)
    {:ok, target} = LocalEndpoint.new(b.database_uuid)

    {:ok, replication_id} =
      Id.calculate(a.database_uuid, b.database_uuid, "push", "one_shot")

    :ok =
      VialKeeper.TestReplicationCheckpoint.seed_matching_checkpoints!(
        a.database_uuid,
        b.database_uuid,
        replication_id
      )

    options = %{
      source: source,
      target: target,
      replication_id: replication_id,
      mode: "one_shot",
      direction: "push"
    }

    assert {:ok, pid} = Worker.start_link(options)
    ref = Process.monitor(pid)
    :gen_statem.cast(pid, :start)
    assert_receive {:DOWN, ^ref, :process, ^pid, :normal}, 10_000

    batch_spans =
      TestExporter.spans_named("vial_keeper.replication.batch")
      |> Enum.filter(fn s ->
        TestExporter.span_attr(s, :"replication.id") == replication_id
      end)

    checkpoint_spans =
      TestExporter.spans_named("vial_keeper.replication.checkpoint")
      |> Enum.filter(fn s ->
        TestExporter.span_attr(s, :"replication.id") == replication_id
      end)

    assert [_] = batch_spans,
           "expected exactly one batch span, got: #{inspect(Enum.map(TestExporter.spans_named("vial_keeper.replication.batch"), & &1[:span_id]))}"

    batch = hd(batch_spans)

    # Both checkpoints (target + source CAS writes) are children of the batch
    # span in the same trace (§6.2 single trace_id).
    assert [_, _] = checkpoint_spans

    for span <- checkpoint_spans do
      assert span[:trace_id] == batch[:trace_id],
             "checkpoint span in trace #{inspect(span[:trace_id])} but batch in #{inspect(batch[:trace_id])}"

      assert span[:parent_span_id] == batch[:span_id],
             "checkpoint span must be a child of the batch span"
    end

    assert Enum.sort(Enum.map(checkpoint_spans, &TestExporter.span_attr(&1, :endpoint))) ==
             [:source, :target]

    # The batch duration histogram carries the real revisions_written (§5.6).
    Eventual.eventually(
      fn ->
        TestMetricExporter.datapoints("vial_keeper.replication.batch.duration")
        |> Enum.any?(fn dp ->
          TestMetricExporter.datapoint_attr(dp, :"replication.id") == replication_id and
            TestMetricExporter.datapoint_attr(dp, :revisions_written) == 1
        end)
      end,
      timeout: 8_000,
      message: "batch.duration histogram never recorded revisions_written: 1"
    )
  end

  test "remote one-shot replication: wire spans on the target share the worker trace", %{
    a: a,
    b: b
  } do
    server_a = TestServer.start_supervised!()
    server_b = TestServer.start_supervised!()

    assert {:ok, _} = VialKeeper.Documents.put(a.database_uuid, %{id: "doc", body: %{"n" => 1}})

    {:ok, replication_id} =
      Id.calculate(a.database_uuid, b.database_uuid, "push", "one_shot")

    :ok =
      VialKeeper.TestReplicationCheckpoint.seed_matching_checkpoints!(
        a.database_uuid,
        b.database_uuid,
        replication_id
      )

    assert {:ok, %{status: 201, body: %{"data" => %{"job_id" => _job_id}}}} =
             Req.post(server_a.base_url <> "/v1/databases/#{a.database_uuid}/replications",
               json: %{
                 "persist" => false,
                 "mode" => "one_shot",
                 "direction" => "push",
                 "enabled" => true,
                 "endpoint" => %{
                   "kind" => "remote",
                   "database_uuid" => b.database_uuid,
                   "base_url" => server_b.base_url
                 }
               }
             )

    # The one-shot job is unpersisted, so observe completion through its
    # effects: the document arrives on the target, then the batch span is
    # recorded (it ends after checkpoint_source).
    Eventual.eventually(
      fn -> match?({:ok, _}, VialKeeper.Documents.get(b.database_uuid, %{id: "doc"})) end,
      timeout: 15_000,
      message: "replicated document never arrived on the target"
    )

    batch_spans =
      Eventual.eventually(
        fn ->
          case TestExporter.spans_named("vial_keeper.replication.batch")
               |> Enum.filter(fn s ->
                 TestExporter.span_attr(s, :"replication.id") == replication_id
               end) do
            [] -> false
            spans -> {:ok, spans}
          end
        end,
        timeout: 15_000,
        message: "no replication batch span recorded for the remote one-shot job"
      )

    batch_traces = MapSet.new(batch_spans, & &1[:trace_id])

    # Wire requests land on the replication WIRE route module
    # (/replication/..., singular). Their spans must share the worker's trace —
    # proof the traceparent was injected outbound and extracted inbound (§6.2,
    # §9). Job-management routes (/replications) are excluded.
    wire_spans =
      TestExporter.spans_named("vial_keeper.http.request")
      |> Enum.filter(fn s ->
        route = TestExporter.span_attr(s, :"http.route")
        is_binary(route) and String.contains?(route, "/replication/")
      end)

    assert wire_spans != [],
           "no http.request spans for replication wire routes; got routes: " <>
             "#{inspect(Enum.map(TestExporter.spans_named("vial_keeper.http.request"), &TestExporter.span_attr(&1, :"http.route")))}"

    # Handshake wire calls (identity/get_checkpoint) happen BEFORE the batch
    # span starts (§5.6: the batch wraps read→diff→fetch→import→checkpoint),
    # so they are legitimately parentless roots. The post-handshake wire calls
    # (diff/import/checkpoint) must be parented by the extracted context and
    # share the worker's trace.
    batch_wire_spans =
      Enum.filter(wire_spans, &MapSet.member?(batch_traces, &1[:trace_id]))

    assert batch_wire_spans != [],
           "no wire span shared a worker batch trace — outbound injection or " <>
             "inbound extraction is broken; worker traces: #{inspect(MapSet.to_list(batch_traces))}"

    for span <- batch_wire_spans do
      assert span[:parent_span_id] != :undefined,
             "wire span in a worker batch trace must be parented by the extracted context"
    end
  end

  test "remote wire codec metrics are bounded and local endpoints emit none", %{a: a, b: b} do
    payload = String.duplicate("wire-metric-payload-", 512)

    assert {:ok, %{blob: digest}} =
             VialKeeper.Attachments.upload_stream(a.database_uuid, [payload])

    assert {:ok, _} =
             VialKeeper.Documents.put(a.database_uuid, %{
               id: "wire-metrics-doc",
               body: %{"ok" => true},
               attachments: %{
                 "file.bin" => %{blob: digest, content_type: "application/octet-stream"}
               }
             })

    # A local one-shot replication crosses no remote wire boundary.
    {:ok, source} = LocalEndpoint.new(a.database_uuid)
    {:ok, target} = LocalEndpoint.new(b.database_uuid)
    {:ok, replication_id} = Id.calculate(a.database_uuid, b.database_uuid, "push", "one_shot")

    :ok =
      VialKeeper.TestReplicationCheckpoint.seed_matching_checkpoints!(
        a.database_uuid,
        b.database_uuid,
        replication_id
      )

    # Periodic metric export can deliver a prior remote test's samples after
    # setup reset. Local endpoints must still emit no wire metrics.
    TestMetricExporter.reset()

    assert {:ok, pid} =
             Worker.start_link(%{
               source: source,
               target: target,
               replication_id: replication_id,
               mode: "one_shot",
               direction: "push"
             })

    ref = Process.monitor(pid)
    :gen_statem.cast(pid, :start)
    assert_receive {:DOWN, ^ref, :process, ^pid, :normal}, 10_000

    Eventual.eventually(
      fn -> TestMetricExporter.datapoints("vial_keeper.replication.batch.duration") != [] end,
      timeout: 8_000,
      message: "local batch duration was not exported"
    )

    assert TestMetricExporter.datapoints_matching("vial_keeper.replication.wire.bytes", %{
             endpoint_kind: :local
           }) == []

    assert TestMetricExporter.datapoints_matching(
             "vial_keeper.replication.wire.codec.duration",
             %{endpoint_kind: :local}
           ) == []

    # A remote one-shot replication records every JSON and blob boundary.
    server_b = TestServer.start_supervised!()
    c_path = "obs-repl-wire-#{System.unique_integer([:positive])}-c.vialkeeper"
    {:ok, c} = DatabaseCatalog.create(c_path)

    on_exit(fn ->
      _ = disable_jobs(c.database_uuid)
      _ = DatabaseCatalog.close(c.database_uuid)
      _ = DatabaseCatalog.unregister(c.database_uuid)
      VialKeeper.TempDatabase.cleanup(Path.join(VialKeeper.Config.database_root(), c_path))
    end)

    {:ok, remote_replication_id} =
      Id.calculate(a.database_uuid, c.database_uuid, "push", "one_shot")

    :ok =
      VialKeeper.TestReplicationCheckpoint.seed_matching_checkpoints!(
        a.database_uuid,
        c.database_uuid,
        remote_replication_id
      )

    {:ok, remote_target} =
      RemoteEndpoint.new(%{
        "kind" => "remote",
        "base_url" => server_b.base_url,
        "database_uuid" => c.database_uuid
      })

    assert {:ok, remote_pid} =
             Worker.start_link(%{
               source: source,
               target: remote_target,
               replication_id: remote_replication_id,
               mode: "one_shot",
               direction: "push"
             })

    remote_ref = Process.monitor(remote_pid)
    :gen_statem.cast(remote_pid, :start)
    assert_receive {:DOWN, ^remote_ref, :process, ^remote_pid, :normal}, 15_000

    assert {:ok, _} = VialKeeper.Documents.get(c.database_uuid, %{id: "wire-metrics-doc"})

    wire_datapoints =
      Eventual.eventually(
        fn ->
          datapoints = TestMetricExporter.datapoints("vial_keeper.replication.wire.bytes")
          kinds = MapSet.new(datapoints, &TestMetricExporter.datapoint_attr(&1, :payload_kind))

          if MapSet.subset?(MapSet.new([:json, :blob]), kinds), do: {:ok, datapoints}, else: false
        end,
        timeout: 8_000,
        message: "wire.bytes was not exported for both JSON and blob payloads"
      )

    for datapoint <- wire_datapoints do
      assert TestMetricExporter.datapoint_attr(datapoint, :endpoint_kind) == :remote
      assert TestMetricExporter.datapoint_attr(datapoint, :direction) in [:egress, :ingress]
      assert TestMetricExporter.datapoint_attr(datapoint, :payload_kind) in [:json, :blob]
      assert TestMetricExporter.datapoint_attr(datapoint, :encoding) in [:zstd, :raw]
    end

    blob_datapoints =
      Enum.filter(
        wire_datapoints,
        &(TestMetricExporter.datapoint_attr(&1, :payload_kind) == :blob)
      )

    assert Enum.any?(
             blob_datapoints,
             &(TestMetricExporter.datapoint_attr(&1, :encoding) == :zstd)
           )

    codec_datapoints =
      Eventual.eventually(
        fn ->
          datapoints =
            TestMetricExporter.datapoints("vial_keeper.replication.wire.codec.duration")

          operations = MapSet.new(datapoints, &TestMetricExporter.datapoint_attr(&1, :operation))

          if MapSet.subset?(MapSet.new([:compress, :decompress]), operations),
            do: {:ok, datapoints},
            else: false
        end,
        timeout: 8_000,
        message: "wire codec duration was not exported for compress and decompress"
      )

    wire_metric_text = inspect(wire_datapoints ++ codec_datapoints)

    for sentinel <- [digest, "wire-metrics-doc", "file.bin", server_b.base_url, "Bearer"] do
      refute wire_metric_text =~ sentinel,
             "privacy sentinel #{sentinel} appeared in wire metric attributes"
    end
  end

  test "transfer span records bounded measurements without private data", %{a: a, b: b} do
    assert {:ok, %{blob: digest}} =
             VialKeeper.Attachments.upload_stream(a.database_uuid, ["abc"])

    assert {:ok, _} =
             VialKeeper.Documents.put(a.database_uuid, %{
               id: "transfer-observability",
               body: %{"ok" => true},
               attachments: %{
                 "file.txt" => %{blob: digest, content_type: "text/plain"}
               }
             })

    {:ok, source} = LocalEndpoint.new(a.database_uuid)
    {:ok, target} = LocalEndpoint.new(b.database_uuid)
    {:ok, replication_id} = Id.calculate(a.database_uuid, b.database_uuid, "push", "one_shot")

    :ok =
      VialKeeper.TestReplicationCheckpoint.seed_matching_checkpoints!(
        a.database_uuid,
        b.database_uuid,
        replication_id
      )

    assert {:ok, pid} =
             Worker.start_link(%{
               source: source,
               target: target,
               replication_id: replication_id,
               mode: "one_shot",
               direction: "push"
             })

    ref = Process.monitor(pid)
    :gen_statem.cast(pid, :start)
    assert_receive {:DOWN, ^ref, :process, ^pid, :normal}, 10_000

    [transfer] =
      TestExporter.spans_named("vial_keeper.replication.transfer")
      |> Enum.filter(&(TestExporter.span_attr(&1, :"replication.id") == replication_id))

    [batch] =
      TestExporter.spans_named("vial_keeper.replication.batch")
      |> Enum.filter(&(TestExporter.span_attr(&1, :"replication.id") == replication_id))

    assert transfer[:trace_id] == batch[:trace_id]
    assert transfer[:parent_span_id] == batch[:span_id]

    [blob_transfer] =
      TestExporter.spans_named("vial_keeper.replication.blob.transfer")
      |> Enum.filter(&(TestExporter.span_attr(&1, :"replication.id") == replication_id))

    assert blob_transfer[:trace_id] == transfer[:trace_id]
    assert blob_transfer[:parent_span_id] == transfer[:span_id]

    for attribute <- [
          :chain_chunks,
          :max_chain_concurrency_observed,
          :blob_count,
          :max_blob_concurrency_observed,
          :logical_blob_bytes,
          :peak_reserved_transfer_bytes
        ] do
      value = TestExporter.span_attr(transfer, attribute)
      assert is_integer(value) and value >= 0
    end

    assert TestExporter.span_attr(transfer, :logical_blob_bytes) == 3

    for forbidden <- [
          :digest,
          :document_id,
          :attachment_name,
          :revision_id,
          :url,
          :pid,
          :ref
        ] do
      refute TestExporter.span_attr(transfer, forbidden),
             "forbidden transfer attribute #{forbidden} was emitted"
    end

    exported_transfer_spans =
      TestExporter.spans_named("vial_keeper.replication.transfer") ++
        TestExporter.spans_named("vial_keeper.replication.blob.transfer")

    for sentinel <- [digest, "transfer-observability", "file.txt"] do
      refute inspect(exported_transfer_spans) =~ sentinel,
             "privacy sentinel #{sentinel} appeared in transfer span attributes"
    end

    Eventual.eventually(
      fn ->
        duration_datapoints =
          TestMetricExporter.datapoints("vial_keeper.replication.transfer.duration")

        duration_datapoint = List.first(duration_datapoints)

        count_datapoints = TestMetricExporter.datapoints("vial_keeper.replication.transfer.count")

        is_map(duration_datapoint) and
          Enum.any?(count_datapoints, &((&1[:value] || 0) >= 1)) and
          Enum.all?(
            [
              :chain_chunks,
              :max_chain_concurrency_observed,
              :blob_count,
              :max_blob_concurrency_observed,
              :logical_blob_bytes,
              :peak_reserved_transfer_bytes
            ],
            &is_integer(TestMetricExporter.datapoint_attr(duration_datapoint, &1))
          )
      end,
      timeout: 8_000,
      message: "transfer count and duration measurements were not exported"
    )

    transfer_metric_text =
      (TestMetricExporter.datapoints("vial_keeper.replication.transfer.duration") ++
         TestMetricExporter.datapoints("vial_keeper.replication.transfer.count") ++
         TestMetricExporter.datapoints("vial_keeper.replication.blob.transfer.duration") ++
         TestMetricExporter.datapoints("vial_keeper.replication.blob.transfer.count"))
      |> inspect()

    for sentinel <- [digest, "transfer-observability", "file.txt"] do
      refute transfer_metric_text =~ sentinel,
             "privacy sentinel #{sentinel} appeared in transfer metric attributes"
    end
  end
end
