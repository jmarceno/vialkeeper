defmodule ElixirDB.Observability.ReplicationTraceTest do
  @moduledoc """
  Plan §7.2 / §9: replication trace continuity.

    * A one-shot local replication emits one `elixir_db.replication.batch`
      span wrapping the cycle, with both `elixir_db.replication.checkpoint`
      spans as children sharing the same trace_id (§6.2).
    * The batch duration histogram records the real `revisions_written`.
    * A one-shot REMOTE replication propagates the trace via W3C traceparent:
      the wire `elixir_db.http.request` spans on the target server share the
      worker's trace_id (§6.2, §9 two-server criterion — both servers run in
      this VM, so both span sets land in the same in-memory exporter).
  """

  use ElixirDB.Observability.OtelCase, async: false

  alias ElixirDB.Eventual
  alias ElixirDB.Observability.{TestExporter, TestMetricExporter}
  alias ElixirDB.Replication.Id
  alias ElixirDB.Replication.JobManager
  alias ElixirDB.Replication.LocalEndpoint
  alias ElixirDB.Replication.Worker
  alias ElixirDB.Runtime.DatabaseCatalog
  alias ElixirDB.TestServer

  setup do
    root = ElixirDB.Config.database_root()
    prefix = "obs-repl-#{System.unique_integer([:positive])}"
    a_path = prefix <> "-a.db"
    b_path = prefix <> "-b.db"

    for path <- [a_path, b_path] do
      ElixirDB.TempDatabase.cleanup(Path.join(root, path))
    end

    {:ok, a} = DatabaseCatalog.create(a_path)
    {:ok, b} = DatabaseCatalog.create(b_path)

    on_exit(fn ->
      for {identity, path} <- [{a, a_path}, {b, b_path}] do
        _ = disable_jobs(identity.database_uuid)
        _ = DatabaseCatalog.close(identity.database_uuid)
        _ = DatabaseCatalog.unregister(identity.database_uuid)
        ElixirDB.TempDatabase.cleanup(Path.join(root, path))
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
    assert {:ok, _} = ElixirDB.Documents.put(a.database_uuid, %{id: "doc", body: %{"n" => 1}})

    {:ok, source} = LocalEndpoint.new(a.database_uuid)
    {:ok, target} = LocalEndpoint.new(b.database_uuid)

    {:ok, replication_id} =
      Id.calculate(a.database_uuid, b.database_uuid, "push", "one_shot")

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
      TestExporter.spans_named("elixir_db.replication.batch")
      |> Enum.filter(fn s ->
        TestExporter.span_attr(s, :"replication.id") == replication_id
      end)

    checkpoint_spans =
      TestExporter.spans_named("elixir_db.replication.checkpoint")
      |> Enum.filter(fn s ->
        TestExporter.span_attr(s, :"replication.id") == replication_id
      end)

    assert [_] = batch_spans,
           "expected exactly one batch span, got: #{inspect(Enum.map(TestExporter.spans_named("elixir_db.replication.batch"), & &1[:span_id]))}"

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
        TestMetricExporter.datapoints("elixir_db.replication.batch.duration")
        |> Enum.any?(fn dp ->
          TestMetricExporter.datapoint_attr(dp, :"replication.id") == replication_id and
            TestMetricExporter.datapoint_attr(dp, :revisions_written) == 1
        end)
      end,
      timeout: 2_000,
      message: "batch.duration histogram never recorded revisions_written: 1"
    )
  end

  test "remote one-shot replication: wire spans on the target share the worker trace", %{
    a: a,
    b: b
  } do
    server_a = TestServer.start_supervised!()
    server_b = TestServer.start_supervised!()

    assert {:ok, _} = ElixirDB.Documents.put(a.database_uuid, %{id: "doc", body: %{"n" => 1}})

    {:ok, replication_id} =
      Id.calculate(a.database_uuid, b.database_uuid, "push", "one_shot")

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
      fn -> match?({:ok, _}, ElixirDB.Documents.get(b.database_uuid, %{id: "doc"})) end,
      timeout: 15_000,
      message: "replicated document never arrived on the target"
    )

    batch_spans =
      Eventual.eventually(
        fn ->
          case TestExporter.spans_named("elixir_db.replication.batch")
               |> Enum.filter(fn s ->
                 TestExporter.span_attr(s, :"replication.id") == replication_id
               end) do
            [] -> false
            spans -> {:ok, spans}
          end
        end,
        timeout: 5_000,
        message: "no replication batch span recorded for the remote one-shot job"
      )

    batch_traces = MapSet.new(batch_spans, & &1[:trace_id])

    # Wire requests land on the replication WIRE route module
    # (/replication/..., singular). Their spans must share the worker's trace —
    # proof the traceparent was injected outbound and extracted inbound (§6.2,
    # §9). Job-management routes (/replications) are excluded.
    wire_spans =
      TestExporter.spans_named("elixir_db.http.request")
      |> Enum.filter(fn s ->
        route = TestExporter.span_attr(s, :"http.route")
        is_binary(route) and String.contains?(route, "/replication/")
      end)

    assert wire_spans != [],
           "no http.request spans for replication wire routes; got routes: " <>
             "#{inspect(Enum.map(TestExporter.spans_named("elixir_db.http.request"), &TestExporter.span_attr(&1, :"http.route")))}"

    # Handshake wire calls (identity/get_checkpoint) happen BEFORE the batch
    # span starts (§5.6: the batch wraps read→diff→fetch→import→checkpoint),
    # so they are legitimately parentless roots. The post-handshake wire calls
    # (diff/import/checkpoint) must be parented by the extracted context and
    # share the worker's trace.
    {propagated, _roots} =
      Enum.split_with(wire_spans, &(&1[:parent_span_id] != :undefined))

    assert propagated != [],
           "no wire span was parented by an extracted trace context — outbound " <>
             "injection or inbound extraction is broken"

    for span <- propagated do
      assert MapSet.member?(batch_traces, span[:trace_id]),
             "wire span trace #{inspect(span[:trace_id])} is not any worker batch trace " <>
               "#{inspect(MapSet.to_list(batch_traces))}"
    end
  end
end
