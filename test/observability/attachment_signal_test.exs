defmodule VialKeeper.Observability.AttachmentSignalTest do
  @moduledoc """
  Attachment OTel signals: upload/download/gc spans and privacy (no digests,
  names, paths, document ids, or bodies).
  """
  use VialKeeper.Observability.OtelCase, async: false

  @moduletag :integration

  alias VialKeeper.Attachments
  alias VialKeeper.Documents
  alias VialKeeper.Eventual
  alias VialKeeper.Observability.{TestExporter, TestMetricExporter}
  alias VialKeeper.Runtime.DatabaseCatalog

  @body_secret "ATT_BODY_SENTINEL_x9y8z7"
  @doc_id "att-obs-doc-sentinel-4242"
  @att_name "secret-name-sentinel.bin"

  setup do
    rel = "obs-attachment-#{System.unique_integer([:positive])}.vialkeeper"
    abs = Path.join(VialKeeper.Config.database_root(), rel)
    VialKeeper.TempDatabase.cleanup(abs)

    {:ok, %{database_uuid: uuid}} = DatabaseCatalog.create(rel)
    assert {:ok, _} = DatabaseCatalog.open(uuid)

    on_exit(fn ->
      _ = DatabaseCatalog.close(uuid)
      _ = DatabaseCatalog.unregister(uuid)
      VialKeeper.TempDatabase.cleanup(abs)
    end)

    [uuid: uuid, abs: abs]
  end

  test "upload and download emit spans/metrics without leaking private fields", %{
    uuid: uuid,
    abs: abs
  } do
    assert {:ok, %{blob: digest, length: length}} =
             Attachments.upload_stream(uuid, [@body_secret])

    assert {:ok, _} =
             Documents.put(uuid, %{
               "id" => @doc_id,
               "body" => %{"v" => 1},
               "attachments" => %{
                 @att_name => %{"blob" => digest, "content_type" => "application/octet-stream"}
               }
             })

    assert {:ok, stream} =
             Attachments.open_stream(uuid, %{"id" => @doc_id, "name" => @att_name})

    assert IO.iodata_to_binary(Enum.to_list(stream.body)) == @body_secret
    stream.close.()

    Eventual.eventually(
      fn ->
        TestMetricExporter.counter_sum("vial_keeper.attachment.write.count", %{
          :"db.uuid" => uuid,
          :outcome => :ok
        }) >= 1
      end,
      timeout: 2_000,
      message: "attachment write counter missing"
    )

    Eventual.eventually(
      fn ->
        TestMetricExporter.datapoints("vial_keeper.attachment.store.phase.duration") != []
      end,
      timeout: 2_000,
      message: "attachment store phase histogram missing"
    )

    Eventual.eventually(
      fn ->
        TestMetricExporter.datapoints("vial_keeper.attachment.upload.phase.duration") != []
      end,
      timeout: 2_000,
      message: "attachment upload phase histogram missing"
    )

    store_phases =
      for datapoint <- TestMetricExporter.datapoints("vial_keeper.attachment.store.phase.duration"),
          phase = metric_attr(datapoint[:attributes], :attachment_phase),
          not is_nil(phase),
          do: phase

    assert :logical_hash in store_phases
    assert :compression_probe in store_phases
    assert :file_sync in store_phases
    assert :cas_install in store_phases

    assert Enum.all?(
             store_phases,
             &(&1 in VialKeeper.Observability.Instrumentation.AttachmentStore.phases())
           )

    upload_phases =
      for datapoint <-
            TestMetricExporter.datapoints("vial_keeper.attachment.upload.phase.duration"),
          phase = metric_attr(datapoint[:attributes], :attachment_phase),
          not is_nil(phase),
          do: phase

    assert :coordinator_wait in upload_phases
    assert :physical_store in upload_phases
    assert :pending_protection in upload_phases

    assert Enum.all?(
             upload_phases,
             &(&1 in VialKeeper.Observability.Instrumentation.AttachmentUpload.phases())
           )

    Eventual.eventually(
      fn ->
        TestMetricExporter.counter_sum("vial_keeper.attachment.read.count", %{
          :"db.uuid" => uuid,
          :outcome => :ok
        }) >= 1
      end,
      timeout: 2_000,
      message: "attachment read counter missing"
    )

    write_spans = TestExporter.spans_named("vial_keeper.attachment.write")
    read_spans = TestExporter.spans_named("vial_keeper.attachment.read")
    assert write_spans != []
    assert read_spans != []

    assert Enum.any?(write_spans, fn span ->
             TestExporter.span_attr(span, :logical_bytes) == length
           end)

    forbidden = [@body_secret, @doc_id, @att_name, digest, abs]
    assert_no_leaks(write_spans ++ read_spans, forbidden)

    assert {:ok, stats} = Attachments.gc(uuid)
    assert is_integer(stats.deleted)

    Eventual.eventually(
      fn ->
        TestMetricExporter.counter_sum("vial_keeper.attachment.gc.count", %{
          :"db.uuid" => uuid
        }) >= 1
      end,
      timeout: 2_000,
      message: "attachment gc counter missing"
    )

    gc_spans = TestExporter.spans_named("vial_keeper.attachment.gc")
    assert gc_spans != []
    assert_no_leaks(gc_spans, forbidden)

    metric_names = [
      "vial_keeper.attachment.write.count",
      "vial_keeper.attachment.read.count",
      "vial_keeper.attachment.upload.phase.duration",
      "vial_keeper.attachment.store.phase.duration",
      "vial_keeper.attachment.gc.count"
    ]

    metric_attrs =
      for name <- metric_names,
          dp <- TestMetricExporter.datapoints(name),
          value <- metric_attr_values(dp[:attributes]),
          do: value

    metric_leaks =
      for value <- metric_attrs,
          secret <- forbidden,
          String.contains?(inspect(value), secret),
          do: secret

    assert metric_leaks == [],
           "forbidden values leaked into metric attributes: #{inspect(metric_leaks)}"
  end

  defp assert_no_leaks(spans, forbidden) do
    leaks =
      for span <- spans,
          value <- span_attr_values(span[:attributes]),
          secret <- forbidden,
          String.contains?(inspect(value), secret) do
        {span[:name], secret}
      end

    assert leaks == [],
           "forbidden values leaked into span attributes: #{inspect(leaks)}"
  end

  defp span_attr_values({:attributes, _, _, _, map}) when is_map(map), do: Map.values(map)
  defp span_attr_values(map) when is_map(map), do: Map.values(map)
  defp span_attr_values(_), do: []

  defp metric_attr_values({:attributes, _, _, _, map}) when is_map(map), do: Map.values(map)
  defp metric_attr_values(map) when is_map(map), do: Map.values(map)
  defp metric_attr_values(_), do: []

  defp metric_attr({:attributes, _, _, _, map}, key) when is_map(map), do: Map.get(map, key)
  defp metric_attr(map, key) when is_map(map), do: Map.get(map, key)
  defp metric_attr(_attributes, _key), do: nil
end
