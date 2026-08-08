defmodule ElixirDB.Observability.AttachmentSignalTest do
  @moduledoc """
  Attachment OTel signals: upload/download/gc spans and privacy (no digests,
  names, paths, document ids, or bodies).
  """
  use ElixirDB.Observability.OtelCase, async: false

  alias ElixirDB.Attachments
  alias ElixirDB.Documents
  alias ElixirDB.Eventual
  alias ElixirDB.Observability.{TestExporter, TestMetricExporter}
  alias ElixirDB.Runtime.DatabaseCatalog

  @body_secret "ATT_BODY_SENTINEL_x9y8z7"
  @doc_id "att-obs-doc-sentinel-4242"
  @att_name "secret-name-sentinel.bin"

  setup do
    rel = "obs-attachment-#{System.unique_integer([:positive])}.elixirdb"
    abs = Path.join(ElixirDB.Config.database_root(), rel)
    ElixirDB.TempDatabase.cleanup(abs)

    {:ok, %{database_uuid: uuid}} = DatabaseCatalog.create(rel)
    assert {:ok, _} = DatabaseCatalog.open(uuid)

    on_exit(fn ->
      _ = DatabaseCatalog.close(uuid)
      _ = DatabaseCatalog.unregister(uuid)
      ElixirDB.TempDatabase.cleanup(abs)
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
        TestMetricExporter.counter_sum("elixir_db.attachment.write.count", %{
          :"db.uuid" => uuid,
          :outcome => :ok
        }) >= 1
      end,
      timeout: 2_000,
      message: "attachment write counter missing"
    )

    Eventual.eventually(
      fn ->
        TestMetricExporter.counter_sum("elixir_db.attachment.read.count", %{
          :"db.uuid" => uuid,
          :outcome => :ok
        }) >= 1
      end,
      timeout: 2_000,
      message: "attachment read counter missing"
    )

    write_spans = TestExporter.spans_named("elixir_db.attachment.write")
    read_spans = TestExporter.spans_named("elixir_db.attachment.read")
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
        TestMetricExporter.counter_sum("elixir_db.attachment.gc.count", %{
          :"db.uuid" => uuid
        }) >= 1
      end,
      timeout: 2_000,
      message: "attachment gc counter missing"
    )

    gc_spans = TestExporter.spans_named("elixir_db.attachment.gc")
    assert gc_spans != []
    assert_no_leaks(gc_spans, forbidden)

    metric_names = [
      "elixir_db.attachment.write.count",
      "elixir_db.attachment.read.count",
      "elixir_db.attachment.gc.count"
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
end
