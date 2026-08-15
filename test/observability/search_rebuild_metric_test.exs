defmodule VialKeeper.Observability.SearchRebuildMetricTest do
  @moduledoc "Covers full-text cache rebuild spans and metrics."

  use VialKeeper.Observability.OtelCase, async: false

  @moduletag :integration

  alias VialKeeper.Documents
  alias VialKeeper.Eventual
  alias VialKeeper.MapAccess
  alias VialKeeper.Observability.{TestExporter, TestMetricExporter}
  alias VialKeeper.Query
  alias VialKeeper.Runtime.DatabaseCatalog
  alias VialKeeper.Search
  alias VialKeeper.Storage.SQLite.Adapter

  @span "vial_keeper.search.rebuild"
  @count_metric "vial_keeper.search.rebuild.count"
  @duration_metric "vial_keeper.search.rebuild.duration"

  @fts_definition %{
    "name" => "titles",
    "type" => "full_text",
    "fields" => ["/title"],
    "tokenization" => %{"strategy" => "unicode_words_v1", "diacritics" => "preserve"}
  }

  setup do
    rel = "obs-search-rebuild-#{System.unique_integer([:positive])}.vialkeeper"
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

  test "create and rebuild emit search.rebuild with trigger and entries", %{uuid: uuid} do
    assert {:ok, _} = Documents.put(uuid, %{id: "doc", body: %{"title" => "hello world"}})

    assert {:ok, %{"index_id" => index_id}} = Query.create_index(uuid, @fts_definition)
    assert_rebuild_signal(uuid, :create, 1)

    TestExporter.reset()
    TestMetricExporter.reset()

    assert {:ok, %{rebuilt: true}} = Query.rebuild_index(uuid, index_id)
    assert_rebuild_signal(uuid, :rebuild, 1)
  end

  test "missing search cache rebuilds with cache_miss trigger" do
    {:ok, bundle_path} = VialKeeper.TempDatabase.create(prefix: "obs-search-miss")
    path = Path.join(bundle_path, "database.sqlite3")
    {:ok, adapter} = Adapter.create(path, %{})

    try do
      assert {:ok, _} =
               Adapter.apply_local_mutation(adapter, %{
                 operation: :put,
                 document_id: "doc",
                 body: %{"title" => "hello world"}
               })

      assert {:ok, %{"index_id" => _index_id}} = Adapter.create_index(adapter, @fts_definition)

      context = Adapter.to_context(adapter)
      assert :ok = Search.stop(context)
      persist = Path.join(context.bundle_root, "tmp/search-index.etf")
      _ = File.rm(persist)

      TestExporter.reset()
      TestMetricExporter.reset()

      assert {:ok, %{results: [%{id: "doc"}]}} =
               Adapter.execute_query(adapter, %{
                 search: %{index: "titles", text: "hello", mode: "all"},
                 limit: 10
               })

      uuid = MapAccess.get(context.identity, :database_uuid)
      assert is_binary(uuid)
      assert_rebuild_signal(uuid, :cache_miss, 1)
    after
      _ = Adapter.close(adapter)
      VialKeeper.TempDatabase.cleanup(bundle_path)
    end
  end

  defp assert_rebuild_signal(uuid, trigger, entries) do
    span =
      Eventual.eventually(
        fn ->
          TestExporter.spans_named(@span)
          |> Enum.find(fn span ->
            TestExporter.span_attr(span, :"db.uuid") == uuid and
              TestExporter.span_attr(span, :trigger) == trigger
          end)
        end,
        timeout: 2_000,
        message: "search.rebuild span missing for #{trigger}"
      )

    assert TestExporter.span_attr(span, :index_type) == :full_text
    assert TestExporter.span_attr(span, :outcome) == :ok
    assert TestExporter.span_attr(span, :entries) == entries
    refute String.contains?(inspect(span), "hello world")

    Eventual.eventually(
      fn ->
        TestMetricExporter.counter_sum(@count_metric, %{
          :"db.uuid" => uuid,
          :trigger => trigger,
          :outcome => :ok
        }) >= 1
      end,
      timeout: 2_000,
      message: "search.rebuild.count missing for #{trigger}"
    )

    Eventual.eventually(
      fn ->
        TestMetricExporter.datapoints_matching(@duration_metric, %{
          :"db.uuid" => uuid,
          :trigger => trigger
        }) != []
      end,
      timeout: 2_000,
      message: "search.rebuild.duration missing for #{trigger}"
    )
  end
end
