defmodule VialKeeper.Observability.SQLiteProbeTest do
  @moduledoc "Real-operation coverage for low-cardinality SQLite hotspot spans."

  use VialKeeper.Observability.OtelCase, async: false

  @moduletag :sqlite_physical
  @moduletag :integration

  alias VialKeeper.Observability.TestExporter
  alias VialKeeper.Storage.SQLite.Adapter

  setup do
    {:ok, bundle_path} = VialKeeper.TempDatabase.create(prefix: "vialkeeper-sqlite-probes")
    path = VialKeeper.TempDatabase.sqlite_path(bundle_path)
    {:ok, adapter} = Adapter.create(path, %{})

    on_exit(fn ->
      _ = Adapter.close(adapter)
      VialKeeper.TempDatabase.cleanup(bundle_path)
    end)

    {:ok, adapter: adapter}
  end

  test "document and bulk-write probes cover the measured write path", %{adapter: adapter} do
    assert {:ok, %{revision: revision}} = put_document(adapter, "read-doc", 1)

    TestExporter.reset()

    assert {:ok, %{body: %{"value" => 1}}} =
             Adapter.get_document(adapter, %{document_id: "read-doc"})

    assert [_] = TestExporter.spans_named("vial_keeper.sqlite.document.lookup")
    assert [] = TestExporter.spans_named("vial_keeper.sqlite.revision.lookup")

    TestExporter.reset()

    assert {:ok, %{body: %{"value" => 1}}} =
             Adapter.get_document(adapter, %{document_id: "read-doc", revision: revision})

    assert [_] = TestExporter.spans_named("vial_keeper.sqlite.document.lookup")
    assert [_] = TestExporter.spans_named("vial_keeper.sqlite.revision.lookup")

    TestExporter.reset()

    assert {:ok, results} =
             Adapter.apply_bulk_mutation(adapter, %{
               operations: [
                 %{operation: :put, document_id: "bulk-a", body: %{"value" => 2}},
                 %{operation: :put, document_id: "bulk-b", body: %{"value" => 3}}
               ]
             })

    assert [_, _] = results
    assert [prepare] = TestExporter.spans_named("vial_keeper.sqlite.mutation.bulk.prepare")
    assert [finalize] = TestExporter.spans_named("vial_keeper.sqlite.mutation.bulk.finalize")
    assert TestExporter.span_attr(prepare, :entries) == 2
    assert TestExporter.span_attr(finalize, :entries) == 2
    assert [_] = TestExporter.spans_named("vial_keeper.sqlite.transaction.begin")
    assert [_] = TestExporter.spans_named("vial_keeper.sqlite.transaction.commit")
  end

  test "changes probes separate metadata, SQL, decoding, and has-more work", %{
    adapter: adapter
  } do
    assert {:ok, _} = put_document(adapter, "change-a", 1)
    assert {:ok, _} = put_document(adapter, "change-b", 2)

    TestExporter.reset()

    assert {:ok, %{results: results, has_more: false}} =
             Adapter.read_changes(adapter, %{since: 0, limit: 10})

    assert [_, _] = results
    assert [_] = TestExporter.spans_named("vial_keeper.sqlite.changes.identity")
    assert [fetch] = TestExporter.spans_named("vial_keeper.sqlite.changes.fetch")
    assert [decode] = TestExporter.spans_named("vial_keeper.sqlite.changes.decode")
    assert [_] = TestExporter.spans_named("vial_keeper.sqlite.changes.has_more")
    assert TestExporter.span_attr(fetch, :entries) == 10
    assert TestExporter.span_attr(decode, :entries) == 2
  end

  test "indexed query probes separate planning, candidates, filtering, ordering, and projection", %{
    adapter: adapter
  } do
    assert {:ok, _} = put_document(adapter, "task", 1, "task")
    assert {:ok, _} = put_document(adapter, "note", 2, "note")

    assert {:ok, %{"index_id" => index_id}} =
             Adapter.create_index(adapter, %{
               "name" => "by-category",
               "type" => "structured",
               "fields" => [%{"path" => "/category", "type" => "string", "direction" => "asc"}]
             })

    TestExporter.reset()

    assert {:ok, %{results: [%{id: "task"}], selected_index: ^index_id}} =
             Adapter.execute_query(adapter, %{
               selector: %{"/category" => "task"},
               index: "by-category",
               limit: 10
             })

    expected_sqlite = [
      "vial_keeper.sqlite.query.prepare_request",
      "vial_keeper.sqlite.query.identity",
      "vial_keeper.sqlite.query.index_catalog",
      "vial_keeper.sqlite.query.candidates"
    ]

    Enum.each(expected_sqlite, fn name -> assert [_] = TestExporter.spans_named(name) end)

    assert [_] = TestExporter.spans_named("vial_keeper.query.execute")

    [candidates] = TestExporter.spans_named("vial_keeper.sqlite.query.candidates")
    assert TestExporter.span_attr(candidates, :plan_kind) == :single
    assert TestExporter.span_attr(candidates, :selected_index_count) == 1
  end

  defp put_document(adapter, document_id, value, category \\ nil) do
    body = %{"value" => value}
    body = if category, do: Map.put(body, "category", category), else: body

    Adapter.apply_local_mutation(adapter, %{
      operation: :put,
      document_id: document_id,
      body: body
    })
  end
end
