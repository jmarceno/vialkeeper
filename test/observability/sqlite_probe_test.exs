defmodule ElixirDB.Observability.SQLiteProbeTest do
  @moduledoc "Real-operation coverage for low-cardinality SQLite hotspot spans."

  use ElixirDB.Observability.OtelCase, async: false

  alias ElixirDB.Observability.TestExporter
  alias ElixirDB.Storage.SQLite.Adapter

  setup do
    {:ok, bundle_path} = ElixirDB.TempDatabase.create(prefix: "elixirdb-sqlite-probes")
    path = ElixirDB.TempDatabase.sqlite_path(bundle_path)
    {:ok, adapter} = Adapter.create(path, %{})

    on_exit(fn ->
      _ = Adapter.close(adapter)
      ElixirDB.TempDatabase.cleanup(bundle_path)
    end)

    {:ok, adapter: adapter}
  end

  test "document and bulk-write probes cover the measured write path", %{adapter: adapter} do
    assert {:ok, _} = put_document(adapter, "read-doc", 1)

    TestExporter.reset()

    assert {:ok, %{body: %{"value" => 1}}} =
             Adapter.get_document(adapter, %{document_id: "read-doc"})

    assert [_] = TestExporter.spans_named("elixir_db.sqlite.document.lookup")
    assert [_] = TestExporter.spans_named("elixir_db.sqlite.revision.lookup")

    TestExporter.reset()

    assert {:ok, results} =
             Adapter.apply_bulk_mutation(adapter, %{
               operations: [
                 %{operation: :put, document_id: "bulk-a", body: %{"value" => 2}},
                 %{operation: :put, document_id: "bulk-b", body: %{"value" => 3}}
               ]
             })

    assert [_, _] = results
    assert [prepare] = TestExporter.spans_named("elixir_db.sqlite.mutation.bulk.prepare")
    assert [finalize] = TestExporter.spans_named("elixir_db.sqlite.mutation.bulk.finalize")
    assert TestExporter.span_attr(prepare, :entries) == 2
    assert TestExporter.span_attr(finalize, :entries) == 2
    assert [_] = TestExporter.spans_named("elixir_db.sqlite.transaction.begin")
    assert [_] = TestExporter.spans_named("elixir_db.sqlite.transaction.commit")
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
    assert [_] = TestExporter.spans_named("elixir_db.sqlite.changes.identity")
    assert [fetch] = TestExporter.spans_named("elixir_db.sqlite.changes.fetch")
    assert [decode] = TestExporter.spans_named("elixir_db.sqlite.changes.decode")
    assert [_] = TestExporter.spans_named("elixir_db.sqlite.changes.has_more")
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

    expected = [
      "elixir_db.sqlite.query.prepare_request",
      "elixir_db.sqlite.query.identity",
      "elixir_db.sqlite.query.index_catalog",
      "elixir_db.sqlite.query.plan",
      "elixir_db.sqlite.query.candidates",
      "elixir_db.sqlite.query.filter",
      "elixir_db.sqlite.query.sort",
      "elixir_db.sqlite.query.cursor",
      "elixir_db.sqlite.query.project"
    ]

    Enum.each(expected, fn name -> assert [_] = TestExporter.spans_named(name) end)

    [candidates] = TestExporter.spans_named("elixir_db.sqlite.query.candidates")
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
