defmodule ElixirDB.EndToEnd.OfflineCopyTest do
  use ExUnit.Case, async: false

  alias ElixirDB.Runtime.DatabaseCatalog

  @tag :slow
  test "offline copy, registration, derived index rebuild, and integrity" do
    source_rel = "e2e-offline-src-#{System.unique_integer([:positive])}.db"
    dest_rel = "e2e-offline-dst-#{System.unique_integer([:positive])}.db"
    root = ElixirDB.Config.database_root()
    source_abs = Path.join(root, source_rel)
    dest_abs = Path.join(root, dest_rel)

    for path <- [source_abs, dest_abs] do
      ElixirDB.TempDatabase.cleanup(path)
    end

    assert {:ok, source} = DatabaseCatalog.create(source_rel)
    source_uuid = source.database_uuid

    assert {:ok, %{revision: revision}} =
             ElixirDB.Documents.put(source_uuid, %{
               id: "portable",
               body: %{"copied" => true, "kind" => "task"}
             })

    assert {:ok, _} =
             ElixirDB.Documents.put(source_uuid, %{
               id: "other",
               body: %{"copied" => true, "kind" => "note"}
             })

    assert {:ok, %{"index_id" => structured_id}} =
             ElixirDB.Query.create_index(source_uuid, %{
               "name" => "by-kind",
               "type" => "structured",
               "fields" => [%{"path" => "/kind", "type" => "string", "direction" => "asc"}]
             })

    assert {:ok, %{"index_id" => fts_id}} =
             ElixirDB.Query.create_index(source_uuid, %{
               "name" => "kind-text",
               "type" => "full_text",
               "fields" => ["/kind"],
               "tokenization" => %{"strategy" => "unicode_words_v1", "diacritics" => "preserve"}
             })

    assert {:ok, %{documents: pre_docs, selected_index: ^structured_id}} =
             ElixirDB.Query.execute(source_uuid, %{
               "selector" => %{"/kind" => "task"},
               "index" => "by-kind",
               "limit" => 10
             })

    assert Enum.any?(pre_docs, &(&1.id == "portable"))

    # Closed copy — LIFE-009: database must be closed before offline portability.
    assert :ok = DatabaseCatalog.close(source_uuid)
    refute File.exists?(source_abs <> "-journal")
    refute File.exists?(source_abs <> "-wal")
    File.cp!(source_abs, dest_abs)

    # Same UUID cannot register twice on this host; unregister the source route first.
    assert :ok = DatabaseCatalog.unregister(source_uuid)
    assert {:ok, restored} = DatabaseCatalog.register(dest_rel)
    assert restored.database_uuid == source_uuid

    on_exit(fn ->
      _ = DatabaseCatalog.close(source_uuid)
      _ = DatabaseCatalog.unregister(source_uuid)
      ElixirDB.TempDatabase.cleanup(source_abs)
      ElixirDB.TempDatabase.cleanup(dest_abs)
    end)

    assert {:ok, %{revision: ^revision, body: %{"copied" => true, "kind" => "task"}}} =
             ElixirDB.Documents.get(source_uuid, %{id: "portable"})

    assert {:ok, indexes} = ElixirDB.Query.list_indexes(source_uuid)
    index_ids = MapSet.new(Enum.map(indexes, & &1["index_id"]))
    assert MapSet.member?(index_ids, structured_id)
    assert MapSet.member?(index_ids, fts_id)

    # Plan §12.6 scenario 6 — rebuild every derived index after registration.
    for %{"index_id" => index_id} <- indexes do
      assert {:ok, %{rebuilt: true}} = ElixirDB.Query.rebuild_index(source_uuid, index_id)
    end

    assert {:ok, %{documents: docs, selected_index: ^structured_id}} =
             ElixirDB.Query.execute(source_uuid, %{
               "selector" => %{"/kind" => "task"},
               "index" => "by-kind",
               "limit" => 10
             })

    assert length(docs) == 1
    assert hd(docs).id == "portable"

    assert {:ok, %{documents: fts_docs, selected_index: ^fts_id}} =
             ElixirDB.Query.execute(source_uuid, %{
               "search" => %{"index" => "kind-text", "text" => "task", "mode" => "all"},
               "limit" => 10
             })

    assert Enum.any?(fts_docs, &(&1.id == "portable"))

    assert {:ok, %{ok: true}} =
             DatabaseCatalog.command(source_uuid, {:command, :integrity_check, %{}})
  end
end
