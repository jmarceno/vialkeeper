defmodule ElixirDB.StorageAdapter.V1ConformanceTest do
  use ExUnit.Case, async: false

  alias ElixirDB.Revisions.Id
  alias ElixirDB.Storage.SQLite.{Adapter, Connection}

  setup do
    path =
      Path.join(System.tmp_dir!(), "elixirdb-conformance-#{System.unique_integer([:positive])}.db")

    {:ok, adapter} = Adapter.create(path, %{})

    on_exit(fn ->
      _ = Adapter.close(adapter)
      _ = File.rm(path)
      _ = File.rm(path <> ".lease")
    end)

    {:ok, adapter: adapter, path: path}
  end

  test "bulk writes are atomic and allocate one change per affected document", %{adapter: adapter} do
    assert {:ok, %{revision: first}} =
             Adapter.apply_local_mutation(adapter, %{
               operation: :put,
               document_id: "doc",
               body: %{"value" => 1}
             })

    assert {:error, %ElixirDB.Error{code: :revision_conflict}} =
             Adapter.apply_bulk_mutation(adapter, %{
               operations: [
                 %{operation: :put, document_id: "doc", if_revision: first, body: %{"value" => 2}},
                 %{operation: :put, document_id: "other", if_revision: "stale", body: %{}}
               ]
             })

    assert {:ok, %{body: %{"value" => 1}}} =
             Adapter.get_document(adapter, %{document_id: "doc"})

    assert {:error, %ElixirDB.Error{code: :document_not_found}} =
             Adapter.get_document(adapter, %{document_id: "other"})

    assert {:ok, %{current_sequence: 1}} = Adapter.identity(adapter)
  end

  test "imported sibling branches preserve conflicts and resolve atomically", %{adapter: adapter} do
    {:ok, root} = Id.calculate("doc", nil, false, %{"value" => 0})
    {:ok, left} = Id.calculate("doc", root, false, %{"value" => 1})
    {:ok, right} = Id.calculate("doc", root, false, %{"value" => 2})

    assert {:ok, %{revisions_inserted: 2}} =
             Adapter.import_revision_chains(adapter, %{
               chains: [
                 %{
                   document_id: "doc",
                   leaf_revision: left,
                   revisions: [
                     wire("doc", root, nil, false, %{"value" => 0}),
                     wire("doc", left, root, false, %{"value" => 1})
                   ]
                 }
               ]
             })

    assert {:ok, %{revisions_inserted: 1}} =
             Adapter.import_revision_chains(adapter, %{
               chains: [
                 %{
                   document_id: "doc",
                   leaf_revision: right,
                   revisions: [
                     wire("doc", root, nil, false, %{"value" => 0}),
                     wire("doc", right, root, false, %{"value" => 2})
                   ]
                 }
               ]
             })

    assert {:ok, %{conflicts: conflicts}} =
             Adapter.get_document(adapter, %{document_id: "doc", include_conflicts: true})

    assert length(conflicts) == 1
    assert hd(conflicts) in [left, right]

    assert {:error, %ElixirDB.Error{code: :revision_conflict}} =
             Adapter.resolve_conflict(adapter, %{
               document_id: "doc",
               expected_live_revisions: [left],
               chosen_parent_revision: left,
               body: %{"value" => 1}
             })

    assert {:ok, %{replayed: false}} =
             Adapter.resolve_conflict(adapter, %{
               document_id: "doc",
               expected_live_revisions: [left, right],
               chosen_parent_revision: left,
               body: %{"value" => 1}
             })
  end

  test "structured and full-text indexes are physical and integrity checked", %{adapter: adapter} do
    assert {:ok, _} =
             Adapter.apply_local_mutation(adapter, %{
               operation: :put,
               document_id: "a",
               body: %{"type" => "task", "title" => "Hello world"}
             })

    assert {:ok, _} =
             Adapter.apply_local_mutation(adapter, %{
               operation: :put,
               document_id: "b",
               body: %{"type" => "note", "title" => "Other"}
             })

    assert {:ok, %{"index_id" => structured_id}} =
             Adapter.create_index(adapter, %{
               "name" => "by-type",
               "type" => "structured",
               "fields" => [%{"path" => "/type", "type" => "string", "direction" => "asc"}]
             })

    assert {:ok, %{"index_id" => full_text_id}} =
             Adapter.create_index(adapter, %{
               "name" => "titles",
               "type" => "full_text",
               "fields" => ["/title"],
               "tokenization" => %{"strategy" => "unicode_words_v1", "diacritics" => "preserve"}
             })

    assert {:ok, %{results: [%{id: "a"}], selected_index: ^structured_id}} =
             Adapter.execute_query(adapter, %{
               selector: %{"/type" => "task"},
               index: "by-type",
               limit: 10
             })

    assert {:ok, %{results: [%{id: "a"}], selected_index: ^full_text_id}} =
             Adapter.execute_query(adapter, %{
               search: %{index: "titles", text: "hello", mode: "all"},
               limit: 10
             })

    assert {:ok, %{ok: true, indexes: 2}} = Adapter.integrity_check(adapter, %{})

    {:ok, indexes} = Adapter.list_indexes(adapter)
    full_text = Enum.find(indexes, &(&1["type"] == "full_text"))
    physical = full_text["_metadata"]["physical_name"]
    assert :ok = Connection.execute(adapter.conn, ~s(DELETE FROM "#{physical}"))

    assert {:error, %ElixirDB.Error{code: :integrity_violation}} =
             Adapter.integrity_check(adapter, %{})

    assert {:ok, %{rebuilt: true}} = Adapter.rebuild_index(adapter, full_text_id)
    assert {:ok, %{ok: true}} = Adapter.integrity_check(adapter, %{})
  end

  test "closed databases reopen with identity, sequence, and configuration intact", %{
    adapter: adapter,
    path: path
  } do
    assert {:ok, %{revision: _}} =
             Adapter.apply_local_mutation(adapter, %{
               operation: :put,
               document_id: "portable",
               body: %{"ok" => true}
             })

    assert {:ok, identity} = Adapter.identity(adapter)
    assert :ok = Adapter.close(adapter)
    assert {:ok, reopened} = Adapter.open(path)
    assert {:ok, reopened_identity} = Adapter.identity(reopened)
    assert reopened_identity.database_uuid == identity.database_uuid
    assert reopened_identity.current_sequence == identity.current_sequence
    assert reopened_identity.config == identity.config

    assert {:ok, %{body: %{"ok" => true}}} =
             Adapter.get_document(reopened, %{document_id: "portable"})

    assert {:ok, %{ok: true}} = Adapter.integrity_check(reopened, %{})
    assert :ok = Adapter.close(reopened)
  end

  defp wire(document_id, revision_id, parent, deleted, body) do
    {:ok, generation} = Id.generation(revision_id)

    %{
      document_id: document_id,
      revision_id: revision_id,
      generation: generation,
      parent_revision: parent,
      deleted: deleted,
      body: body
    }
  end
end
