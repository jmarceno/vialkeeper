defmodule VialKeeper.EndToEnd.ReleaseGateScenarioTest do
  @moduledoc """
  End-to-end scenario covering the release-gate sequence.

  Walks as many of the 16 release-gate steps as practical with local endpoints
  (and Plug.Test HTTP for database creation).
  """
  use ExUnit.Case, async: false

  @moduletag :integration

  alias Plug.Conn
  alias VialKeeper.HTTP.Router
  alias VialKeeper.JSON.StrictDecoder
  alias VialKeeper.MapAccess
  alias VialKeeper.Replication.Id
  alias VialKeeper.Runtime.DatabaseCatalog
  @tag :slow
  test "release-gate scenario with local endpoints" do
    root = VialKeeper.Config.database_root()
    prefix = "release-gate-#{System.unique_integer([:positive])}"
    a_path = prefix <> "-a.vialkeeper"
    b_path = prefix <> "-b.vialkeeper"

    for path <- [a_path, b_path] do
      VialKeeper.TempDatabase.cleanup(Path.join(root, path))
    end

    # Step 1 — create and register two databases through the Version 1 HTTP API.
    a_created = http(:post, "/v1/databases", %{"path" => a_path})
    assert a_created.status == 201
    {:ok, %{"data" => %{"database_uuid" => a_uuid}}} = decode(a_created)

    b_created = http(:post, "/v1/databases", %{"path" => b_path})
    assert b_created.status == 201
    {:ok, %{"data" => %{"database_uuid" => b_uuid}}} = decode(b_created)

    on_exit(fn ->
      for {uuid, path} <- [{a_uuid, a_path}, {b_uuid, b_path}] do
        _ = DatabaseCatalog.close(uuid)
        _ = DatabaseCatalog.unregister(uuid)
        VialKeeper.TempDatabase.cleanup(Path.join(root, path))
      end
    end)

    # Step 2 — write independently and retry a mutation after losing its response.
    assert {:ok, %{revision: a_root, replayed: false}} =
             VialKeeper.Documents.put(a_uuid, %{id: "alpha", body: %{"side" => "a", "n" => 1}})

    assert {:ok, %{revision: ^a_root, replayed: true}} =
             VialKeeper.Documents.put(a_uuid, %{id: "alpha", body: %{"side" => "a", "n" => 1}})

    assert {:ok, %{revision: b_root}} =
             VialKeeper.Documents.put(b_uuid, %{id: "beta", body: %{"side" => "b", "n" => 1}})

    assert {:ok, %{revision: conflict_seed}} =
             VialKeeper.Documents.put(a_uuid, %{id: "shared", body: %{"v" => 0}})

    assert {:ok, %{status: :completed}} = VialKeeper.Replication.one_shot(a_uuid, b_uuid)

    # Step 3 — create divergent revisions through replication + local writes.
    assert {:ok, %{revision: left}} =
             VialKeeper.Documents.put(a_uuid, %{
               id: "shared",
               if_revision: conflict_seed,
               body: %{"v" => "left"}
             })

    assert {:ok, %{revision: right}} =
             VialKeeper.Documents.put(b_uuid, %{
               id: "shared",
               if_revision: conflict_seed,
               body: %{"v" => "right"}
             })

    assert left != right

    # Step 4 — replicate both directions and verify active conflicts.
    assert {:ok, %{status: :completed}} = VialKeeper.Replication.one_shot(a_uuid, b_uuid)
    assert {:ok, %{status: :completed}} = VialKeeper.Replication.one_shot(b_uuid, a_uuid)

    assert {:ok, %{conflicts: a_conflicts}} =
             VialKeeper.Documents.get(a_uuid, %{id: "shared", include_conflicts: true})

    assert {:ok, %{conflicts: b_conflicts}} =
             VialKeeper.Documents.get(b_uuid, %{id: "shared", include_conflicts: true})

    assert [_] = a_conflicts
    assert [_] = b_conflicts

    assert MapSet.new([left, right]) ==
             MapSet.new([hd(a_conflicts), conflict_winner(a_uuid)])

    assert MapSet.new([left, right]) ==
             MapSet.new([hd(b_conflicts), conflict_winner(b_uuid)])

    # Second conflict document for delete-all resolution.
    assert {:ok, %{revision: c_seed}} =
             VialKeeper.Documents.put(a_uuid, %{id: "doomed", body: %{"x" => 1}})

    assert {:ok, %{status: :completed}} = VialKeeper.Replication.one_shot(a_uuid, b_uuid)

    assert {:ok, %{revision: c_left}} =
             VialKeeper.Documents.put(a_uuid, %{
               id: "doomed",
               if_revision: c_seed,
               body: %{"x" => "L"}
             })

    assert {:ok, %{revision: c_right}} =
             VialKeeper.Documents.put(b_uuid, %{
               id: "doomed",
               if_revision: c_seed,
               body: %{"x" => "R"}
             })

    assert {:ok, %{status: :completed}} = VialKeeper.Replication.one_shot(a_uuid, b_uuid)
    assert {:ok, %{status: :completed}} = VialKeeper.Replication.one_shot(b_uuid, a_uuid)

    # Step 5 — resolve one conflict to a surviving body and another by deleting all.
    live = Enum.sort([left, right])

    assert {:ok, %{revision: resolved, replayed: false}} =
             VialKeeper.Documents.resolve(a_uuid, %{
               id: "shared",
               expected_live_revisions: live,
               chosen_parent_revision: left,
               body: %{"v" => "merged"}
             })

    doomed_live = Enum.sort([c_left, c_right])

    assert {:ok, %{revision: deleted}} =
             VialKeeper.Documents.resolve(a_uuid, %{
               id: "doomed",
               expected_live_revisions: doomed_live,
               delete_all: true
             })

    assert {:ok, %{status: :completed}} = VialKeeper.Replication.one_shot(a_uuid, b_uuid)

    assert {:ok, %{revision: ^resolved, body: %{"v" => "merged"}, conflicts: []}} =
             VialKeeper.Documents.get(b_uuid, %{id: "shared", include_conflicts: true})

    assert {:ok, %{revision: ^deleted, deleted: true}} =
             VialKeeper.Documents.get(b_uuid, %{id: "doomed", revision: deleted})

    # Step 6 — verify changes entries contain the final physical leaf sets.
    assert {:ok, %{results: changes}} = VialKeeper.Changes.read(a_uuid, %{since: 0, limit: 100})

    shared_change =
      changes
      |> Enum.filter(&(&1.document_id == "shared"))
      |> List.last()

    doomed_change =
      changes
      |> Enum.filter(&(&1.document_id == "doomed"))
      |> List.last()

    assert shared_change
    assert doomed_change
    assert shared_change.winning_revision == resolved
    assert doomed_change.winning_revision == deleted

    assert Enum.any?(shared_change.leaf_revisions, fn leaf ->
             MapAccess.get(leaf, :revision) == resolved
           end)

    assert Enum.any?(doomed_change.leaf_revisions, fn leaf ->
             MapAccess.get(leaf, :revision) == deleted and
               MapAccess.get(leaf, :deleted) == true
           end)

    # Step 7 — structured + full-text indexes and pointer-keyed projections.
    assert {:ok, %{"index_id" => structured_id}} =
             VialKeeper.Query.create_index(a_uuid, %{
               "name" => "by-side",
               "type" => "structured",
               "fields" => [%{"path" => "/side", "type" => "string", "direction" => "asc"}]
             })

    assert {:ok, %{"index_id" => fts_id}} =
             VialKeeper.Query.create_index(a_uuid, %{
               "name" => "body-text",
               "type" => "full_text",
               "fields" => ["/side"],
               "tokenization" => %{"strategy" => "unicode_words_v1", "diacritics" => "preserve"}
             })

    assert {:ok, %{documents: [projected], selected_index: ^structured_id}} =
             VialKeeper.Query.execute(a_uuid, %{
               "selector" => %{"/side" => "a"},
               "fields" => ["/side"],
               "index" => "by-side",
               "limit" => 10
             })

    assert projected.id == "alpha"
    assert projected.fields["/side"] == "a"

    assert {:ok, %{documents: [%{id: "alpha"}], selected_index: ^fts_id}} =
             VialKeeper.Query.execute(a_uuid, %{
               "search" => %{"index" => "body-text", "text" => "a", "mode" => "all"},
               "limit" => 10
             })

    # Step 8 — paginated queries; mutate; old bookmark becomes stale.
    assert {:ok, _} =
             VialKeeper.Documents.put(a_uuid, %{id: "gamma", body: %{"side" => "a", "n" => 2}})

    assert {:ok, page1} =
             VialKeeper.Query.execute(a_uuid, %{
               "selector" => %{"/side" => "a"},
               "index" => "by-side",
               "limit" => 1
             })

    assert is_binary(page1.bookmark)
    assert [_] = page1.documents

    assert {:ok, %{documents: [%{id: "gamma"}]}} =
             VialKeeper.Query.execute(a_uuid, %{
               "selector" => %{"/side" => "a"},
               "index" => "by-side",
               "limit" => 1,
               "bookmark" => page1.bookmark
             })

    assert {:ok, _} =
             VialKeeper.Documents.put(a_uuid, %{
               id: "delta",
               body: %{"side" => "a", "n" => 3}
             })

    assert {:error, %VialKeeper.Error{code: :bookmark_stale}} =
             VialKeeper.Query.execute(a_uuid, %{
               "selector" => %{"/side" => "a"},
               "index" => "by-side",
               "limit" => 1,
               "bookmark" => page1.bookmark
             })

    # Steps 9–11 — checkpointed replication, verify local-only state, clean stop.
    assert {:ok, %{status: :completed}} = VialKeeper.Replication.one_shot(a_uuid, b_uuid)

    assert {:ok, replication_id} =
             Id.calculate(a_uuid, b_uuid, "push", "one_shot")

    assert {:ok, %{version: a_cp_version, value: a_checkpoint}} =
             DatabaseCatalog.command(
               a_uuid,
               {:command, :get_local_record, "checkpoints", replication_id}
             )

    assert a_cp_version >= 1
    assert a_checkpoint["source_sequence"] >= 1

    # Local indexes / checkpoints are not protocol-replicated as documents.
    assert {:ok, b_indexes} = VialKeeper.Query.list_indexes(b_uuid)
    refute Enum.any?(b_indexes, &(&1["index_id"] == structured_id))
    refute Enum.any?(b_indexes, &(&1["index_id"] == fts_id))

    assert {:ok, %{revision: ^a_root}} = VialKeeper.Documents.get(b_uuid, %{id: "alpha"})
    assert {:ok, %{revision: ^b_root}} = VialKeeper.Documents.get(a_uuid, %{id: "beta"})

    assert :ok = DatabaseCatalog.close(a_uuid)
    assert :ok = DatabaseCatalog.close(b_uuid)

    # Steps 12–16 — OS copy, re-register, rebuild indexes, verify integrity.
    a_copy = prefix <> "-a-copy.vialkeeper"
    b_copy = prefix <> "-b-copy.vialkeeper"
    File.cp_r!(Path.join(root, a_path), Path.join(root, a_copy))
    File.cp_r!(Path.join(root, b_path), Path.join(root, b_copy))

    on_exit(fn ->
      for path <- [a_copy, b_copy] do
        abs = Path.join(root, path)
        VialKeeper.TempDatabase.cleanup(abs)
      end
    end)

    assert :ok = DatabaseCatalog.unregister(a_uuid)
    assert :ok = DatabaseCatalog.unregister(b_uuid)

    assert {:ok, a_restored} = DatabaseCatalog.register(a_copy)
    assert {:ok, b_restored} = DatabaseCatalog.register(b_copy)
    assert a_restored.database_uuid == a_uuid
    assert b_restored.database_uuid == b_uuid

    assert {:ok, indexes} = VialKeeper.Query.list_indexes(a_uuid)

    for %{"index_id" => index_id} <- indexes do
      assert {:ok, %{rebuilt: true}} = VialKeeper.Query.rebuild_index(a_uuid, index_id)
    end

    assert {:ok, %{revision: ^resolved, body: %{"v" => "merged"}}} =
             VialKeeper.Documents.get(a_uuid, %{id: "shared"})

    assert {:ok, %{revision: ^deleted, deleted: true}} =
             VialKeeper.Documents.get(a_uuid, %{id: "doomed", revision: deleted})

    assert {:ok, %{ok: true}} =
             DatabaseCatalog.command(a_uuid, {:command, :integrity_check, %{}})

    assert {:ok, %{ok: true}} =
             DatabaseCatalog.command(b_uuid, {:command, :integrity_check, %{}})

    assert {:ok, %{documents: docs}} =
             VialKeeper.Query.execute(a_uuid, %{
               "selector" => %{"/side" => "a"},
               "index" => "by-side",
               "limit" => 10
             })

    assert Enum.any?(docs, &(&1.id == "alpha"))
  end

  @tag :slow
  test "replicates a live leaf alongside a deleted conflict branch" do
    alias VialKeeper.RevisionFixtures
    alias VialKeeper.Revisions.Id, as: RevisionId
    alias VialKeeper.Storage.AdapterCase

    root = VialKeeper.Config.database_root()
    prefix = "release-gate-del-conflict-#{System.unique_integer([:positive])}"
    a_path = prefix <> "-a.vialkeeper"
    b_path = prefix <> "-b.vialkeeper"

    for path <- [a_path, b_path] do
      VialKeeper.TempDatabase.cleanup(Path.join(root, path))
    end

    {:ok, a} = DatabaseCatalog.create(a_path)
    {:ok, b} = DatabaseCatalog.create(b_path)

    on_exit(fn ->
      for {identity, path} <- [{a, a_path}, {b, b_path}] do
        _ = DatabaseCatalog.close(identity.database_uuid)
        _ = DatabaseCatalog.unregister(identity.database_uuid)
        VialKeeper.TempDatabase.cleanup(Path.join(root, path))
      end
    end)

    a_uuid = a.database_uuid
    b_uuid = b.database_uuid
    document_id = "deleted-conflict"

    root_body = %{"role" => "root"}
    left_body = %{"role" => "left"}
    history_id = RevisionFixtures.shared_history_id()
    {:ok, root_rev} = RevisionId.calculate(document_id, history_id, nil, false, root_body, %{})
    {:ok, left} = RevisionId.calculate(document_id, history_id, root_rev, false, left_body, %{})
    {:ok, right_deleted} = RevisionId.calculate(document_id, history_id, root_rev, true, nil, %{})

    assert {:ok, _} =
             DatabaseCatalog.command(a_uuid, {
               :command,
               :import_revision_chains,
               %{
                 chains: [
                   %{
                     document_id: document_id,
                     leaf_revision: left,
                     revisions: [
                       AdapterCase.wire_revision(document_id, root_rev, nil, false, root_body),
                       AdapterCase.wire_revision(document_id, left, root_rev, false, left_body)
                     ]
                   },
                   %{
                     document_id: document_id,
                     leaf_revision: right_deleted,
                     revisions: [
                       AdapterCase.wire_revision(document_id, root_rev, nil, false, root_body),
                       AdapterCase.wire_revision(document_id, right_deleted, root_rev, true, nil)
                     ]
                   }
                 ]
               }
             })

    assert {:ok, %{status: :completed}} = VialKeeper.Replication.one_shot(a_uuid, b_uuid)

    assert {:ok, %{revision: ^left, conflicts: []}} =
             VialKeeper.Documents.get(b_uuid, %{id: document_id, include_conflicts: true})

    # Tombstone is a physical leaf, not a live conflict entry.
    assert {:ok, %{revision: ^right_deleted, deleted: true}} =
             VialKeeper.Documents.get(b_uuid, %{id: document_id, revision: right_deleted})

    assert {:ok, %{results: changes}} = VialKeeper.Changes.read(b_uuid, %{since: 0, limit: 100})

    change =
      changes
      |> Enum.filter(&(MapAccess.get(&1, :document_id) == document_id))
      |> List.last()

    assert change
    assert MapAccess.get(change, :winning_revision) == left

    assert Enum.any?(change.leaf_revisions, fn leaf ->
             MapAccess.get(leaf, :revision) == left and MapAccess.get(leaf, :deleted) == false
           end)

    assert Enum.any?(change.leaf_revisions, fn leaf ->
             MapAccess.get(leaf, :revision) == right_deleted and
               MapAccess.get(leaf, :deleted) == true
           end)

    a_leaves_before = document_leaf_set(a_uuid, document_id)
    assert {:ok, %{status: :completed}} = VialKeeper.Replication.one_shot(b_uuid, a_uuid)
    assert document_leaf_set(a_uuid, document_id) == a_leaves_before
  end

  defp document_leaf_set(uuid, document_id) do
    assert {:ok, %{results: changes}} = VialKeeper.Changes.read(uuid, %{since: 0, limit: 100})

    change =
      changes
      |> Enum.filter(&(MapAccess.get(&1, :document_id) == document_id))
      |> List.last()

    change.leaf_revisions
    |> Enum.map(fn leaf ->
      {MapAccess.get(leaf, :revision), MapAccess.get(leaf, :deleted)}
    end)
    |> MapSet.new()
  end

  defp conflict_winner(uuid) do
    {:ok, %{revision: revision}} = VialKeeper.Documents.get(uuid, %{id: "shared"})
    revision
  end

  defp http(method, path, body) do
    payload = IO.iodata_to_binary(JSON.encode_to_iodata!(body))

    Plug.Test.conn(method, path, payload)
    |> Conn.put_req_header("content-type", "application/json")
    |> Router.call([])
  end

  defp decode(%Conn{resp_body: body}), do: StrictDecoder.decode(body)
end
