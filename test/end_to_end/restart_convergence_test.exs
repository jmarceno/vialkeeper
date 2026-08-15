defmodule VialKeeper.EndToEnd.RestartConvergenceTest do
  @moduledoc """
  Gap D8 companion: replicate, stop/restart owners via close/open, verify state.
  """
  use ExUnit.Case, async: false

  @moduletag :integration

  alias VialKeeper.MapAccess
  alias VialKeeper.Replication.Id
  alias VialKeeper.Runtime.DatabaseCatalog

  @tag :slow
  test "replication converges across close/reopen of source and target owners" do
    prefix = "e2e-restart-#{System.unique_integer([:positive])}"
    a_path = prefix <> "-a.vialkeeper"
    b_path = prefix <> "-b.vialkeeper"
    root = VialKeeper.Config.database_root()

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

    assert {:ok, %{revision: first}} =
             VialKeeper.Documents.put(a.database_uuid, %{id: "doc", body: %{"n" => 1}})

    assert {:ok, %{status: :completed}} =
             VialKeeper.Replication.one_shot(a.database_uuid, b.database_uuid)

    assert {:ok, %{revision: ^first}} =
             VialKeeper.Documents.get(b.database_uuid, %{id: "doc"})

    # Stop owners (releases file leases) and reopen.
    assert :ok = DatabaseCatalog.close(a.database_uuid)
    assert :ok = DatabaseCatalog.close(b.database_uuid)
    assert {:ok, _} = DatabaseCatalog.open(a.database_uuid)
    assert {:ok, _} = DatabaseCatalog.open(b.database_uuid)

    assert {:ok, %{revision: ^first, body: %{"n" => 1}}} =
             VialKeeper.Documents.get(a.database_uuid, %{id: "doc"})

    assert {:ok, %{revision: ^first, body: %{"n" => 1}}} =
             VialKeeper.Documents.get(b.database_uuid, %{id: "doc"})

    assert {:ok, %{revision: second}} =
             VialKeeper.Documents.put(a.database_uuid, %{
               id: "doc",
               if_revision: first,
               body: %{"n" => 2}
             })

    assert {:ok, %{status: :completed}} =
             VialKeeper.Replication.one_shot(a.database_uuid, b.database_uuid)

    assert {:ok, %{revision: ^second, body: %{"n" => 2}}} =
             VialKeeper.Documents.get(b.database_uuid, %{id: "doc"})

    # Second restart cycle: checkpointed resume must still transfer later changes.
    assert :ok = DatabaseCatalog.close(a.database_uuid)
    assert :ok = DatabaseCatalog.close(b.database_uuid)
    assert {:ok, _} = DatabaseCatalog.open(a.database_uuid)
    assert {:ok, _} = DatabaseCatalog.open(b.database_uuid)

    assert {:ok, %{revision: third}} =
             VialKeeper.Documents.put(a.database_uuid, %{
               id: "doc",
               if_revision: second,
               body: %{"n" => 3}
             })

    assert {:ok, %{status: :completed}} =
             VialKeeper.Replication.one_shot(a.database_uuid, b.database_uuid)

    assert {:ok, %{revision: ^third, body: %{"n" => 3}}} =
             VialKeeper.Documents.get(b.database_uuid, %{id: "doc"})

    assert {:ok, %{ok: true}} =
             DatabaseCatalog.command(b.database_uuid, {:command, :integrity_check, %{}})
  end

  @tag :slow
  test "repeated one_shot replication is idempotent and does not advance checkpoints" do
    prefix = "e2e-idempotent-#{System.unique_integer([:positive])}"
    a_path = prefix <> "-a.vialkeeper"
    b_path = prefix <> "-b.vialkeeper"
    root = VialKeeper.Config.database_root()

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

    for i <- 1..3 do
      assert {:ok, _} =
               VialKeeper.Documents.put(a_uuid, %{id: "doc-#{i}", body: %{"n" => i}})
    end

    assert {:ok, %{status: :completed}} = VialKeeper.Replication.one_shot(a_uuid, b_uuid)

    assert {:ok, replication_id} = Id.calculate(a_uuid, b_uuid, "push", "one_shot")

    assert {:ok, %{value: a_value}} =
             DatabaseCatalog.command(
               a_uuid,
               {:command, :get_local_record, "checkpoints", replication_id}
             )

    assert {:ok, %{value: b_value}} =
             DatabaseCatalog.command(
               b_uuid,
               {:command, :get_local_record, "checkpoints", replication_id}
             )

    a_seq = MapAccess.get(a_value, :source_sequence)
    b_seq = MapAccess.get(b_value, :source_sequence)
    assert a_seq == b_seq
    assert a_seq >= 3

    assert {:ok, %{results: b_changes_before}} =
             VialKeeper.Changes.read(b_uuid, %{since: 0, limit: 100})

    leaf_sets_before = leaf_sets_by_document(b_changes_before)
    doc_ids_before = MapSet.new(Enum.map(b_changes_before, &MapAccess.get(&1, :document_id)))

    for _ <- 1..4 do
      assert {:ok, %{status: :completed}} = VialKeeper.Replication.one_shot(a_uuid, b_uuid)
    end

    assert {:ok, %{value: a_value_after}} =
             DatabaseCatalog.command(
               a_uuid,
               {:command, :get_local_record, "checkpoints", replication_id}
             )

    assert {:ok, %{value: b_value_after}} =
             DatabaseCatalog.command(
               b_uuid,
               {:command, :get_local_record, "checkpoints", replication_id}
             )

    # Protocol progress is unchanged. Caught-up re-runs still append session
    # history (and therefore bump the local-record CAS version); that is not
    # the same as advancing source_sequence or rewriting documents.
    assert MapAccess.get(a_value_after, :source_sequence) == a_seq
    assert MapAccess.get(b_value_after, :source_sequence) == b_seq

    history = List.wrap(MapAccess.get(a_value_after, :history))

    noop_entries =
      Enum.filter(history, fn entry ->
        MapAccess.get(entry, :source_sequence) == a_seq and
          MapAccess.get(entry, :documents_read) == 0 and
          MapAccess.get(entry, :revisions_written) == 0
      end)

    assert Enum.count_until(noop_entries, 4) == 4

    assert {:ok, %{results: b_changes_after}} =
             VialKeeper.Changes.read(b_uuid, %{since: 0, limit: 100})

    assert MapSet.new(Enum.map(b_changes_after, &MapAccess.get(&1, :document_id))) ==
             doc_ids_before

    assert leaf_sets_by_document(b_changes_after) == leaf_sets_before

    for id <- ["doc-1", "doc-2", "doc-3"] do
      assert {:ok, %{conflicts: []}} =
               VialKeeper.Documents.get(b_uuid, %{id: id, include_conflicts: true})
    end

    assert {:ok, %{revision: new_rev}} =
             VialKeeper.Documents.put(a_uuid, %{id: "doc-4", body: %{"n" => 4}})

    assert {:ok, %{status: :completed}} = VialKeeper.Replication.one_shot(a_uuid, b_uuid)

    assert {:ok, %{revision: ^new_rev}} = VialKeeper.Documents.get(b_uuid, %{id: "doc-4"})

    assert {:ok, %{value: a_value_final}} =
             DatabaseCatalog.command(
               a_uuid,
               {:command, :get_local_record, "checkpoints", replication_id}
             )

    assert {:ok, %{value: b_value_final}} =
             DatabaseCatalog.command(
               b_uuid,
               {:command, :get_local_record, "checkpoints", replication_id}
             )

    assert MapAccess.get(a_value_final, :source_sequence) == a_seq + 1
    assert MapAccess.get(b_value_final, :source_sequence) == a_seq + 1
  end

  defp leaf_sets_by_document(changes) do
    Map.new(changes, fn change ->
      leaves =
        MapAccess.get(change, :leaf_revisions, [])
        |> Enum.map(fn leaf ->
          {MapAccess.get(leaf, :revision), MapAccess.get(leaf, :deleted)}
        end)
        |> MapSet.new()

      {MapAccess.get(change, :document_id), leaves}
    end)
  end
end
