defmodule ElixirDB.StorageAdapter.MutationsTest do
  use ElixirDB.Storage.AdapterCase, adapter: ElixirDB.Storage.SQLite.Adapter

  alias ElixirDB.Storage.SQLite.Connection

  test "bulk mutations are all-or-nothing", %{adapter: adapter} do
    assert {:ok, %{revision: first}} =
             @adapter.apply_local_mutation(adapter, %{
               operation: :put,
               document_id: "doc",
               body: %{"value" => 1}
             })

    assert {:error, %ElixirDB.Error{code: :revision_conflict}} =
             @adapter.apply_bulk_mutation(adapter, %{
               operations: [
                 %{
                   operation: :put,
                   document_id: "doc",
                   if_revision: first,
                   body: %{"value" => 2}
                 },
                 %{
                   operation: :put,
                   document_id: "other",
                   if_revision: "1-missing",
                   body: %{"value" => 9}
                 }
               ]
             })

    assert {:ok, %{body: %{"value" => 1}, revision: ^first}} =
             @adapter.get_document(adapter, %{document_id: "doc"})

    assert {:error, %ElixirDB.Error{code: :document_not_found}} =
             @adapter.get_document(adapter, %{document_id: "other"})

    assert {:ok, %{current_sequence: 1}} = @adapter.identity(adapter)

    assert {:ok, results} =
             @adapter.apply_bulk_mutation(adapter, %{
               operations: [
                 %{
                   operation: :put,
                   document_id: "doc",
                   if_revision: first,
                   body: %{"value" => 2}
                 },
                 %{operation: :put, document_id: "other", body: %{"value" => 9}}
               ]
             })

    assert length(results) == 2
    assert Enum.all?(results, &(&1.replayed == false))

    assert {:ok, %{body: %{"value" => 2}}} =
             @adapter.get_document(adapter, %{document_id: "doc"})

    assert {:ok, %{body: %{"value" => 9}}} =
             @adapter.get_document(adapter, %{document_id: "other"})
  end

  test "bulk mutations reject unknown operation types", %{adapter: adapter} do
    assert {:error, %ElixirDB.Error{code: :invalid_request}} =
             @adapter.apply_bulk_mutation(adapter, %{
               operations: [
                 %{operation: :upsert, document_id: "doc", body: %{"value" => 1}}
               ]
             })

    assert {:error, %ElixirDB.Error{code: :document_not_found}} =
             @adapter.get_document(adapter, %{document_id: "doc"})
  end

  test "integrity mismatch is detected after revision corruption", %{adapter: adapter} do
    assert {:ok, %{revision: revision}} =
             @adapter.apply_local_mutation(adapter, %{
               operation: :put,
               document_id: "doc",
               body: %{"ok" => true}
             })

    assert {:ok, %{ok: true}} = @adapter.integrity_check(adapter, %{})

    assert :ok =
             Connection.execute(
               adapter.conn,
               "UPDATE revisions SET body_json = ? WHERE revision_id = ?",
               [~s({"ok":false}), revision]
             )

    assert {:error, %ElixirDB.Error{code: :integrity_violation}} =
             @adapter.integrity_check(adapter, %{})
  end

  test "put replay returns the same revision without advancing sequence", %{adapter: adapter} do
    assert {:ok, %{revision: revision, sequence: sequence, replayed: false}} =
             @adapter.apply_local_mutation(adapter, %{
               operation: :put,
               document_id: "doc",
               body: %{"value" => 1}
             })

    assert {:ok, %{revision: ^revision, sequence: ^sequence, replayed: true}} =
             @adapter.apply_local_mutation(adapter, %{
               operation: :put,
               document_id: "doc",
               body: %{"value" => 1}
             })

    assert {:ok, %{revision: next, replayed: false}} =
             @adapter.apply_local_mutation(adapter, %{
               operation: :put,
               document_id: "doc",
               if_revision: revision,
               body: %{"value" => 2}
             })

    assert next != revision

    assert {:ok, %{revision: ^next, replayed: true}} =
             @adapter.apply_local_mutation(adapter, %{
               operation: :put,
               document_id: "doc",
               if_revision: revision,
               body: %{"value" => 2}
             })

    assert {:ok, %{current_sequence: 2}} = @adapter.identity(adapter)
  end

  test "stale-parent retry of a superseded revision returns revision_conflict with operation_already_committed",
       %{adapter: adapter} do
    # 1. Put doc A -> rev1.
    assert {:ok, %{revision: rev1}} =
             @adapter.apply_local_mutation(adapter, %{
               operation: :put,
               document_id: "doc",
               body: %{"value" => 1}
             })

    # 2. Put doc A (if_revision rev1) -> rev2. Winner = rev2.
    assert {:ok, %{revision: rev2, replayed: false}} =
             @adapter.apply_local_mutation(adapter, %{
               operation: :put,
               document_id: "doc",
               if_revision: rev1,
               body: %{"value" => 2}
             })

    assert rev2 != rev1

    # 3. Put doc A (if_revision rev2) -> rev3. Winner = rev3; rev2 is now superseded.
    assert {:ok, %{revision: rev3, replayed: false}} =
             @adapter.apply_local_mutation(adapter, %{
               operation: :put,
               document_id: "doc",
               if_revision: rev2,
               body: %{"value" => 3}
             })

    assert rev3 != rev2

    # 4. Retry the rev1->rev2 put (same body that produced rev2) with the stale parent rev1.
    #    The identical revision rev2 already exists, but a later revision (rev3) changed the
    #    current state, so rev2 is no longer the winner. TX-006 requires revision_conflict
    #    with operation_already_committed: true, NOT a silent replay.
    assert {:error,
            %ElixirDB.Error{
              code: :revision_conflict,
              details: %{operation_already_committed: true}
            }} =
             @adapter.apply_local_mutation(adapter, %{
               operation: :put,
               document_id: "doc",
               if_revision: rev1,
               body: %{"value" => 2}
             })

    # Winner is unchanged at rev3; sequence did not advance.
    assert {:ok, %{body: %{"value" => 3}, revision: ^rev3}} =
             @adapter.get_document(adapter, %{document_id: "doc"})
  end

  test "stale-parent retry that is still the winner replays successfully", %{adapter: adapter} do
    assert {:ok, %{revision: rev1, replayed: false}} =
             @adapter.apply_local_mutation(adapter, %{
               operation: :put,
               document_id: "doc",
               body: %{"value" => 1}
             })

    # Idempotent retry of the same create: identical revision exists and is still the winner.
    assert {:ok, %{revision: ^rev1, replayed: true}} =
             @adapter.apply_local_mutation(adapter, %{
               operation: :put,
               document_id: "doc",
               body: %{"value" => 1}
             })
  end

  test "bulk write with a stale-parent superseded retry fails the batch atomically",
       %{adapter: adapter} do
    assert {:ok, %{revision: rev1}} =
             @adapter.apply_local_mutation(adapter, %{
               operation: :put,
               document_id: "doc",
               body: %{"value" => 1}
             })

    assert {:ok, %{revision: rev2}} =
             @adapter.apply_local_mutation(adapter, %{
               operation: :put,
               document_id: "doc",
               if_revision: rev1,
               body: %{"value" => 2}
             })

    # Advance the winner past rev2 so a retry of the rev1->rev2 put is genuinely superseded.
    assert {:ok, %{revision: rev3}} =
             @adapter.apply_local_mutation(adapter, %{
               operation: :put,
               document_id: "doc",
               if_revision: rev2,
               body: %{"value" => 3}
             })

    # Batch whose first op is a stale-parent superseded retry of rev2. The whole batch must
    # abort (TX-004) with operation_already_committed: true and no partial writes.
    assert {:error,
            %ElixirDB.Error{
              code: :revision_conflict,
              details: %{operation_already_committed: true}
            }} =
             @adapter.apply_bulk_mutation(adapter, %{
               operations: [
                 %{
                   operation: :put,
                   document_id: "doc",
                   if_revision: rev1,
                   body: %{"value" => 2}
                 },
                 %{operation: :put, document_id: "fresh", body: %{"value" => 9}}
               ]
             })

    # Winner unchanged at rev3; the second op did not land.
    assert {:ok, %{body: %{"value" => 3}, revision: ^rev3}} =
             @adapter.get_document(adapter, %{document_id: "doc"})

    assert {:error, %ElixirDB.Error{code: :document_not_found}} =
             @adapter.get_document(adapter, %{document_id: "fresh"})
  end
end
