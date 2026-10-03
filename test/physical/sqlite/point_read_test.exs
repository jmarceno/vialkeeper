defmodule VialKeeper.StorageAdapter.PointReadTest do
  @moduledoc "Covers the statement shape of storage document reads."
  # Call trace patterns are VM-global, so this module does not run async.
  use ExUnit.Case, async: false

  @moduletag :sqlite_physical

  alias VialKeeper.Storage.Services
  alias VialKeeper.Storage.SQLite.{Adapter, Connection}

  setup do
    {:ok, adapter} = Adapter.create(":memory:", %{storage_mode: :memory})
    on_exit(fn -> Adapter.close(adapter) end)

    {:ok, %{revision: first}} =
      Adapter.apply_local_mutation(adapter, %{
        operation: :put,
        document_id: "doc",
        body: %{"n" => 1}
      })

    {:ok, %{revision: second}} =
      Adapter.apply_local_mutation(adapter, %{
        operation: :put,
        document_id: "doc",
        if_revision: first,
        body: %{"n" => 2}
      })

    {:ok, %{revision: gone}} =
      Adapter.apply_local_mutation(adapter, %{
        operation: :put,
        document_id: "gone",
        body: %{"n" => 0}
      })

    {:ok, _} =
      Adapter.apply_local_mutation(adapter, %{
        operation: :delete,
        document_id: "gone",
        if_revision: gone
      })

    %{context: Adapter.to_context(adapter), first: first, second: second}
  end

  test "a winner read is one statement without a snapshot", %{context: context, second: second} do
    {result, calls} =
      traced(fn -> Services.get_document(context, %{document_id: "doc"}) end)

    assert {:ok, %{id: "doc", revision: ^second, body: %{"n" => 2}, deleted: false}} = result
    assert snapshot_read(context, %{document_id: "doc"}) == result
    assert [{:query, _sql}] = calls
  end

  test "historical and conflict reads look the document up once inside one snapshot", %{
    context: context,
    first: first,
    second: second
  } do
    {result, calls} =
      traced(fn -> Services.get_document(context, %{document_id: "doc", revision: first}) end)

    assert {:ok, %{revision: ^first, body: %{"n" => 1}}} = result
    assert [{:exec, "BEGIN"}, {:query, _}, {:query, _}, {:exec, "COMMIT"}] = calls

    {result, calls} =
      traced(fn ->
        Services.get_document(context, %{document_id: "doc", include_conflicts: true})
      end)

    assert {:ok, %{revision: ^second, conflicts: []}} = result

    assert [{:exec, "BEGIN"}, {:query, _}, {:query, _}, {:query, _}, {:exec, "COMMIT"}] =
             calls
  end

  test "winner reads report missing and deleted documents like snapshot reads", %{
    context: context
  } do
    for request <- [%{document_id: "missing"}, %{document_id: "gone"}] do
      assert {:error, winner_error} = Services.get_document(context, request)
      assert {:error, snapshot_error} = snapshot_read(context, request)
      assert winner_error.code == :document_not_found

      assert {winner_error.message, winner_error.details} ==
               {snapshot_error.message, snapshot_error.details}
    end

    assert {:error, %{details: %{winning_revision: revision}}} =
             Services.get_document(context, %{document_id: "gone"})

    assert is_binary(revision)
  end

  test "an unknown revision is reported as missing", %{context: context} do
    assert {:error, %{code: :revision_not_found}} =
             Services.get_document(context, %{
               document_id: "doc",
               revision: "9-" <> String.duplicate("0", 64)
             })
  end

  # The snapshot path for the same winner, reached through an explicit
  # conflict listing that is then dropped.
  defp snapshot_read(context, request) do
    case Services.get_document(context, Map.put(request, :include_conflicts, true)) do
      {:ok, document} -> {:ok, Map.delete(document, :conflicts)}
      {:error, _} = error -> error
    end
  end

  defp traced(fun) do
    {result, calls} =
      VialKeeper.CallTrace.run([{Connection, :query, 3}, {Connection, :exec, 2}], fun)

    {result,
     Enum.map(calls, fn
       {Connection, :query, [_conn, sql, _params]} -> {:query, sql}
       {Connection, :exec, [_conn, sql]} -> {:exec, sql}
     end)}
  end
end
