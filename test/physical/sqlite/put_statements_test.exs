defmodule VialKeeper.StorageAdapter.PutStatementsTest do
  @moduledoc "Covers the statement shape and scheduling of single-document writes."
  # Call trace patterns are VM-global, so this module does not run async.
  use ExUnit.Case, async: false

  @moduletag :sqlite_physical

  alias Exqlite.Sqlite3
  alias VialKeeper.Storage.Services
  alias VialKeeper.Storage.SQLite.{Adapter, Connection}

  @statements [
    {Connection, :query, 3},
    {Connection, :execute, 3},
    {Connection, :point_query, 3},
    {Connection, :point_execute, 3},
    {Connection, :exec, 2}
  ]
  @steps [{Sqlite3, :multi_step, 3}, {Sqlite3, :multi_step_inline, 3}]

  setup do
    {:ok, adapter} = Adapter.create(":memory:", %{storage_mode: :memory})
    on_exit(fn -> Adapter.close(adapter) end)
    %{adapter: adapter, context: Adapter.to_context(adapter)}
  end

  test "inserting a new document writes each row once and reads nothing back", %{
    context: context
  } do
    {result, calls} = traced(fn -> put(context, %{document_id: "doc", body: %{"n" => 1}}) end)

    assert {:ok, %{revision: "1-" <> _, sequence: 1, conflicts: []}} = result

    assert [
             {:exec, "BEGIN IMMEDIATE"},
             {:point_query, "SELECT doc_key" <> _},
             {:point_query, "UPDATE db_meta" <> _},
             {:point_query, "INSERT INTO documents" <> _},
             {:point_execute, "INSERT INTO revisions" <> _},
             {:point_execute, "INSERT INTO changes" <> _},
             {:exec, "COMMIT"}
           ] = statements(calls)
  end

  test "updating a document reads its row and leaves once", %{context: context} do
    {:ok, %{revision: first}} = put(context, %{document_id: "doc", body: %{"n" => 1}})

    {result, calls} =
      traced(fn ->
        put(context, %{document_id: "doc", if_revision: first, body: %{"n" => 2}})
      end)

    assert {:ok, %{revision: "2-" <> _, sequence: 2, conflicts: []}} = result

    assert [
             {:exec, "BEGIN IMMEDIATE"},
             {:point_query, "SELECT doc_key" <> _},
             {:point_query, "SELECT r.revision_id" <> leaves_sql},
             {:point_query, "SELECT r.revision_id" <> _candidate_sql},
             {:point_execute, "UPDATE revisions SET is_leaf = 0" <> _},
             {:point_execute, "INSERT INTO revisions" <> _},
             {:point_query, "UPDATE db_meta" <> _},
             {:point_execute, "UPDATE documents" <> _},
             {:point_execute, "INSERT INTO changes" <> _},
             {:exec, "COMMIT"}
           ] = statements(calls)

    assert leaves_sql =~ "is_leaf = 1"
  end

  test "point statements step inline only inside the write transaction", %{context: context} do
    {_result, calls} =
      traced(fn -> put(context, %{document_id: "doc", body: %{"n" => 1}}) end, @steps)

    # The five point statements of the insert; the only other statement is the
    # process-cached replication marker read (see `statements/1`).
    assert 5 == Enum.count(calls, &match?({Sqlite3, :multi_step_inline, _}, &1))

    {result, calls} =
      traced(fn -> Services.get_document(context, %{document_id: "doc"}) end, @steps)

    assert {:ok, %{body: %{"n" => 1}}} = result
    assert [{Sqlite3, :multi_step, _}] = calls
  end

  defp put(context, request),
    do: Services.apply_local_mutation(context, Map.put(request, :operation, :put))

  defp traced(fun, mfas \\ @statements), do: VialKeeper.CallTrace.run(mfas, fun)

  # The pending-replication marker is cached per process, and every traced call
  # runs in a fresh process, so its read is not part of a warm write.
  defp statements(calls) do
    calls
    |> Enum.map(fn
      {Connection, :exec, [_conn, sql]} -> {:exec, sql}
      {Connection, name, [_conn, sql, _params]} -> {name, IO.iodata_to_binary(sql)}
    end)
    |> Enum.reject(fn {_name, sql} -> sql =~ "local_records" end)
  end
end
