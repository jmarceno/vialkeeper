Code.require_file("../../bench/overhead/capture.exs", __DIR__)

defmodule VialKeeper.Bench.OverheadCaptureTest do
  use ExUnit.Case, async: false

  alias VialKeeper.Benchmarks.Overhead.Capture
  alias VialKeeper.Storage.SQLite.{Adapter, Connection}

  setup do
    worker =
      Capture.start(fn ->
        {:ok, adapter} = Adapter.create(":memory:", %{storage_mode: :memory})

        {:ok, [_]} =
          Adapter.apply_bulk_mutation(adapter, %{
            operations: [%{operation: :put, document_id: "doc", body: %{"n" => 1}}]
          })

        adapter
      end)

    on_exit(fn -> Capture.stop(worker) end)
    %{worker: worker}
  end

  test "records the statements and row counts of an adapter read", %{worker: worker} do
    {result, ops} = Capture.capture(worker, &Adapter.get_document(&1, %{document_id: "doc"}))

    assert {:ok, %{id: "doc"}} = result
    assert [%{kind: :query, sql: sql, params: ["doc"], rows: 1}] = ops
    assert sql =~ "FROM documents"
  end

  test "records transaction control and writes of a bulk mutation", %{worker: worker} do
    operations = [%{operation: :put, document_id: "other", body: %{"n" => 2}}]

    {{:ok, [_]}, ops} =
      Capture.capture(worker, &Adapter.apply_bulk_mutation(&1, %{operations: operations}))

    assert [%{kind: :exec, sql: "BEGIN IMMEDIATE"} | _] = ops
    assert [%{kind: :exec, sql: "COMMIT"} | _] = Enum.reverse(ops)
    assert Enum.any?(ops, &(&1.kind in [:query, :execute] and &1.sql =~ "INSERT INTO documents"))
  end

  test "removes trace patterns after a capture", %{worker: worker} do
    {_result, _ops} = Capture.capture(worker, &Adapter.get_document(&1, %{document_id: "doc"}))

    for mfa <- [{Connection, :query, 3}, {Connection, :execute, 3}, {Connection, :exec, 2}] do
      assert :erlang.trace_info(mfa, :traced) == {:traced, false}
    end

    assert {:ok, []} = Capture.capture(worker, fn _adapter -> :ok end)
  end
end
