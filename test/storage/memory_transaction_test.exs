defmodule ElixirDB.Storage.MemoryTransactionTest do
  @moduledoc """
  Verifies rollback and serialization guarantees of the Memory transaction port.
  """

  use ExUnit.Case, async: true

  alias ElixirDB.Storage.Memory.{Adapter, DocumentFacts}
  alias ElixirDB.Storage.Ports.Errors
  alias ElixirDB.Storage.Transaction

  setup do
    root = ElixirDB.TempDatabase.path(prefix: "elixirdb-memory-transaction")
    assert {:ok, adapter} = Adapter.create(root)

    on_exit(fn ->
      _ = Adapter.close(adapter)
      _ = File.rm_rf(root)
    end)

    %{context: Adapter.to_context(adapter)}
  end

  test "restores state when the callback returns an error", %{context: context} do
    error = ElixirDB.Error.invalid_request("rollback")

    assert {:error, ^error} =
             Transaction.run(context, fn tx_context ->
               assert {:ok, _document} = DocumentFacts.ensure_document(tx_context, "rollback")
               {:error, error}
             end)

    assert {:ok, nil} = DocumentFacts.find_document(context, "rollback")
  end

  test "restores state before re-raising callback exceptions", %{context: context} do
    assert_raise RuntimeError, "boom", fn ->
      Transaction.run(context, fn tx_context ->
        assert {:ok, _document} = DocumentFacts.ensure_document(tx_context, "exception")
        raise "boom"
      end)
    end

    assert {:ok, nil} = DocumentFacts.find_document(context, "exception")
  end

  test "serializes concurrent callbacks without losing successful writes", %{context: context} do
    results =
      1..8
      |> Task.async_stream(
        fn number ->
          Transaction.run(context, fn tx_context ->
            DocumentFacts.ensure_document(tx_context, "document-#{number}")
            |> Errors.wrap()
          end)
        end,
        max_concurrency: 8,
        ordered: true
      )
      |> Enum.to_list()

    assert Enum.all?(results, &match?({:ok, {:ok, _}}, &1))

    for number <- 1..8 do
      assert {:ok, %{document_id: "document-" <> _}} =
               DocumentFacts.find_document(context, "document-#{number}")
    end
  end
end
