defmodule VialKeeper.Storage.BulkVsIndividualPropertiesTest do
  @moduledoc """
  Successful bulk puts preserve the same logical document state as the same
  individual puts. History IDs are random, so this compares bodies and deletion,
  not revision identifiers.
  """

  use ExUnit.Case, async: true
  use ExUnitProperties

  alias VialKeeper.ModelGenerators
  alias VialKeeper.Storage.Memory.Adapter
  alias VialKeeper.TempDatabase

  property "independent bulk puts match sequential puts on body and deleted" do
    check all(
            pairs <-
              StreamData.list_of(
                StreamData.tuple({ModelGenerators.document_id(), ModelGenerators.document_body()}),
                min_length: 1,
                max_length: 4
              ),
            max_runs: 15
          ) do
      pairs = uniquify(pairs)
      {:ok, sequential_bundle} = TempDatabase.create(prefix: "vk-bulk-seq")
      {:ok, bulk_bundle} = TempDatabase.create(prefix: "vk-bulk-batch")
      {:ok, sequential} = Adapter.create(sequential_bundle, %{})
      {:ok, bulk} = Adapter.create(bulk_bundle, %{})

      try do
        Enum.each(pairs, fn {document_id, body} ->
          assert {:ok, _} =
                   Adapter.apply_local_mutation(sequential, %{
                     operation: :put,
                     document_id: document_id,
                     body: body
                   })
        end)

        operations =
          Enum.map(pairs, fn {document_id, body} ->
            %{operation: :put, document_id: document_id, body: body}
          end)

        assert {:ok, results} = Adapter.apply_bulk_mutation(bulk, %{operations: operations})
        assert length(results) == length(pairs)
        assert Enum.all?(results, &(&1.replayed == false))

        Enum.each(pairs, fn {document_id, body} ->
          assert {:ok, sequential_doc} =
                   Adapter.get_document(sequential, %{document_id: document_id})

          assert {:ok, bulk_doc} = Adapter.get_document(bulk, %{document_id: document_id})
          assert sequential_doc.body == body
          assert bulk_doc.body == body
          assert sequential_doc.deleted == false
          assert bulk_doc.deleted == false
        end)
      after
        _ = Adapter.close(sequential)
        _ = Adapter.close(bulk)
        TempDatabase.cleanup(sequential_bundle)
        TempDatabase.cleanup(bulk_bundle)
      end
    end
  end

  defp uniquify(pairs) do
    nonce = System.unique_integer([:positive])

    pairs
    |> Enum.with_index()
    |> Enum.map(fn {{document_id, body}, index} ->
      {"#{document_id}-#{nonce}-#{index}", body}
    end)
  end
end
