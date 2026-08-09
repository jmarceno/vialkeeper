defmodule ElixirDB.Replication.TransferPipelineTest do
  use ExUnit.Case, async: true

  alias ElixirDB.Replication.TransferPipeline

  test "partitions missing documents deterministically within configured limits" do
    documents = Enum.map(1..7, &%{document_id: "doc-#{&1}", leaf_revisions: []})
    config = %{"replication" => %{"batch_documents" => 3, "max_concurrent_chain_fetches" => 2}}

    assert [
             %{ordinal: 0, documents: first},
             %{ordinal: 1, documents: second},
             %{ordinal: 2, documents: third}
           ] = TransferPipeline.partition_chain_fetches(documents, config)

    assert Enum.map(first ++ second ++ third, & &1.document_id) ==
             Enum.map(documents, & &1.document_id)

    assert_chunks_within(
      [
        %{documents: first},
        %{documents: second},
        %{documents: third}
      ],
      3
    )

    assert TransferPipeline.partition_chain_fetches(documents, config) ==
             TransferPipeline.partition_chain_fetches(documents, config)

    assert TransferPipeline.partition_chain_fetches([], config) == []
  end

  test "partitions one document into one bounded chunk" do
    chunks = partition_documents(1, 4, 10)
    assert Enum.map(chunks, & &1.documents) == [[document(1)]]
    assert_chunks_within(chunks, 10)
  end

  test "partitions fewer documents than concurrency without empty chunks" do
    chunks = partition_documents(2, 4, 10)
    assert Enum.map(chunks, &length(&1.documents)) == [1, 1]
    assert_chunks_within(chunks, 10)
  end

  test "partitions exact divisibility into equal chunks" do
    chunks = partition_documents(6, 2, 10)
    assert Enum.map(chunks, &length(&1.documents)) == [3, 3]
    assert_chunks_within(chunks, 10)
  end

  test "batch document limit wins over concurrency target" do
    chunks = partition_documents(10, 3, 2)
    assert Enum.map(chunks, &length(&1.documents)) == [2, 2, 2, 2, 2]
    assert_chunks_within(chunks, 2)
  end

  test "aggregates chains identically for every completion order" do
    expected = [%{document_id: "doc-1"}, %{document_id: "doc-2"}, %{document_id: "doc-3"}]

    for order <- permutations([0, 1, 2]) do
      completed = Map.new(order, fn ordinal -> {ordinal, [Enum.at(expected, ordinal)]} end)
      assert TransferPipeline.aggregate_chains(completed) == expected
    end
  end

  test "deduplicates a digest repeated across chains" do
    chains = [
      chain_with_attachment("a" <> String.duplicate("0", 63), 10),
      chain_with_attachment("a" <> String.duplicate("0", 63), 10)
    ]

    assert {:ok, [%TransferPipeline.BlobObligation{digest: digest, length: 10}]} =
             TransferPipeline.extract_blob_obligations(chains)

    assert digest == "a" <> String.duplicate("0", 63)
  end

  test "rejects conflicting lengths for one digest" do
    digest = String.duplicate("b", 64)

    assert {:error, %ElixirDB.Error{code: :integrity_violation}} =
             TransferPipeline.blob_obligations([
               chain_with_attachment(digest, 10),
               chain_with_attachment(digest, 11)
             ])
  end

  test "rejects attachment metadata with a missing digest" do
    assert_integrity_error(chain_with_attachment(%{length: 10}))
  end

  test "rejects attachment metadata with a non-binary digest" do
    assert_integrity_error(chain_with_attachment(%{digest: 123, length: 10}))
  end

  test "rejects attachment metadata with a missing length" do
    assert_integrity_error(chain_with_attachment(%{digest: String.duplicate("c", 64)}))
  end

  test "rejects attachment metadata with a negative or non-integer length" do
    assert_integrity_error(chain_with_attachment(%{digest: String.duplicate("d", 64), length: -1}))

    assert_integrity_error(
      chain_with_attachment(%{digest: String.duplicate("e", 64), length: "10"})
    )
  end

  test "rejects non-canonical attachment digests" do
    assert_integrity_error(chain_with_attachment(%{digest: String.duplicate("A", 64), length: 10}))

    assert_integrity_error(chain_with_attachment(%{digest: String.duplicate("f", 63), length: 10}))
  end

  defp chain_with_attachment(digest, length) do
    chain_with_attachment(%{digest: digest, length: length})
  end

  defp chain_with_attachment(entry) do
    %{
      document_id: "doc",
      revisions: [%{attachments: %{"file" => entry}}]
    }
  end

  defp assert_integrity_error(chain) do
    assert {:error, %ElixirDB.Error{code: :integrity_violation}} =
             TransferPipeline.blob_obligations([chain])
  end

  defp partition_documents(count, concurrency, batch_documents) do
    documents = Enum.map(1..count, &document/1)

    TransferPipeline.partition_chain_fetches(documents, %{
      "replication" => %{
        "batch_documents" => batch_documents,
        "max_concurrent_chain_fetches" => concurrency
      }
    })
  end

  defp assert_chunks_within(chunks, batch_documents) do
    assert Enum.all?(chunks, &(length(&1.documents) <= batch_documents))
  end

  defp document(id), do: %{document_id: "doc-#{id}", leaf_revisions: []}

  defp permutations([]), do: [[]]

  defp permutations(list) do
    for element <- list,
        rest <- permutations(list -- [element]),
        do: [element | rest]
  end
end
