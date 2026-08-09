defmodule ElixirDB.Replication.TransferPipelineTest do
  use ExUnit.Case, async: true

  alias ElixirDB.Error
  alias ElixirDB.Replication.TransferPipeline

  defmodule ElixirDB.Replication.TransferPipelineTestEndpoint do
    defstruct [:owner, :crash_ordinal, :counter, :max_concurrent, :trace_expected]

    def get_revision_chains(
          %__MODULE__{
            owner: owner,
            crash_ordinal: crash_ordinal,
            counter: counter,
            max_concurrent: max_concurrent,
            trace_expected: trace_expected
          },
          %{documents: [%{document_id: document_id}]}
        ) do
      ordinal = document_id |> String.trim_leading("doc-") |> String.to_integer()
      active = increment_active(counter)

      if is_integer(max_concurrent) and active > max_concurrent do
        send(owner, {:bound_violation, active})
        raise "concurrency bound exceeded"
      end

      send(owner, {:chain_started, ordinal, self()})

      if not is_nil(trace_expected) do
        send(owner, {:trace_context, OpenTelemetry.Ctx.get_current() == trace_expected})
      end

      if ordinal == crash_ordinal do
        raise "test chain failure"
      end

      if ordinal < 2 do
        receive do
          {:release, ^ordinal} ->
            decrement_active(counter)
            {:ok, %{chains: [%{document_id: "chain-#{ordinal}"}]}}
        end
      else
        decrement_active(counter)
        {:ok, %{chains: [%{document_id: "chain-#{ordinal}"}]}}
      end
    end

    defp increment_active(nil), do: 0
    defp increment_active(counter), do: :ets.update_counter(counter, :active, 1)

    defp decrement_active(nil), do: :ok
    defp decrement_active(counter), do: :ets.update_counter(counter, :active, -1)
  end

  defmodule ElixirDB.Replication.TransferPipelineTestErrorEndpoint do
    defstruct [:response]

    def get_revision_chains(%__MODULE__{response: response}, _request), do: response
  end

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

  test "fetches independent chain chunks up to the configured bound" do
    counter = :ets.new(:transfer_pipeline_counter, [:set, :public])
    :ets.insert(counter, {:active, 0})

    endpoint = %ElixirDB.Replication.TransferPipelineTestEndpoint{
      owner: self(),
      counter: counter,
      max_concurrent: 2
    }

    parent = self()

    runner =
      spawn(fn ->
        send(
          parent,
          {:result, TransferPipeline.run(endpoint, nil, %{documents: documents(4)}, config(2))}
        )
      end)

    started = Enum.map(1..2, fn _ -> receive_started() end)
    assert Enum.map(started, &elem(&1, 0)) |> Enum.sort() == [0, 1]

    Enum.each(started, fn {ordinal, pid} -> send(pid, {:release, ordinal}) end)

    assert_receive {:result, {:ok, %{chains: chains}}}
    assert chains == Enum.map(0..3, &%{document_id: "chain-#{&1}"})
    refute_received {:bound_violation, _}
    refute Process.alive?(runner)
  end

  test "normalizes a task crash and terminates every sibling task" do
    endpoint = %ElixirDB.Replication.TransferPipelineTestEndpoint{
      owner: self(),
      crash_ordinal: 1
    }

    parent = self()

    runner =
      spawn(fn ->
        result = TransferPipeline.run(endpoint, nil, %{documents: documents(2)}, config(2))
        downs = drain_down_messages()
        send(parent, {:result, result, downs})
      end)

    started = Enum.map(1..2, fn _ -> receive_started() end)
    sibling = started |> Enum.find(fn {ordinal, _pid} -> ordinal == 0 end) |> elem(1)
    assert_receive {:result, {:error, %Error{code: :internal_error}}, []}
    refute Process.alive?(sibling)
    refute Process.alive?(runner)
  end

  test "returns an empty chain list without starting endpoint work" do
    context = %{documents: [], chains: [:stale]}
    assert {:ok, %{chains: []}} = TransferPipeline.run(nil, nil, context, config(2))
  end

  test "normalizes malformed and non-Error endpoint responses" do
    malformed = %ElixirDB.Replication.TransferPipelineTestErrorEndpoint{
      response: {:ok, %{}}
    }

    failing = %ElixirDB.Replication.TransferPipelineTestErrorEndpoint{
      response: {:error, :timeout}
    }

    assert {:error, %Error{code: :invalid_request}} =
             TransferPipeline.run(malformed, nil, %{documents: documents(1)}, config(1))

    assert {:error, %Error{code: :internal_error}} =
             TransferPipeline.run(failing, nil, %{documents: documents(1)}, config(1))
  end

  test "propagates the caller trace context into chain tasks" do
    expected = OpenTelemetry.Ctx.get_current()

    endpoint = %ElixirDB.Replication.TransferPipelineTestEndpoint{
      owner: self(),
      trace_expected: expected
    }

    parent = self()

    runner =
      spawn(fn ->
        token = OpenTelemetry.Ctx.attach(expected)

        try do
          send(
            parent,
            {:result, TransferPipeline.run(endpoint, nil, %{documents: documents(1)}, config(1))}
          )
        after
          OpenTelemetry.Ctx.detach(token)
        end
      end)

    {_ordinal, child} = receive_started()
    assert_receive {:trace_context, true}
    send(child, {:release, 0})
    assert_receive {:result, {:ok, %{chains: [_]}}}
    refute Process.alive?(runner)
  end

  test "caller death takes down the private supervisor and chain children" do
    endpoint = %ElixirDB.Replication.TransferPipelineTestEndpoint{owner: self()}
    parent = self()

    runner =
      spawn(fn ->
        TransferPipeline.run(endpoint, nil, %{documents: documents(1)}, config(1))
        send(parent, :run_finished)
      end)

    {_ordinal, child} = receive_started()
    monitor = Process.monitor(runner)
    child_monitor = Process.monitor(child)
    Process.exit(runner, :kill)

    assert_receive {:DOWN, ^monitor, :process, ^runner, :killed}
    assert_receive {:DOWN, ^child_monitor, :process, ^child, _reason}
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

    assert {:error, %Error{code: :integrity_violation}} =
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
    assert {:error, %Error{code: :integrity_violation}} =
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

  defp documents(count), do: Enum.map(0..(count - 1), &document/1)

  defp config(concurrency) do
    %{"replication" => %{"batch_documents" => 1, "max_concurrent_chain_fetches" => concurrency}}
  end

  defp receive_started do
    receive do
      {:chain_started, ordinal, pid} -> {ordinal, pid}
    after
      1_000 -> flunk("chain task did not start")
    end
  end

  defp drain_down_messages(acc \\ []) do
    receive do
      {:DOWN, _ref, :process, _pid, _reason} = down ->
        drain_down_messages([down | acc])
    after
      0 -> Enum.reverse(acc)
    end
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
