defmodule ElixirDB.Replication.TransferPipelineBlobsTest do
  use ExUnit.Case, async: false

  alias ElixirDB.Error
  alias ElixirDB.Replication.BlobStream
  alias ElixirDB.Replication.TransferPipeline

  defmodule FakeEndpoint do
    defstruct [
      :owner,
      :chains,
      :lengths,
      :missing,
      :blocked_chains,
      :blocked_blobs,
      :failed_blob,
      :put_result
    ]

    def get_revision_chains(endpoint, %{documents: [%{document_id: document_id}]}) do
      send(endpoint.owner, {:chain_started, document_id, self()})
      wait_for(endpoint.blocked_chains, {:release_chain, document_id})
      {:ok, %{chains: Map.get(endpoint.chains, document_id, [])}}
    end

    def diff_blobs(endpoint, digests) do
      send(endpoint.owner, {:diff_started, digests, self()})
      {:ok, endpoint.missing || digests}
    end

    def open_blob(endpoint, digest) do
      send(endpoint.owner, {:blob_opened, digest, self()})
      wait_for(endpoint.blocked_blobs, {:release_blob, digest})

      {:ok, stream} = BlobStream.new(digest, endpoint.lengths[digest], [])
      {:ok, stream}
    end

    def put_blob(endpoint, stream) do
      send(endpoint.owner, {:blob_put, stream.digest, self()})

      cond do
        endpoint.failed_blob == stream.digest ->
          {:error, Error.internal_error("fake blob failure")}

        endpoint.put_result == :ok ->
          :ok

        true ->
          endpoint.put_result
      end
    end

    defp wait_for(blocked, message) do
      if elem(message, 1) in List.wrap(blocked) do
        receive do
          ^message -> :ok
        end
      end
    end
  end

  defmodule SourceEndpoint do
    defstruct [:inner]

    def get_revision_chains(endpoint, request),
      do: FakeEndpoint.get_revision_chains(endpoint.inner, request)

    def open_blob(endpoint, digest), do: FakeEndpoint.open_blob(endpoint.inner, digest)
  end

  defmodule TargetEndpoint do
    defstruct [:inner]
    def diff_blobs(endpoint, digests), do: FakeEndpoint.diff_blobs(endpoint.inner, digests)
    def put_blob(endpoint, stream), do: FakeEndpoint.put_blob(endpoint.inner, stream)
  end

  test "bounds blob concurrency and reaches the configured bound" do
    digests = digests(3)

    source =
      endpoint(chains: chains_for(digests), lengths: lengths(digests), blocked_blobs: digests)

    target = endpoint(missing: digests, lengths: lengths(digests), blocked_blobs: digests)

    spawn_runner(source, target, documents(1), config(2, 100))
    assert_receive {:chain_started, "doc-0", _}, 1_000
    assert_receive {:diff_started, ^digests, _}, 1_000
    opened = [receive_blob_opened(), receive_blob_opened()]
    refute_received {:blob_opened, _, _}

    Enum.each(opened, fn {digest, pid} -> send(pid, {:release_blob, digest}) end)
    assert_receive {:blob_put, _, _}, 1_000
    assert_receive {:blob_put, _, _}, 1_000
    release_remaining_blob(digests, opened)
    assert_receive {:result, {:ok, _}}, 1_000
  end

  test "byte budget waits for reservation release before starting another blob" do
    digests = digests(2)

    source =
      endpoint(
        chains: chains_for(digests, lengths(digests, 6)),
        lengths: lengths(digests, 6),
        blocked_blobs: digests
      )

    target = endpoint(missing: digests, lengths: lengths(digests, 6))

    spawn_runner(source, target, documents(1), config(2, 10))
    assert_receive {:diff_started, ^digests, _}, 1_000
    {first, first_pid} = receive_blob_opened()
    refute_received {:blob_opened, _, _}
    send(first_pid, {:release_blob, first})
    assert_receive {:blob_put, ^first, _}, 1_000
    {second, second_pid} = receive_blob_opened()
    send(second_pid, {:release_blob, second})
    assert_receive {:blob_put, ^second, _}, 1_000
    assert_receive {:result, {:ok, _}}, 1_000
  end

  test "byte budget FIFO keeps later queued blobs when the head cannot reserve yet" do
    # With concurrency 3 and A(60) in flight, available becomes 2 while reserved=60.
    # Selecting [B(50), C(40)] must not drop C when B cannot reserve yet.
    [digest_a, digest_b, digest_c] = digests = digests(3)
    lengths = %{digest_a => 60, digest_b => 50, digest_c => 40}

    source =
      endpoint(
        chains: chains_for(digests, lengths),
        lengths: lengths,
        blocked_blobs: digests
      )

    target = endpoint(missing: digests, lengths: lengths)

    spawn_runner(source, target, documents(1), config(3, 100))
    assert_receive {:diff_started, ^digests, _}, 1_000
    {^digest_a, pid_a} = receive_blob_opened()
    refute_received {:blob_opened, _, _}
    send(pid_a, {:release_blob, digest_a})
    assert_receive {:blob_put, ^digest_a, _}, 1_000

    opened =
      Map.new([receive_blob_opened(), receive_blob_opened()], fn {digest, pid} -> {digest, pid} end)

    assert MapSet.new(Map.keys(opened)) == MapSet.new([digest_b, digest_c])
    Enum.each(opened, fn {digest, pid} -> send(pid, {:release_blob, digest}) end)
    assert_receive {:blob_put, ^digest_b, _}, 1_000
    assert_receive {:blob_put, ^digest_c, _}, 1_000
    assert_receive {:result, {:ok, _}}, 1_000
  end

  test "runs open and put in the same task process and accepts {:ok, result}" do
    [digest] = digests(1)
    source = endpoint(chains: chains_for([digest]), lengths: lengths([digest]))
    target = endpoint(missing: [digest], lengths: lengths([digest]), put_result: {:ok, :installed})

    spawn_runner(source, target, documents(1), config(1, 100))
    assert_receive {:blob_opened, ^digest, pid}, 1_000
    assert_receive {:blob_put, ^digest, ^pid}, 1_000
    assert_receive {:result, {:ok, _}}, 1_000
  end

  test "blob failure cancels active sibling blob work" do
    digests = digests(2)

    source = endpoint(chains: chains_for(digests), lengths: lengths(digests))

    target = endpoint(missing: digests, lengths: lengths(digests), failed_blob: hd(digests))

    spawn_runner(source, target, documents(1), config(2, 100))
    assert_receive {:blob_opened, _, _}, 1_000
    assert_receive {:blob_opened, _, sibling_pid}, 1_000
    assert_receive {:blob_put, _, _}, 1_000
    assert_receive {:result, {:error, %Error{code: :internal_error}}}, 1_000
    refute Process.alive?(sibling_pid)
  end

  test "already installed and repeated durable blobs are safe no-ops" do
    [digest] = digests(1)
    source = endpoint(chains: chains_for([digest]), lengths: lengths([digest]))
    target = endpoint(missing: [], lengths: lengths([digest]))

    assert {:ok, _} =
             TransferPipeline.run(source, target, %{documents: documents(1)}, config(1, 100))

    refute_received {:blob_opened, _, _}
    refute_received {:blob_put, _, _}

    assert {:ok, _} =
             TransferPipeline.run(source, target, %{documents: documents(1)}, config(1, 100))

    refute_received {:blob_opened, _, _}
    refute_received {:blob_put, _, _}
  end

  test "supports distinct source and target endpoint modules" do
    [digest] = digests(1)
    source_inner = endpoint(chains: chains_for([digest]), lengths: lengths([digest]))
    target_inner = endpoint(missing: [digest], lengths: lengths([digest]))
    source = %SourceEndpoint{inner: source_inner}
    target = %TargetEndpoint{inner: target_inner}

    assert {:ok, _} =
             TransferPipeline.run(source, target, %{documents: documents(1)}, config(1, 100))

    assert_received {:blob_opened, ^digest, pid}
    assert_received {:blob_put, ^digest, ^pid}
  end

  test "overlaps blob transfer with a later blocked chain fetch" do
    [digest] = digests(1)

    source =
      endpoint(
        chains: chains_for([digest]) |> Map.put("doc-1", [%{document_id: "chain-1"}]),
        lengths: lengths([digest]),
        blocked_chains: ["doc-1"]
      )

    target = endpoint(missing: [digest], lengths: lengths([digest]))
    runner = spawn_runner(source, target, documents(2), config(2, 100))
    assert_receive {:chain_started, "doc-0", _}, 1_000
    assert_receive {:chain_started, "doc-1", blocked_pid}, 1_000
    assert_receive {:blob_opened, ^digest, blob_pid}, 1_000
    send(blob_pid, {:release_blob, digest})
    assert_receive {:blob_put, ^digest, _}, 1_000
    refute_received {:result, _}
    send(blocked_pid, {:release_chain, "doc-1"})
    assert_receive {:result, {:ok, _}}, 1_000
    refute Process.alive?(runner)
  end

  test "rejects conflicting digest lengths discovered across chain tasks" do
    [digest] = digests(1)

    chains = %{
      "doc-0" => [chain_with_attachment(digest, 10)],
      "doc-1" => [chain_with_attachment(digest, 11)]
    }

    source = endpoint(chains: chains, lengths: %{digest => 10})

    runner =
      spawn_runner(
        source,
        endpoint(missing: [digest], lengths: %{digest => 10}),
        documents(2),
        config(2, 100)
      )

    assert_receive {:chain_started, "doc-0", first}, 1_000
    assert_receive {:chain_started, "doc-1", second}, 1_000
    send(first, {:release_chain, "doc-0"})
    send(second, {:release_chain, "doc-1"})
    assert_receive {:result, {:error, %Error{code: :integrity_violation}}}, 1_000
    refute Process.alive?(runner)
  end

  defp receive_blob_opened do
    receive do
      {:blob_opened, digest, pid} -> {digest, pid}
    after
      1_000 -> flunk("blob task did not open")
    end
  end

  defp release_remaining_blob(digests, opened) do
    opened_digests = MapSet.new(opened, &elem(&1, 0))

    Enum.each(digests -- MapSet.to_list(opened_digests), fn digest ->
      receive do
        {:blob_opened, ^digest, pid} -> send(pid, {:release_blob, digest})
      after
        1_000 -> flunk("queued blob task did not open")
      end
    end)
  end

  defp spawn_runner(source, target, documents, config) do
    parent = self()

    spawn(fn ->
      send(parent, {:result, TransferPipeline.run(source, target, %{documents: documents}, config)})
    end)
  end

  defp endpoint(overrides) do
    struct!(
      FakeEndpoint,
      Keyword.merge([owner: self(), chains: %{}, lengths: %{}, put_result: :ok], overrides)
    )
  end

  defp config(blob_transfers, bytes) do
    %{
      "replication" => %{
        "batch_documents" => 10,
        "max_concurrent_chain_fetches" => 2,
        "max_concurrent_blob_transfers" => blob_transfers,
        "max_transfer_bytes_in_flight" => bytes
      }
    }
  end

  defp documents(count), do: Enum.map(0..(count - 1), &%{document_id: "doc-#{&1}"})

  defp chains_for(digests), do: chains_for(digests, lengths(digests))

  defp chains_for(digests, lengths),
    do: %{"doc-0" => [chain_with_attachments(digests, lengths)]}

  defp chain_with_attachments(digests, lengths),
    do: %{
      document_id: "chain",
      revisions: [%{attachments: Map.new(digests, &{&1, %{digest: &1, length: lengths[&1]}})}]
    }

  defp chain_with_attachment(digest, length),
    do: %{
      document_id: "chain",
      revisions: [%{attachments: %{"blob" => %{digest: digest, length: length}}}]
    }

  defp digests(count), do: Enum.map(0..(count - 1), &<<?a + &1, String.duplicate("0", 63)::binary>>)

  defp lengths(digests, length \\ 10), do: Map.new(digests, &{&1, length})
end
