defmodule ElixirDB.Replication.TransferPipelineBlobsTest do
  use ExUnit.Case, async: true

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
      :diff_result,
      :open_result,
      :stream_failure,
      :durable_digests,
      :lost_response,
      :put_result
    ]

    def get_revision_chains(endpoint, %{documents: [%{document_id: document_id}]}) do
      send(endpoint.owner, {:chain_started, document_id, self()})
      wait_for(endpoint.blocked_chains, {:release_chain, document_id})
      {:ok, %{chains: Map.get(endpoint.chains, document_id, [])}}
    end

    def diff_blobs(endpoint, digests) do
      send(endpoint.owner, {:diff_started, digests, self()})

      missing =
        case endpoint.durable_digests do
          nil ->
            endpoint.missing || digests

          agent ->
            installed = Agent.get(agent, fn value -> value end)
            Enum.reject(endpoint.missing || digests, &MapSet.member?(installed, &1))
        end

      endpoint.diff_result || {:ok, missing}
    end

    def open_blob(endpoint, digest) do
      send(endpoint.owner, {:blob_opened, digest, self()})
      wait_for(endpoint.blocked_blobs, {:release_blob, digest})

      case endpoint.open_result do
        nil ->
          body =
            case endpoint.stream_failure do
              :source -> Stream.concat([<<"partial">>, Stream.map([:fail], &raise_stream/1)])
              _ -> [<<"chunk">>]
            end

          BlobStream.new(digest, endpoint.lengths[digest], body)

        result ->
          result
      end
    end

    def put_blob(endpoint, stream) do
      send(endpoint.owner, {:blob_put, stream.digest, self()})

      cond do
        endpoint.failed_blob == stream.digest ->
          {:error, Error.internal_error("fake blob failure")}

        endpoint.lost_response == true and not is_nil(endpoint.durable_digests) ->
          durable_install(endpoint.durable_digests, stream.digest)

        endpoint.put_result == :ok ->
          consume_stream(stream.body, endpoint.stream_failure)

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

    defp raise_stream(_value), do: raise("source stream failure")

    defp durable_install(agent, digest) do
      first_install =
        Agent.get_and_update(agent, fn installed ->
          if MapSet.member?(installed, digest) do
            {false, installed}
          else
            {true, MapSet.put(installed, digest)}
          end
        end)

      if first_install,
        do: {:error, Error.internal_error("durable install response lost")},
        else: :ok
    end

    defp consume_stream(body, :target),
      do: Enum.each(body, fn _chunk -> raise "target stream failure" end)

    defp consume_stream(body, _failure), do: Enum.each(body, & &1)
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
    failed_digest = hd(digests)

    source = endpoint(chains: chains_for(digests), lengths: lengths(digests))

    target = endpoint(missing: digests, lengths: lengths(digests), failed_blob: hd(digests))

    spawn_runner(source, target, documents(1), config(2, 100))
    opened = Map.new([receive_blob_opened(), receive_blob_opened()])
    assert MapSet.new(Map.keys(opened)) == MapSet.new(digests)
    failed_pid = Map.fetch!(opened, failed_digest)
    sibling_pid = Map.fetch!(opened, List.last(digests))
    assert_receive {:blob_put, ^failed_digest, ^failed_pid}, 1_000
    assert_receive {:result, {:error, %Error{code: :internal_error}}}, 1_000
    refute Process.alive?(failed_pid)
    refute Process.alive?(sibling_pid)
  end

  test "explicit cancellation cleans active blob tasks" do
    [digest] = digests(1)

    source =
      endpoint(
        chains: chains_for([digest]),
        lengths: lengths([digest]),
        blocked_blobs: [digest]
      )

    target = endpoint(missing: [digest], lengths: lengths([digest]))
    runner = spawn_runner(source, target, documents(1), config(1, 100))
    assert_receive {:blob_opened, ^digest, blob_pid}, 1_000
    blob_monitor = Process.monitor(blob_pid)

    assert :ok = TransferPipeline.cancel(runner)
    assert_receive {:result, {:error, %Error{code: :database_closed}}}, 1_000
    assert_receive {:DOWN, ^blob_monitor, :process, ^blob_pid, _reason}, 1_000
    refute Process.alive?(runner)
  end

  test "normalizes blob diff and source-open failures" do
    [digest] = digests(1)
    source = endpoint(chains: chains_for([digest]), lengths: lengths([digest]))

    assert {:error, %Error{code: :internal_error}} =
             TransferPipeline.run(
               source,
               endpoint(diff_result: {:error, Error.internal_error("diff failed")}),
               %{documents: documents(1)},
               config(1, 100)
             )

    assert {:error, %Error{code: :internal_error}} =
             TransferPipeline.run(
               endpoint(
                 chains: chains_for([digest]),
                 lengths: lengths([digest]),
                 open_result: {:error, Error.internal_error("open failed")}
               ),
               endpoint(missing: [digest], lengths: lengths([digest])),
               %{documents: documents(1)},
               config(1, 100)
             )
  end

  test "deduplicates repeated missing blob digests" do
    [digest] = digests(1)
    source = endpoint(chains: chains_for([digest]), lengths: lengths([digest]))

    target =
      endpoint(
        diff_result: {:ok, [digest, digest]},
        lengths: lengths([digest])
      )

    assert {:ok, _} =
             TransferPipeline.run(source, target, %{documents: documents(1)}, config(1, 100))

    assert_receive {:blob_opened, ^digest, pid}, 1_000
    assert_receive {:blob_put, ^digest, ^pid}, 1_000
    refute_received {:blob_opened, ^digest, _}
    refute_received {:blob_put, ^digest, _}
  end

  test "normalizes mid-source and mid-target stream failures" do
    [digest] = digests(1)
    chains = chains_for([digest])

    assert {:error, %Error{code: :internal_error}} =
             TransferPipeline.run(
               endpoint(chains: chains, lengths: lengths([digest]), stream_failure: :source),
               endpoint(missing: [digest], lengths: lengths([digest])),
               %{documents: documents(1)},
               config(1, 100)
             )

    assert {:error, %Error{code: :internal_error}} =
             TransferPipeline.run(
               endpoint(chains: chains, lengths: lengths([digest])),
               endpoint(
                 missing: [digest],
                 lengths: lengths([digest]),
                 stream_failure: :target
               ),
               %{documents: documents(1)},
               config(1, 100)
             )
  end

  test "reuses a durable blob after the install response is lost" do
    [digest] = digests(1)
    {:ok, installed} = Agent.start_link(fn -> MapSet.new() end)
    source = endpoint(chains: chains_for([digest]), lengths: lengths([digest]))

    target =
      endpoint(
        missing: [digest],
        lengths: lengths([digest]),
        durable_digests: installed,
        lost_response: true
      )

    assert {:error, %Error{code: :internal_error}} =
             TransferPipeline.run(source, target, %{documents: documents(1)}, config(1, 100))

    assert_receive {:diff_started, [^digest], _}, 1_000
    assert_receive {:blob_opened, ^digest, _}, 1_000
    assert_receive {:blob_put, ^digest, _}, 1_000

    assert {:ok, _} =
             TransferPipeline.run(source, target, %{documents: documents(1)}, config(1, 100))

    assert_receive {:diff_started, [^digest], _}, 1_000
    refute_received {:blob_opened, ^digest, _}
    refute_received {:blob_put, ^digest, _}
    assert MapSet.member?(Agent.get(installed, & &1), digest)
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
