defmodule ElixirDB.Runtime.AdmissionNonRetentionTest do
  @moduledoc """
  Long-lived waits and byte streams must not retain an owner permit.
  """
  use ExUnit.Case, async: false

  @moduletag :integration

  alias ElixirDB.Attachments
  alias ElixirDB.Eventual
  alias ElixirDB.JSON.StrictDecoder
  alias ElixirDB.Query.Subscriptions
  alias ElixirDB.Replication.LocalEndpoint
  alias ElixirDB.Runtime.{ChangeNotifier, DatabaseAdmission, DatabaseCatalog}
  alias ElixirDB.TestServer

  setup _context do
    {:ok, server: TestServer.start_supervised!()}
  end

  defp open_database!(suffix) do
    rel = "admission-nonret-#{suffix}-#{System.unique_integer([:positive])}.elixirdb"
    absolute = Path.join(ElixirDB.Config.database_root(), rel)
    ElixirDB.TempDatabase.cleanup(absolute)

    assert {:ok, identity} = DatabaseCatalog.create(rel)
    uuid = identity.database_uuid
    assert {:ok, _} = DatabaseCatalog.open(uuid)

    on_exit(fn ->
      _ = DatabaseCatalog.close(uuid)
      _ = DatabaseCatalog.unregister(uuid)
      ElixirDB.TempDatabase.cleanup(absolute)
    end)

    uuid
  end

  defp assert_no_active_permit(uuid, alive_pid, message) do
    Eventual.eventually(
      fn -> permit_released_while_alive?(uuid, alive_pid) end,
      timeout: 2_000,
      message: message
    )
  end

  defp permit_released_while_alive?(uuid, pid) when is_pid(pid) do
    with {:ok, 0} <- DatabaseAdmission.active_count(uuid),
         true <- Process.alive?(pid) do
      :ok
    else
      _ -> false
    end
  end

  defp permit_released_while_alive?(_uuid, _pid), do: false

  test "changes long-poll wait does not retain an owner permit" do
    uuid = open_database!("long-poll")

    waiter =
      Task.async(fn ->
        ElixirDB.Changes.wait(uuid, %{since: 999_999, limit: 1, wait_ms: 5_000})
      end)

    Eventual.eventually(
      fn ->
        case ChangeNotifier.subscriber_count(uuid) do
          n when is_integer(n) and n >= 1 -> :ok
          _ -> false
        end
      end,
      timeout: 2_000,
      message: "changes long-poll did not enter wait after bounded read"
    )

    assert_no_active_permit(
      uuid,
      waiter.pid,
      "changes long-poll retained an admission permit"
    )

    Task.shutdown(waiter, :brutal_kill)
  end

  test "NDJSON changes stream heartbeat wait does not retain an owner permit", %{server: server} do
    uuid = open_database!("ndjson")
    parent = self()
    url = server.base_url <> "/v1/databases/#{uuid}/changes/stream"

    assert {:ok, _} =
             DatabaseCatalog.command(
               uuid,
               {:command, :put, %{document_id: "doc", body: %{"n" => 1}}}
             )

    stream =
      Task.async(fn ->
        collect_stream_events(url, %{"since" => 0, "limit" => 10, "heartbeat_ms" => 50}, parent)
      end)

    assert_receive {:ndjson_event, %{"type" => "heartbeat"}}, 5_000

    assert_no_active_permit(
      uuid,
      stream.pid,
      "NDJSON stream wait retained an admission permit"
    )

    Task.shutdown(stream, :brutal_kill)
  end

  test "live query subscription heartbeat wait does not retain an owner permit" do
    uuid = open_database!("subscription")

    assert {:ok, pid} =
             Subscriptions.open(
               uuid,
               %{"query" => %{"selector" => %{"/type" => "missing"}}, "heartbeat_ms" => 50},
               self()
             )

    assert {:ok, %{type: :caught_up}} = Subscriptions.next(pid, 5_000)

    heartbeat_waiter =
      Task.async(fn ->
        Subscriptions.next(pid, 5_000)
      end)

    assert {:ok, %{type: :heartbeat}} = Task.await(heartbeat_waiter, 5_000)

    next_waiter =
      Task.async(fn ->
        Subscriptions.next(pid, 5_000)
      end)

    assert_no_active_permit(
      uuid,
      next_waiter.pid,
      "subscription heartbeat wait retained an admission permit"
    )

    Task.shutdown(next_waiter, :brutal_kill)
    Subscriptions.close(pid)
  end

  test "slow attachment upload does not retain an owner permit" do
    uuid = open_database!("upload")
    parent = self()
    gate = make_ref()

    source = fn ->
      {:ok, "hello-",
       fn ->
         send(parent, {:upload_blocked, gate})

         receive do
           {:release, ^gate} -> {:ok, "world", fn -> :done end}
         after
           10_000 -> {:error, :timeout}
         end
       end}
    end

    upload = Task.async(fn -> Attachments.upload_stream(uuid, source) end)
    assert_receive {:upload_blocked, ^gate}, 2_000

    assert_no_active_permit(uuid, upload.pid, "attachment upload retained an admission permit")

    send(upload.pid, {:release, gate})
    assert {:ok, _} = Task.await(upload, 10_000)
  end

  test "slow attachment download does not retain an owner permit" do
    uuid = open_database!("download")
    parent = self()
    gate = make_ref()
    payload = "download-nonret-#{System.unique_integer([:positive])}"

    assert {:ok, %{blob: blob}} = Attachments.upload_stream(uuid, [payload])

    assert {:ok, put} =
             ElixirDB.Documents.put(uuid, %{
               "id" => "dl-doc",
               "body" => %{},
               "attachments" => %{
                 "a.bin" => %{"blob" => blob, "content_type" => "application/octet-stream"}
               }
             })

    revision = put.revision

    download =
      Task.async(fn ->
        assert {:ok, stream} =
                 Attachments.open_stream(uuid, %{
                   "id" => "dl-doc",
                   "revision" => revision,
                   "name" => "a.bin"
                 })

        {[first], rest} = Enum.split(stream.body, 1)
        send(parent, {:download_blocked, gate, first})

        receive do
          {:release, ^gate} -> Enum.into(rest, <<>>)
        after
          10_000 -> flunk("download was not released")
        end
      end)

    assert_receive {:download_blocked, ^gate, first_chunk}, 2_000
    assert is_binary(first_chunk)

    assert_no_active_permit(uuid, download.pid, "attachment download retained an admission permit")

    send(download.pid, {:release, gate})
    remainder = Task.await(download, 10_000)
    assert first_chunk <> remainder == payload
  end

  test "slow replication blob transfer does not retain an owner permit" do
    source_uuid = open_database!("repl-source")
    target_uuid = open_database!("repl-target")

    {:ok, source_endpoint} = LocalEndpoint.new(source_uuid)
    {:ok, target_endpoint} = LocalEndpoint.new(target_uuid)

    payload = :binary.copy("replication-transfer-", 16_384)
    digest = :crypto.hash(:sha256, payload) |> Base.encode16(case: :lower)
    parent = self()
    gate = make_ref()

    assert {:ok, %{blob: ^digest}} = Attachments.upload_stream(source_uuid, [payload])

    transfer =
      Task.async(fn ->
        assert {:ok, source_stream} =
                 LocalEndpoint.open_blob_representation(source_endpoint, digest)

        gated_body =
          Stream.transform(source_stream.body, false, fn chunk, blocked? ->
            if blocked? do
              {[chunk], true}
            else
              send(parent, {:blob_transfer_blocked, gate})

              receive do
                {:release, ^gate} -> {[chunk], true}
              after
                10_000 -> flunk("blob transfer was not released")
              end
            end
          end)

        LocalEndpoint.put_blob_representation(
          target_endpoint,
          %{source_stream | body: gated_body}
        )
      end)

    assert_receive {:blob_transfer_blocked, ^gate}, 2_000

    assert_no_active_permit(
      source_uuid,
      transfer.pid,
      "replication blob transfer retained a source admission permit"
    )

    assert_no_active_permit(
      target_uuid,
      transfer.pid,
      "replication blob transfer retained a target admission permit"
    )

    send(transfer.pid, {:release, gate})
    assert :ok = Task.await(transfer, 10_000)
  end

  test "concurrent replication blob transfers do not retain owner permits" do
    source_uuid = open_database!("repl-multi-source")
    target_uuid = open_database!("repl-multi-target")

    {:ok, source_endpoint} = LocalEndpoint.new(source_uuid)
    {:ok, target_endpoint} = LocalEndpoint.new(target_uuid)

    parent = self()
    gate = make_ref()
    transfer_count = 3

    digests =
      for index <- 1..transfer_count do
        payload = :binary.copy("replication-transfer-#{index}-", 8_192)
        digest = :crypto.hash(:sha256, payload) |> Base.encode16(case: :lower)
        assert {:ok, %{blob: ^digest}} = Attachments.upload_stream(source_uuid, [payload])
        {digest, byte_size(payload)}
      end

    transfers =
      Enum.map(digests, fn {digest, _size} ->
        Task.async(fn ->
          assert {:ok, source_stream} =
                   LocalEndpoint.open_blob_representation(source_endpoint, digest)

          gated_body =
            Stream.transform(source_stream.body, false, fn chunk, blocked? ->
              if blocked? do
                {[chunk], true}
              else
                send(parent, {:blob_transfer_blocked, gate, digest})

                receive do
                  {:release, ^gate} -> {[chunk], true}
                after
                  10_000 -> flunk("blob transfer was not released")
                end
              end
            end)

          LocalEndpoint.put_blob_representation(
            target_endpoint,
            %{source_stream | body: gated_body}
          )
        end)
      end)

    for _ <- 1..transfer_count do
      assert_receive {:blob_transfer_blocked, ^gate, _digest}, 2_000
    end

    for transfer <- transfers, uuid <- [source_uuid, target_uuid] do
      assert_no_active_permit(
        uuid,
        transfer.pid,
        "concurrent replication transfers retained an admission permit on #{uuid}"
      )
    end

    for transfer <- transfers do
      send(transfer.pid, {:release, gate})
    end

    for transfer <- transfers do
      assert :ok = Task.await(transfer, 10_000)
    end
  end

  defp collect_stream_events(url, body, parent) do
    case Req.post(url,
           json: body,
           receive_timeout: 15_000,
           into: fn
             {:data, data}, {req, resp} ->
               for line <- String.split(IO.iodata_to_binary(data), "\n", trim: true) do
                 case StrictDecoder.decode(line) do
                   {:ok, event} when is_map(event) -> send(parent, {:ndjson_event, event})
                   _ -> :ok
                 end
               end

               {:cont, {req, resp}}

             {:trailer, _}, acc ->
               {:cont, acc}
           end
         ) do
      {:ok, _resp} -> :ok
      {:error, reason} -> send(parent, {:ndjson_stream_error, reason})
    end
  end
end
