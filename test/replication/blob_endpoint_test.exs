defmodule ElixirDB.Replication.BlobEndpointTest do
  @moduledoc """
  Local and remote replication blob endpoint round-trips.
  """
  use ExUnit.Case, async: false

  @moduletag :integration

  alias ElixirDB.Attachments
  alias ElixirDB.Attachments.FilesystemStore
  alias ElixirDB.Attachments.Manifest
  alias ElixirDB.Eventual
  alias ElixirDB.Replication.{BlobRepresentationStream, LocalEndpoint, RemoteEndpoint}
  alias ElixirDB.Runtime.{AttachmentCoordinator, DatabaseCatalog}
  alias ElixirDB.TestServer

  setup do
    path = "blob-endpoint-#{System.unique_integer([:positive])}.elixirdb"
    absolute = Path.join(ElixirDB.Config.database_root(), path)
    ElixirDB.TempDatabase.cleanup(absolute)

    assert {:ok, identity} = DatabaseCatalog.create(path)
    uuid = identity.database_uuid
    assert {:ok, _} = DatabaseCatalog.open(uuid)

    on_exit(fn ->
      _ = DatabaseCatalog.close(uuid)
      _ = DatabaseCatalog.unregister(uuid)
      ElixirDB.TempDatabase.cleanup(absolute)
    end)

    {:ok, endpoint} = LocalEndpoint.new(uuid)
    server = TestServer.start_supervised!()

    {:ok, uuid: uuid, endpoint: endpoint, base_url: server.base_url, path: path}
  end

  test "local diff/open/put round-trip", %{endpoint: endpoint, uuid: uuid} do
    bytes = "replication-blob-payload"
    digest = sha256_hex(bytes)
    missing_digest = sha256_hex("absent")

    assert {:ok, [^digest, ^missing_digest]} =
             LocalEndpoint.diff_blobs(endpoint, [digest, missing_digest])

    assert {:ok, stream} = raw_stream(digest, bytes)
    assert :ok = LocalEndpoint.put_blob_representation(endpoint, stream)

    assert {:ok, [^missing_digest]} =
             LocalEndpoint.diff_blobs(endpoint, [digest, missing_digest])

    assert {:ok, opened} = LocalEndpoint.open_blob_representation(endpoint, digest)
    assert opened.logical_digest == digest
    assert opened.logical_length == byte_size(bytes)
    assert opened.encoding == :raw
    assert IO.iodata_to_binary(Enum.to_list(opened.body)) == bytes

    assert {:ok, meta} =
             DatabaseCatalog.command(uuid, {:command, :resolve_blob_metadata, %{digest: digest}})

    assert Manifest.validate_digest(meta.digest || meta[:digest] || digest) == {:ok, digest}
  end

  test "local put rejects digest mismatch", %{endpoint: endpoint} do
    bytes = "actual-bytes"
    wrong = sha256_hex("other-bytes")

    assert {:ok, stream} = raw_stream(wrong, bytes)

    assert {:error, %ElixirDB.Error{code: :integrity_violation}} =
             LocalEndpoint.put_blob_representation(endpoint, stream)
  end

  test "remote wire diff/open/put matches local", %{uuid: uuid, base_url: base_url} do
    bytes = "remote-wire-blob"
    digest = sha256_hex(bytes)
    other = sha256_hex("not-present")

    {:ok, remote} =
      RemoteEndpoint.new(%{
        "kind" => "remote",
        "base_url" => base_url,
        "database_uuid" => uuid
      })

    assert {:ok, [^digest, ^other]} = RemoteEndpoint.diff_blobs(remote, [digest, other])

    assert {:ok, stream} = raw_stream(digest, bytes)
    assert :ok = RemoteEndpoint.put_blob_representation(remote, stream)

    assert {:ok, [^other]} = RemoteEndpoint.diff_blobs(remote, [digest, other])

    assert {:ok, opened} = RemoteEndpoint.open_blob_representation(remote, digest)
    assert opened.logical_digest == digest
    assert opened.logical_length == byte_size(bytes)
    assert IO.iodata_to_binary(Enum.to_list(opened.body)) == bytes
  end

  test "wire GET missing digest returns attachment_blob_not_found", %{
    uuid: uuid,
    base_url: base_url
  } do
    digest = sha256_hex("missing-wire")

    assert {:ok, %{status: 404, headers: headers, body: body}} =
             Req.get(base_url <> "/v1/databases/#{uuid}/replication/blobs/#{digest}",
               headers: [{"accept-encoding", "zstd"}],
               decode_body: false,
               compressed: false
             )

    decoded = ElixirDB.TestReplicationWire.decode_response(headers, body)
    assert decoded["error"]["code"] == "attachment_blob_not_found"
  end

  test "Attachments helpers stay equivalent to LocalEndpoint", %{uuid: uuid, endpoint: endpoint} do
    bytes = "helper-equivalence"
    digest = sha256_hex(bytes)

    assert {:ok, stream} = raw_stream(digest, bytes)
    assert :ok = Attachments.put_blob_representation(uuid, stream)

    assert LocalEndpoint.diff_blobs(endpoint, [digest]) == Attachments.diff_blobs(uuid, [digest])
    assert {:ok, []} = Attachments.diff_blobs(uuid, [digest])
  end

  test "slow blob transfer leaves source and target owners available", %{
    uuid: source_uuid,
    endpoint: source_endpoint
  } do
    target_path = "blob-transfer-target-#{System.unique_integer([:positive])}.elixirdb"
    target_absolute = Path.join(ElixirDB.Config.database_root(), target_path)
    ElixirDB.TempDatabase.cleanup(target_absolute)

    assert {:ok, target_identity} = DatabaseCatalog.create(target_path)
    target_uuid = target_identity.database_uuid
    assert {:ok, _} = DatabaseCatalog.open(target_uuid)

    on_exit(fn ->
      _ = DatabaseCatalog.close(target_uuid)
      _ = DatabaseCatalog.unregister(target_uuid)
      ElixirDB.TempDatabase.cleanup(target_absolute)
    end)

    {:ok, target_endpoint} = LocalEndpoint.new(target_uuid)
    payload = :binary.copy("replication-transfer-", 16_384)
    digest = sha256_hex(payload)
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

        LocalEndpoint.put_blob_representation(target_endpoint, %{source_stream | body: gated_body})
      end)

    assert_receive {:blob_transfer_blocked, ^gate}, 2_000

    assert Eventual.eventually(
             fn ->
               source_status = AttachmentCoordinator.status(source_uuid)
               target_status = AttachmentCoordinator.status(target_uuid)

               match?(%{active_reads: 1}, source_status) and
                 match?(%{active_writes: 1}, target_status)
             end,
             timeout: 1_000,
             message: "target attachment write was not admitted before the transfer blocked"
           )

    assert {:ok, _} =
             ElixirDB.Documents.put(source_uuid, %{
               "id" => "source-during-transfer",
               "body" => %{"ok" => true}
             })

    assert {:ok, _} =
             ElixirDB.Documents.put(target_uuid, %{
               "id" => "target-during-transfer",
               "body" => %{"ok" => true}
             })

    send(transfer.pid, {:release, gate})
    assert :ok = Task.await(transfer, 10_000)
    assert {:ok, []} = Attachments.diff_blobs(target_uuid, [digest])

    {:ok, source_bundle} = DatabaseCatalog.bundle_root(source_uuid)
    {:ok, target_bundle} = DatabaseCatalog.bundle_root(target_uuid)

    assert representation_file_bytes!(source_bundle, digest) ==
             representation_file_bytes!(target_bundle, digest)
  end

  test "local transfer preserves raw and zstd payload bytes without probing", %{
    uuid: source_uuid,
    endpoint: source_endpoint
  } do
    cases = [
      {:raw, :crypto.strong_rand_bytes(4_096)},
      {:zstd, String.duplicate("compress-me-please-", 4_096)}
    ]

    Enum.each(cases, fn {expected_encoding, payload} ->
      digest = sha256_hex(payload)
      assert {:ok, %{blob: ^digest}} = Attachments.upload_stream(source_uuid, [payload])

      {:ok, source_bundle} = DatabaseCatalog.bundle_root(source_uuid)
      assert {:ok, %{encoding: ^expected_encoding}} = FilesystemStore.stat(source_bundle, digest)
      source_bytes = representation_file_bytes!(source_bundle, digest)

      target_path = "blob-copy-#{expected_encoding}-#{System.unique_integer([:positive])}.elixirdb"
      target_absolute = Path.join(ElixirDB.Config.database_root(), target_path)
      ElixirDB.TempDatabase.cleanup(target_absolute)
      assert {:ok, target_identity} = DatabaseCatalog.create(target_path)
      target_uuid = target_identity.database_uuid
      assert {:ok, _} = DatabaseCatalog.open(target_uuid)

      on_exit(fn ->
        _ = DatabaseCatalog.close(target_uuid)
        _ = DatabaseCatalog.unregister(target_uuid)
        ElixirDB.TempDatabase.cleanup(target_absolute)
      end)

      {:ok, target_endpoint} = LocalEndpoint.new(target_uuid)

      probes =
        count_probes(fn ->
          assert {:ok, stream} = LocalEndpoint.open_blob_representation(source_endpoint, digest)
          assert stream.encoding == expected_encoding
          assert :ok = LocalEndpoint.put_blob_representation(target_endpoint, stream)
        end)

      assert probes == 0

      {:ok, target_bundle} = DatabaseCatalog.bundle_root(target_uuid)
      assert representation_file_bytes!(target_bundle, digest) == source_bytes
      assert {:ok, %{encoding: ^expected_encoding}} = FilesystemStore.stat(target_bundle, digest)
    end)
  end

  defp raw_stream(digest, bytes) do
    BlobRepresentationStream.new(%{
      logical_digest: digest,
      logical_length: byte_size(bytes),
      format_version: 1,
      encoding: :raw,
      payload_length: byte_size(bytes),
      payload_sha256: digest,
      body: [bytes]
    })
  end

  defp representation_file_bytes!(bundle, digest) do
    path = Path.join([bundle, "blobs", String.slice(digest, 0, 2), digest <> ".blob"])
    File.read!(path)
  end

  defp count_probes(fun) do
    parent = self()
    :erlang.trace_pattern({ElixirDB.Attachments.Compression, :probe, 1}, true, [])
    :erlang.trace(:existing, true, [:call, :set_on_spawn])
    :erlang.trace(parent, false, [:call])

    try do
      fun.()
      receive_probes(0, 50)
    after
      :erlang.trace(:all, false, [:call])
      :erlang.trace_pattern({ElixirDB.Attachments.Compression, :probe, 1}, false, [])
      flush_traces()
    end
  end

  defp receive_probes(count, remaining) when remaining <= 0, do: count

  defp receive_probes(count, remaining) do
    receive do
      {:trace, _pid, :call, {ElixirDB.Attachments.Compression, :probe, _}} ->
        receive_probes(count + 1, remaining)

      {:trace, _pid, :call, _} ->
        receive_probes(count, remaining)
    after
      20 ->
        receive_probes(count, remaining - 1)
    end
  end

  defp flush_traces do
    receive do
      {:trace, _, _, _} -> flush_traces()
    after
      0 -> :ok
    end
  end

  defp sha256_hex(bytes) do
    :crypto.hash(:sha256, bytes) |> Base.encode16(case: :lower)
  end
end
