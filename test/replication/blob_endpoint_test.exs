defmodule ElixirDB.Replication.BlobEndpointTest do
  @moduledoc """
  Local and remote replication blob endpoint round-trips.
  """
  use ExUnit.Case, async: false

  alias ElixirDB.Attachments
  alias ElixirDB.Attachments.Manifest
  alias ElixirDB.Eventual
  alias ElixirDB.Replication.{BlobStream, LocalEndpoint, RemoteEndpoint}
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

    assert {:ok, stream} = BlobStream.new(digest, byte_size(bytes), [bytes])
    assert :ok = LocalEndpoint.put_blob(endpoint, stream)

    assert {:ok, [^missing_digest]} =
             LocalEndpoint.diff_blobs(endpoint, [digest, missing_digest])

    assert {:ok, opened} = LocalEndpoint.open_blob(endpoint, digest)
    assert opened.digest == digest
    assert opened.length == byte_size(bytes)
    assert IO.iodata_to_binary(Enum.to_list(opened.body)) == bytes

    assert {:ok, meta} =
             DatabaseCatalog.command(uuid, {:command, :resolve_blob_metadata, %{digest: digest}})

    assert Manifest.validate_digest(meta.digest || meta[:digest] || digest) == {:ok, digest}
  end

  test "local put rejects digest mismatch", %{endpoint: endpoint} do
    bytes = "actual-bytes"
    wrong = sha256_hex("other-bytes")

    assert {:ok, stream} = BlobStream.new(wrong, byte_size(bytes), [bytes])

    assert {:error, %ElixirDB.Error{code: :integrity_violation}} =
             LocalEndpoint.put_blob(endpoint, stream)
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

    assert {:ok, stream} = BlobStream.new(digest, byte_size(bytes), [bytes])
    assert :ok = RemoteEndpoint.put_blob(remote, stream)

    assert {:ok, [^other]} = RemoteEndpoint.diff_blobs(remote, [digest, other])

    assert {:ok, opened} = RemoteEndpoint.open_blob(remote, digest)
    assert opened.digest == digest
    assert opened.length == byte_size(bytes)
    assert IO.iodata_to_binary(Enum.to_list(opened.body)) == bytes
  end

  test "wire GET missing digest returns attachment_blob_not_found", %{
    uuid: uuid,
    base_url: base_url
  } do
    digest = sha256_hex("missing-wire")

    assert {:ok, %{status: 404, body: body}} =
             Req.get(base_url <> "/v1/databases/#{uuid}/replication/blobs/#{digest}",
               decode_body: true
             )

    assert body["error"]["code"] == "attachment_blob_not_found"
  end

  test "Attachments helpers stay equivalent to LocalEndpoint", %{uuid: uuid, endpoint: endpoint} do
    bytes = "helper-equivalence"
    digest = sha256_hex(bytes)

    assert {:ok, stream} = BlobStream.new(digest, byte_size(bytes), [bytes])
    assert :ok = Attachments.put_blob(uuid, stream)
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

    assert {:ok, %{blob: ^digest, length: length}} =
             Attachments.upload_stream(source_uuid, [payload])

    transfer =
      Task.async(fn ->
        assert {:ok, source_stream} = LocalEndpoint.open_blob(source_endpoint, digest)

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

        assert {:ok, target_stream} = BlobStream.new(digest, length, gated_body)
        LocalEndpoint.put_blob(target_endpoint, target_stream)
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

    assert {:ok, target_blob} = Attachments.open_blob(target_uuid, digest)
    assert Enum.into(target_blob.body, <<>>) == payload
  end

  defp sha256_hex(bytes) do
    :crypto.hash(:sha256, bytes) |> Base.encode16(case: :lower)
  end
end
