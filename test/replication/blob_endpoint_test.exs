defmodule ElixirDB.Replication.BlobEndpointTest do
  @moduledoc """
  Local and remote replication blob endpoint round-trips (plan §15 / Wave 3 B+C).
  """
  use ExUnit.Case, async: false

  alias ElixirDB.Attachments
  alias ElixirDB.Attachments.Manifest
  alias ElixirDB.Replication.{BlobStream, LocalEndpoint, RemoteEndpoint}
  alias ElixirDB.Runtime.DatabaseCatalog
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

  defp sha256_hex(bytes) do
    :crypto.hash(:sha256, bytes) |> Base.encode16(case: :lower)
  end
end
