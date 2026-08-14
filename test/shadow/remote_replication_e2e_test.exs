defmodule ElixirDB.Shadow.RemoteReplicationE2ETest do
  use ExUnit.Case, async: false

  alias ElixirDB.Attachments
  alias ElixirDB.Replication.Profile
  alias ElixirDB.Runtime.DatabaseCatalog
  alias ElixirDB.Shadow.{ReadRouter, RemoteEndpoint, Replicator, RouteTable, Worker}
  alias ElixirDB.TestServer

  @moduletag :integration

  setup do
    prefix = "shadow-remote-e2e-#{System.unique_integer([:positive])}"
    source_path = prefix <> ".elixirdb"
    worker_storage_root = prefix <> "-worker"
    source_uuid = ElixirDB.UUID.v4()
    shadow_uuid = ElixirDB.UUID.v4()
    operation_id = ElixirDB.UUID.v4()
    root = ElixirDB.Config.database_root()
    source_absolute = Path.join(root, source_path)
    source_blob_root = Path.join(source_absolute, "blobs")
    control_token = prefix <> "-control-token"
    control_token_digest = :crypto.hash(:sha256, control_token) |> Base.encode16(case: :lower)
    previous_worker = Application.get_env(:elixir_db, :shadow_worker, [])

    ElixirDB.TempDatabase.cleanup(source_absolute)

    assert {:ok, source} = DatabaseCatalog.create(source_path, %{database_uuid: source_uuid})
    server = TestServer.start_supervised!()

    Application.put_env(
      :elixir_db,
      :shadow_worker,
      enabled: true,
      storage_root: worker_storage_root,
      control_token_digests: [control_token_digest],
      allowed_attachment_roots: [source_absolute],
      allowed_source_origins: [server.base_url]
    )

    request = %{
      "source_uuid" => source_uuid,
      "shadow_uuid" => shadow_uuid,
      "generation" => 1,
      "operation_id" => operation_id,
      "attachment_store_type" => "external_cas",
      "attachment_location" => source_blob_root,
      "specification_digest" => String.duplicate("c", 64),
      "source_base_url" => server.base_url,
      "source_bearer_token" => nil
    }

    on_exit(fn ->
      _ = Worker.destroy(Map.take(request, ["source_uuid", "generation"]))
      Application.put_env(:elixir_db, :shadow_worker, previous_worker)
      _ = DatabaseCatalog.close(source_uuid)
      _ = DatabaseCatalog.unregister(source_uuid)
      ElixirDB.TempDatabase.cleanup(source_absolute)
      ElixirDB.TempDatabase.cleanup(Path.join(root, worker_storage_root))
      RouteTable.delete(source_uuid)
    end)

    {:ok, source: source, request: request, control_token: control_token, server: server}
  end

  test "remote shadow pull serves documents and external-CAS attachments", %{
    source: source,
    request: request,
    control_token: control_token,
    server: server
  } do
    assert {:ok, remote} =
             RemoteEndpoint.new(%{
               "base_url" => server.base_url,
               "auth_token" => control_token,
               "control_timeout_ms" => 5_000,
               "read_timeout_ms" => 5_000
             })

    assert {:ok, %{"protocol_major" => 1}} = RemoteEndpoint.capabilities(remote, 5_000)
    assert {:ok, %{"state" => "bootstrapping"}} = RemoteEndpoint.provision(remote, request, 5_000)

    payload = String.duplicate("remote-shadow-attachment-", 2_048)

    assert {:ok, %{blob: digest, length: _length}} =
             Attachments.upload_stream(source.database_uuid, [payload])

    assert {:ok, %{revision: revision}} =
             ElixirDB.Documents.put(source.database_uuid, %{
               "id" => "attached",
               "body" => %{"source" => "remote"},
               "attachments" => %{
                 "file.bin" => %{blob: digest, content_type: "text/plain"}
               }
             })

    profile =
      Profile.shadow(
        source_database_uuid: source.database_uuid,
        target_database_uuid: request["shadow_uuid"],
        generation: request["generation"],
        operation_id: request["operation_id"]
      )

    assert {:ok, %{status: :completed, source_sequence: sequence}} =
             Replicator.pull(request, mode: "one_shot")

    assert :ok =
             Worker.mark_ready(
               Map.take(request, ~w(source_uuid shadow_uuid generation operation_id)),
               sequence
             )

    assert {:ok, %{"state" => "ready"}} = RemoteEndpoint.inspect(remote, request, 5_000)

    assert {:ok, %{"document" => %{"revision" => ^revision}}} =
             RemoteEndpoint.read_document(
               remote,
               Map.put(
                 Map.take(request, ~w(source_uuid shadow_uuid generation operation_id)),
                 "id",
                 "attached"
               ),
               5_000,
               []
             )

    assert :ok =
             RouteTable.put(source.database_uuid, %{
               endpoint: remote,
               source_uuid: source.database_uuid,
               shadow_uuid: request["shadow_uuid"],
               generation: request["generation"],
               operation_id: request["operation_id"],
               worker_node_id: "remote-test-worker",
               applied_source_sequence: sequence
             })

    assert {:ok, %{body: body}, %{served_by: "shadow", source_watermark: ^sequence}} =
             ReadRouter.get(source.database_uuid, %{id: "attached"},
               read_consistency: :eventual,
               primary: fn _ -> flunk("eventual read should use the remote shadow") end
             )

    assert body == %{"source" => "remote"}

    assert {:ok, %{content_type: "text/plain", etag: etag, body: body}, meta} =
             Attachments.open_stream_with_meta(
               source.database_uuid,
               %{
                 "id" => "attached",
                 "name" => "file.bin"
               },
               read_consistency: :eventual
             )

    assert etag == ~s("#{digest}")
    assert meta == %{served_by: "shadow", source_watermark: sequence}
    assert IO.iodata_to_binary(Enum.to_list(body)) == payload

    assert profile.kind == :shadow
  end
end
