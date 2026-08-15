defmodule VialKeeper.Shadow.RemoteLifecycleE2ETest do
  use ExUnit.Case, async: false

  alias VialKeeper.Runtime.DatabaseCatalog
  alias VialKeeper.Shadow.RemoteEndpoint
  alias VialKeeper.TestServer

  @moduletag :integration

  test "remote provision inspect and destroy use one compressed control generation" do
    prefix = "shadow-remote-life-#{System.unique_integer([:positive])}"
    control_token = prefix <> "-token"
    digest = :crypto.hash(:sha256, control_token) |> Base.encode16(case: :lower)
    previous = Application.get_env(:vial_keeper, :shadow_worker, [])
    root = VialKeeper.Config.database_root()
    source_path = prefix <> "-source.vialkeeper"
    source_uuid = VialKeeper.UUID.v4()
    attachment_location = Path.join(root, source_path <> "/blobs")
    server = TestServer.start_supervised!()

    Application.put_env(:vial_keeper, :shadow_worker,
      enabled: true,
      storage_root: prefix <> "-worker",
      control_token_digests: [digest],
      allowed_attachment_roots: [Path.join(root, source_path)]
    )

    assert {:ok, _} = DatabaseCatalog.create(source_path, %{database_uuid: source_uuid})
    File.mkdir_p!(attachment_location)

    on_exit(fn ->
      Application.put_env(:vial_keeper, :shadow_worker, previous)
      _ = DatabaseCatalog.close(source_uuid)
      _ = DatabaseCatalog.unregister(source_uuid)
      VialKeeper.TempDatabase.cleanup(Path.join(root, source_path))
      VialKeeper.TempDatabase.cleanup(Path.join(root, prefix <> "-worker"))
    end)

    assert {:ok, remote} =
             RemoteEndpoint.new(%{
               "base_url" => server.base_url,
               "auth_token" => control_token,
               "control_timeout_ms" => 5_000,
               "read_timeout_ms" => 5_000
             })

    request = %{
      "source_uuid" => source_uuid,
      "shadow_uuid" => VialKeeper.UUID.v4(),
      "generation" => 1,
      "operation_id" => VialKeeper.UUID.v4(),
      "attachment_store_type" => "external_cas",
      "attachment_location" => attachment_location,
      "specification_digest" => String.duplicate("e", 64)
    }

    assert {:ok, %{"state" => "absent"}} = RemoteEndpoint.inspect(remote, request, 5_000)
    assert {:ok, %{"state" => "bootstrapping"}} = RemoteEndpoint.provision(remote, request, 5_000)
    assert {:ok, %{"state" => "bootstrapping"}} = RemoteEndpoint.inspect(remote, request, 5_000)
    assert {:ok, %{"state" => "absent"}} = RemoteEndpoint.destroy(remote, request, 5_000)
    assert {:ok, %{"state" => "absent"}} = RemoteEndpoint.inspect(remote, request, 5_000)
  end
end
