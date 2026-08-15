defmodule VialKeeper.Shadow.LocalLifecycleE2ETest do
  use ExUnit.Case, async: false

  alias VialKeeper.Replication.{LocalEndpoint, Profile}
  alias VialKeeper.Runtime.DatabaseCatalog
  alias VialKeeper.Shadow.{ReadRouter, RouteTable, Worker}
  alias VialKeeper.Storage.Results

  @moduletag :integration

  test "a local worker generation can be provisioned, marked ready, and serve a routed read" do
    prefix = "shadow-local-life-#{System.unique_integer([:positive])}"
    source_path = prefix <> "-source.vialkeeper"
    root = VialKeeper.Config.database_root()
    source_uuid = VialKeeper.UUID.v4()
    shadow_uuid = VialKeeper.UUID.v4()
    operation_id = VialKeeper.UUID.v4()
    worker_root = Path.join(root, prefix <> "-worker")
    attachment_location = Path.join(root, Path.basename(source_path) <> "/blobs")

    assert {:ok, source} = DatabaseCatalog.create(source_path, %{database_uuid: source_uuid})
    File.mkdir_p!(attachment_location)

    request = %{
      "source_uuid" => source_uuid,
      "shadow_uuid" => shadow_uuid,
      "generation" => 1,
      "operation_id" => operation_id,
      "attachment_store_type" => "external_cas",
      "attachment_location" => attachment_location,
      "specification_digest" => String.duplicate("d", 64)
    }

    opts = [root: worker_root, allowed_attachment_roots: [Path.join(root, source_path)]]

    on_exit(fn ->
      _ = Worker.destroy(Map.take(request, ["source_uuid", "generation"]), opts)
      RouteTable.delete(source_uuid)
      _ = DatabaseCatalog.close(source_uuid)
      _ = DatabaseCatalog.unregister(source_uuid)
      VialKeeper.TempDatabase.cleanup(Path.join(root, source_path))
      VialKeeper.TempDatabase.cleanup(worker_root)
    end)

    assert {:ok, %{"state" => "bootstrapping"}} = Worker.provision(request, opts)

    assert {:ok, %{revision: revision}} =
             VialKeeper.Documents.put(source.database_uuid, %{id: "doc", body: %{"n" => 1}})

    profile =
      Profile.shadow(
        source_database_uuid: source_uuid,
        target_database_uuid: shadow_uuid,
        generation: 1,
        operation_id: operation_id
      )

    {:ok, source_endpoint} = LocalEndpoint.new(source_uuid)

    {:ok, shadow_endpoint} =
      LocalEndpoint.new(shadow_uuid,
        shadow: true,
        source_database_uuid: source_uuid,
        generation: 1,
        operation_id: operation_id
      )

    assert {:ok, %{status: :completed, source_sequence: sequence}} =
             VialKeeper.Replication.one_shot_endpoints(
               source_endpoint,
               shadow_endpoint,
               profile: profile,
               direction: "pull",
               shadow_ready: false
             )

    assert :ok =
             Worker.mark_ready(
               Map.take(request, ~w(source_uuid shadow_uuid generation operation_id)),
               sequence,
               opts
             )

    {:ok, local} = VialKeeper.Shadow.LocalEndpoint.new(worker_options: opts)

    assert :ok =
             RouteTable.put(source_uuid, %{
               endpoint: local,
               source_uuid: source_uuid,
               shadow_uuid: shadow_uuid,
               generation: 1,
               operation_id: operation_id
             })

    assert {:ok, %Results.GetDocument{revision: ^revision, body: %{"n" => 1}},
            %{served_by: "shadow", source_watermark: ^sequence}} =
             ReadRouter.get(source_uuid, %{id: "doc"},
               primary: fn _ -> flunk("ready local shadow should serve the read") end
             )
  end
end
