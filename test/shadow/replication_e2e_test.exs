defmodule ElixirDB.Shadow.ReplicationE2ETest do
  use ExUnit.Case, async: false

  alias ElixirDB.Replication.{Id, LocalEndpoint, Profile}
  alias ElixirDB.Runtime.{CommandContext, DatabaseCatalog}

  @moduletag :integration

  setup do
    prefix = "shadow-e2e-#{System.unique_integer([:positive])}"
    source_path = prefix <> "-source.elixirdb"
    shadow_path = prefix <> "-shadow.elixirdb"
    root = ElixirDB.Config.database_root()
    source_uuid = ElixirDB.UUID.v4()
    shadow_uuid = ElixirDB.UUID.v4()
    operation_id = ElixirDB.UUID.v4()

    metadata = %{
      source_database_uuid: source_uuid,
      shadow_database_uuid: shadow_uuid,
      generation: 1,
      operation_id: operation_id,
      attachment_store_type: "external_cas",
      attachment_location: Path.join(root, prefix <> "-cas/blobs"),
      specification_digest: String.duplicate("a", 64),
      created_at: DateTime.utc_now() |> DateTime.truncate(:second) |> DateTime.to_iso8601()
    }

    assert {:ok, source} =
             DatabaseCatalog.create(source_path, %{database_uuid: source_uuid})

    assert {:ok, shadow} =
             DatabaseCatalog.create_shadow_internal(shadow_path, %{
               database_uuid: shadow_uuid,
               database_kind: :shadow,
               shadow_metadata: metadata
             })

    on_exit(fn ->
      for {identity, path} <- [{source, source_path}, {shadow, shadow_path}] do
        _ = DatabaseCatalog.close(identity.database_uuid)
        _ = DatabaseCatalog.unregister(identity.database_uuid)
        ElixirDB.TempDatabase.cleanup(Path.join(root, path))
      end
    end)

    {:ok,
     source: source,
     shadow: shadow,
     profile:
       Profile.shadow(
         source_database_uuid: source_uuid,
         target_database_uuid: shadow_uuid,
         generation: 1,
         operation_id: operation_id
       )}
  end

  test "pulls a document into a shadow without creating a source checkpoint", %{
    source: source,
    shadow: shadow,
    profile: profile
  } do
    assert {:ok, %{revision: revision}} =
             ElixirDB.Documents.put(source.database_uuid, %{id: "doc", body: %{"n" => 1}})

    {:ok, source_endpoint} = LocalEndpoint.new(source.database_uuid)
    {:ok, shadow_endpoint} = LocalEndpoint.new(shadow.database_uuid, shadow_opts(profile))

    assert {:ok, %{status: :completed}} =
             ElixirDB.Replication.one_shot_endpoints(
               source_endpoint,
               shadow_endpoint,
               profile: profile,
               direction: "pull",
               shadow_ready: false
             )

    assert {:ok, %{revision: ^revision, body: %{"n" => 1}, sequence: 1}} =
             DatabaseCatalog.command_with_context(
               shadow.database_uuid,
               CommandContext.shadow_read(shadow_context(profile)),
               {:command, :get_document, %{document_id: "doc"}}
             )

    assert {:ok, replication_id} =
             Id.calculate(source.database_uuid, shadow.database_uuid, "pull", "one_shot")

    assert {:ok, nil} =
             DatabaseCatalog.command(
               source.database_uuid,
               {:command, :get_local_record, "checkpoints", replication_id}
             )
  end

  test "resumes from the durable shadow checkpoint after a target restart", %{
    source: source,
    shadow: shadow,
    profile: profile
  } do
    assert {:ok, %{revision: first}} =
             ElixirDB.Documents.put(source.database_uuid, %{id: "doc", body: %{"n" => 1}})

    pull!(source.database_uuid, profile)
    assert :ok = DatabaseCatalog.close(shadow.database_uuid)
    assert {:ok, _} = DatabaseCatalog.open_internal(shadow.database_uuid)

    assert {:ok, %{revision: second}} =
             ElixirDB.Documents.put(source.database_uuid, %{
               id: "doc",
               if_revision: first,
               body: %{"n" => 2}
             })

    pull!(source.database_uuid, profile)

    assert {:ok, %{revision: ^second, body: %{"n" => 2}}} = shadow_document(profile)
  end

  test "reports replacement instead of bootstrapping a ready shadow with a stale epoch", %{
    source: source,
    shadow: shadow,
    profile: profile
  } do
    assert {:ok, replication_id} =
             Id.calculate(source.database_uuid, shadow.database_uuid, "pull", "one_shot")

    assert {:ok, _} =
             DatabaseCatalog.command_with_context(
               shadow.database_uuid,
               CommandContext.shadow_replication(shadow_context(profile)),
               {:command, :put_local_record,
                %{
                  namespace: "shadow_checkpoints",
                  key: replication_id,
                  expected_version: 0,
                  value: %{"source_history_epoch" => "stale", "history" => []}
                }}
             )

    {:ok, source_endpoint} = LocalEndpoint.new(source.database_uuid)
    {:ok, shadow_endpoint} = LocalEndpoint.new(shadow.database_uuid, shadow_opts(profile))

    assert {:error, %{code: :shadow_replacement_required}} =
             ElixirDB.Replication.handshake(
               source_endpoint,
               shadow_endpoint,
               profile: profile,
               direction: "pull",
               shadow_ready: true
             )
  end

  defp pull!(source_uuid, profile) do
    {:ok, source_endpoint} = LocalEndpoint.new(source_uuid)
    {:ok, shadow_endpoint} = LocalEndpoint.new(profile.target_database_uuid, shadow_opts(profile))

    assert {:ok, %{status: :completed}} =
             ElixirDB.Replication.one_shot_endpoints(
               source_endpoint,
               shadow_endpoint,
               profile: profile,
               direction: "pull",
               shadow_ready: false
             )
  end

  defp shadow_document(profile) do
    DatabaseCatalog.command_with_context(
      profile.target_database_uuid,
      CommandContext.shadow_read(shadow_context(profile)),
      {:command, :get_document, %{document_id: "doc"}}
    )
  end

  defp shadow_opts(profile) do
    [
      shadow: true,
      source_database_uuid: profile.source_database_uuid,
      generation: profile.generation,
      operation_id: profile.operation_id
    ]
  end

  defp shadow_context(profile) do
    [
      source_database_uuid: profile.source_database_uuid,
      shadow_database_uuid: profile.target_database_uuid,
      generation: profile.generation,
      operation_id: profile.operation_id
    ]
  end
end
