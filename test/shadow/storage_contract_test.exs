defmodule ElixirDB.Shadow.StorageContractTest do
  use ExUnit.Case, async: true

  alias ElixirDB.Storage.AdapterCase
  alias ElixirDB.Storage.Memory.Adapter, as: MemoryAdapter
  alias ElixirDB.Storage.Ports.Access
  alias ElixirDB.Storage.SQLite.Adapter, as: SQLiteAdapter
  alias ElixirDB.TempDatabase
  alias ElixirDB.UUID

  @adapters [MemoryAdapter, SQLiteAdapter]

  test "shadow metadata is durable and source origins are monotonic" do
    for adapter_mod <- @adapters do
      {:ok, bundle_path} = TempDatabase.create(prefix: "elixirdb-shadow-contract")
      path = AdapterCase.adapter_path(adapter_mod, bundle_path)
      source_uuid = UUID.v4()
      shadow_uuid = UUID.v4()

      metadata = %{
        source_database_uuid: source_uuid,
        shadow_database_uuid: shadow_uuid,
        generation: 1,
        operation_id: UUID.v4(),
        attachment_store_type: "external_cas",
        attachment_location: Path.join(bundle_path, "attachments"),
        specification_digest: "spec-digest",
        created_at: DateTime.utc_now() |> DateTime.truncate(:second) |> DateTime.to_iso8601()
      }

      assert {:ok, adapter} =
               adapter_mod.create(path, %{
                 database_uuid: shadow_uuid,
                 database_kind: :shadow,
                 shadow_metadata: metadata
               })

      context = adapter_mod.to_context(adapter)
      shadow_port = Access.port(context, :shadow_state)

      assert {:ok, ^metadata} = shadow_port.metadata(context)
      assert {:ok, 7} = shadow_port.put_origin(context, "doc", 7)
      assert {:ok, 7} = shadow_port.put_origin(context, "doc", 7)
      assert {:ok, 7} = shadow_port.origin(context, "doc")

      assert {:error, %{code: :shadow_generation_conflict}} =
               shadow_port.put_origin(context, "doc", 6)

      assert :ok = adapter_mod.close(adapter)
      assert {:ok, reopened} = adapter_mod.open(path)
      assert {:ok, ^metadata} = shadow_port.metadata(adapter_mod.to_context(reopened))
      assert :ok = adapter_mod.close(reopened)
      TempDatabase.cleanup(bundle_path)
    end
  end
end
