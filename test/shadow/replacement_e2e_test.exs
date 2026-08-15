defmodule VialKeeper.Shadow.ReplacementE2ETest do
  use ExUnit.Case, async: false

  alias VialKeeper.Runtime.DatabaseCatalog
  alias VialKeeper.Shadow.{Definition, Registry, RouteTable}

  test "a newer generation cannot be overwritten by a stale ready snapshot" do
    {source_uuid, path} = VialKeeper.ShadowSource.open!("shadow-replace")
    current = snapshot(source_uuid, 2)
    stale = snapshot(source_uuid, 1)
    assert :ok = RouteTable.put(source_uuid, current)
    assert :stale = RouteTable.put(source_uuid, stale)
    assert {:ok, ^current} = RouteTable.get(source_uuid)
    VialKeeper.ShadowSource.close!(source_uuid, path)
  end

  test "disable persists desired state before the matching route is removed" do
    {source_uuid, path} = VialKeeper.ShadowSource.open!("shadow-replace")

    assert {:ok, definition} =
             Definition.new(source_uuid, %{
               "location" => "local",
               "attachment_location" =>
                 Path.join(VialKeeper.Config.database_root(), "disabled-blobs")
             })

    assert {:ok, ^definition} = Registry.put_desired(definition)
    snapshot = Map.merge(Definition.token(definition), %{endpoint: :probe})
    assert :ok = RouteTable.put(source_uuid, snapshot)
    assert {:ok, disabled} = Registry.disable(source_uuid)
    assert disabled.enabled == false
    assert :ok = RouteTable.compare_delete(source_uuid, Definition.token(definition))
    assert :not_found = RouteTable.get(source_uuid)
    assert {:ok, %{desired: %{enabled: false}}} = Registry.get(source_uuid)
    VialKeeper.ShadowSource.close!(source_uuid, path)
  end

  test "unregister converts desired shadow state to disabled cleanup" do
    {source_uuid, path} = VialKeeper.ShadowSource.open!("shadow-replace")

    assert {:ok, definition} =
             Definition.new(source_uuid, %{
               "location" => "local",
               "attachment_location" => Path.join(VialKeeper.Config.database_root(), "unreg-blobs")
             })

    assert {:ok, ^definition} = Registry.put_desired(definition)

    assert :ok =
             RouteTable.put(
               source_uuid,
               Map.merge(Definition.token(definition), %{endpoint: :probe})
             )

    assert :ok = DatabaseCatalog.close(source_uuid)
    assert :not_found = RouteTable.get(source_uuid)
    assert :ok = DatabaseCatalog.unregister(source_uuid)
    assert {:ok, %{desired: %{enabled: false}}} = Registry.get(source_uuid)
    VialKeeper.TempDatabase.cleanup(Path.join(VialKeeper.Config.database_root(), path))
  end

  defp snapshot(source_uuid, generation) do
    %{
      source_uuid: source_uuid,
      shadow_uuid: VialKeeper.UUID.v4(),
      generation: generation,
      operation_id: VialKeeper.UUID.v4()
    }
  end
end
