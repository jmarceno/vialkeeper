defmodule ElixirDB.Shadow.RouteTableTest do
  use ExUnit.Case, async: true

  alias ElixirDB.Shadow.RouteTable

  test "compare-delete matches generation identity and preserves a newer route" do
    name = unique_name(:shadow_routes)
    {:ok, pid} = RouteTable.start_link(name: name)
    on_exit(fn -> if Process.alive?(pid), do: GenServer.stop(pid, :normal) end)

    source_uuid = ElixirDB.UUID.v4()
    old = snapshot(source_uuid, 1)
    new = snapshot(source_uuid, 2)
    assert :ok = RouteTable.put(source_uuid, new, name)
    assert :stale = RouteTable.compare_delete(source_uuid, old, name)
    assert {:ok, ^new} = RouteTable.get(source_uuid, name)
    assert :ok = RouteTable.compare_delete(source_uuid, Map.put(new, :endpoint, :ignored), name)
    assert :not_found = RouteTable.get(source_uuid, name)
  end

  test "compare-put refuses to install an older generation" do
    name = unique_name(:shadow_routes)
    {:ok, pid} = RouteTable.start_link(name: name)
    on_exit(fn -> if Process.alive?(pid), do: GenServer.stop(pid, :normal) end)

    source_uuid = ElixirDB.UUID.v4()
    current = snapshot(source_uuid, 2)
    older = snapshot(source_uuid, 1)
    assert :ok = RouteTable.put(source_uuid, current, name)
    assert :stale = RouteTable.put(source_uuid, older, name)
    assert {:ok, ^current} = RouteTable.get(source_uuid, name)
  end

  test "get is a direct ETS lookup" do
    name = unique_name(:shadow_routes)
    {:ok, pid} = RouteTable.start_link(name: name)
    on_exit(fn -> if Process.alive?(pid), do: GenServer.stop(pid, :normal) end)

    source_uuid = ElixirDB.UUID.v4()
    snapshot = snapshot(source_uuid, 1)
    assert :ok = RouteTable.put(source_uuid, snapshot, name)
    assert [{^source_uuid, ^snapshot}] = :ets.lookup(name, source_uuid)
    assert {:ok, ^snapshot} = RouteTable.get(source_uuid, name)
  end

  defp snapshot(source_uuid, generation) do
    %{
      source_uuid: source_uuid,
      shadow_uuid: ElixirDB.UUID.v4(),
      generation: generation,
      operation_id: ElixirDB.UUID.v4()
    }
  end

  defp unique_name(prefix), do: String.to_atom("#{prefix}_#{System.unique_integer([:positive])}")
end
