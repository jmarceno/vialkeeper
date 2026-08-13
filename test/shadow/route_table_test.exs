defmodule ElixirDB.Shadow.RouteTableTest do
  use ExUnit.Case, async: true

  alias ElixirDB.Shadow.RouteTable

  test "compare-delete preserves a newer route" do
    name = unique_name(:shadow_routes)
    {:ok, pid} = RouteTable.start_link(name: name)
    on_exit(fn -> if Process.alive?(pid), do: GenServer.stop(pid, :normal) end)

    source_uuid = ElixirDB.UUID.v4()
    old = %{generation: 1, shadow_uuid: ElixirDB.UUID.v4()}
    new = %{generation: 2, shadow_uuid: ElixirDB.UUID.v4()}
    assert :ok = RouteTable.put(source_uuid, new, name)
    assert :stale = RouteTable.compare_delete(source_uuid, old, name)
    assert {:ok, ^new} = RouteTable.get(source_uuid, name)
    assert :ok = RouteTable.compare_delete(source_uuid, new, name)
    assert :not_found = RouteTable.get(source_uuid, name)
  end

  defp unique_name(prefix), do: String.to_atom("#{prefix}_#{System.unique_integer([:positive])}")
end
