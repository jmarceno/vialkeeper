defmodule ElixirDB.Shadow.RestartRecoveryTest do
  use ExUnit.Case, async: true

  alias ElixirDB.Shadow.{Definition, Observation, Reconciler, Registry, RouteTable}

  test "persisted ready observations are not trusted as live routes after restart" do
    root =
      Path.join(
        System.tmp_dir!(),
        "elixirdb-shadow-restart-#{System.unique_integer([:positive])}"
      )

    File.mkdir_p!(root)
    name = unique_name(:shadow_registry)
    {:ok, pid} = Registry.start_link(root: root, name: name)
    source_uuid = ElixirDB.UUID.v4()

    assert {:ok, definition} =
             Definition.new(source_uuid, %{
               location: "worker-a",
               attachment_location: "/mnt/source/blobs"
             })

    assert {:ok, ^definition} = Registry.put_desired(definition, name)
    {:ok, observation} = Observation.new(%{state: :ready, applied_source_sequence: 9})

    assert :ok =
             Registry.apply_observation(
               source_uuid,
               Definition.token(definition),
               observation,
               name
             )

    GenServer.stop(pid, :normal)

    restart_name = unique_name(:shadow_registry_restart)
    {:ok, restart_pid} = Registry.start_link(root: root, name: restart_name)
    table_name = unique_name(:shadow_routes)
    {:ok, table_pid} = RouteTable.start_link(name: table_name)

    on_exit(fn ->
      if Process.alive?(restart_pid), do: GenServer.stop(restart_pid, :normal)
      if Process.alive?(table_pid), do: GenServer.stop(table_pid, :normal)
      File.rm_rf(root)
    end)

    assert {:ok, %{desired: ^definition, observed: %{state: :ready}}} =
             Registry.get(source_uuid, restart_name)

    assert :not_found = RouteTable.get(source_uuid, table_name)
  end

  test "recover_desired does not start reconcilers while the controller is disabled" do
    assert :ok = Reconciler.recover_desired()
  end

  defp unique_name(prefix), do: String.to_atom("#{prefix}_#{System.unique_integer([:positive])}")
end
