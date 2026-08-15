defmodule VialKeeper.Shadow.RegistryTest do
  use ExUnit.Case, async: true

  alias VialKeeper.Shadow.{Definition, Observation, Registry}

  test "persists desired state, observations, and orphan records" do
    root = temp_root()
    name = unique_name(:shadow_registry)
    {:ok, pid} = Registry.start_link(root: root, name: name)
    on_exit(fn -> stop(pid, root) end)

    source_uuid = VialKeeper.UUID.v4()

    assert {:ok, definition} =
             Definition.new(source_uuid, %{
               location: "worker-a",
               attachment_location: "/mnt/source/blobs"
             })

    assert {:ok, ^definition} = Registry.put_desired(definition, name)

    {:ok, observation} = Observation.new(%{state: :ready, applied_source_sequence: 12})

    assert :ok =
             Registry.apply_observation(
               source_uuid,
               Definition.token(definition),
               observation,
               name
             )

    assert :ok = Registry.record_orphan(source_uuid, %{"generation" => 0}, name)

    assert {:ok,
            %{desired: ^definition, observed: %{state: :ready}, orphans: [%{"generation" => 0}]}} =
             Registry.get(source_uuid, name)

    restart_name = unique_name(:shadow_registry_restart)
    {:ok, restart_pid} = Registry.start_link(root: root, name: restart_name)
    on_exit(fn -> if Process.alive?(restart_pid), do: GenServer.stop(restart_pid, :normal) end)
  end

  test "late observation cannot change a replacement" do
    root = temp_root()
    name = unique_name(:shadow_registry)
    {:ok, pid} = Registry.start_link(root: root, name: name)
    on_exit(fn -> stop(pid, root) end)

    source_uuid = VialKeeper.UUID.v4()

    assert {:ok, first} =
             Definition.new(source_uuid, %{
               location: "worker-a",
               attachment_location: "/mnt/a/blobs"
             })

    assert {:ok, second} =
             Definition.replace(first, %{location: "worker-b", attachment_location: "/mnt/b/blobs"})

    assert {:ok, _} = Registry.put_desired(first, name)
    assert {:ok, _} = Registry.put_desired(second, name)
    {:ok, observation} = Observation.new(%{state: :ready})

    assert :stale =
             Registry.apply_observation(source_uuid, Definition.token(first), observation, name)

    assert :ok =
             Registry.apply_observation(source_uuid, Definition.token(second), observation, name)
  end

  test "orphan records are capped per source" do
    root = temp_root()
    name = unique_name(:shadow_registry)
    {:ok, pid} = Registry.start_link(root: root, name: name)
    on_exit(fn -> stop(pid, root) end)

    source_uuid = VialKeeper.UUID.v4()

    for generation <- 1..8 do
      assert :ok = Registry.record_orphan(source_uuid, %{"generation" => generation}, name)
    end

    assert {:error, %{code: :resource_limit}} =
             Registry.record_orphan(source_uuid, %{"generation" => 9}, name)
  end

  defp temp_root do
    root =
      Path.join(
        System.tmp_dir!(),
        "vialkeeper-shadow-registry-#{System.unique_integer([:positive])}"
      )

    File.mkdir_p!(root)
    root
  end

  defp unique_name(prefix), do: String.to_atom("#{prefix}_#{System.unique_integer([:positive])}")

  defp stop(pid, root) do
    if Process.alive?(pid), do: GenServer.stop(pid, :normal)
    File.rm_rf(root)
  end
end
