defmodule ElixirDB.Shadow.ReconcilerTest do
  use ExUnit.Case, async: false

  alias ElixirDB.Shadow.{Definition, Reconciler, Registry, RouteTable}

  defmodule ProbeEndpoint do
    defstruct [:pid]

    def capabilities(%__MODULE__{}, _timeout),
      do: {:ok, ElixirDB.Shadow.Protocol.response("00000000-0000-4000-8000-000000000001")}

    def inspect(%__MODULE__{pid: pid}, _request, _timeout) do
      send(pid, :inspected)
      {:ok, %{"state" => "ready", "worker_node_id" => "probe-node", "applied_source_sequence" => 4}}
    end

    def provision(%__MODULE__{pid: pid}, _request, _timeout) do
      send(pid, :provisioned)
      {:error, ElixirDB.Error.internal_error("provision must not run for a ready generation")}
    end

    def destroy(%__MODULE__{}, _request, _timeout), do: {:ok, %{"state" => "absent"}}
  end

  test "inspect-first reconcile leaves a still-valid ready route in place" do
    {source_uuid, path} = ElixirDB.ShadowSource.open!("shadow-reconcile")
    attachment_location = Path.join(ElixirDB.Config.database_root(), "probe-blobs")
    File.mkdir_p!(attachment_location)

    assert {:ok, definition} =
             Definition.new(source_uuid, %{
               "location" => "local",
               "attachment_location" => attachment_location
             })

    assert {:ok, ^definition} = Registry.put_desired(definition)

    snapshot = %{
      endpoint: %ProbeEndpoint{pid: self()},
      source_uuid: source_uuid,
      shadow_uuid: definition.shadow_uuid,
      generation: definition.generation,
      operation_id: definition.operation_id
    }

    assert :ok = RouteTable.put(source_uuid, snapshot)

    {:ok, pid} =
      Reconciler.start_link(%{definition: definition, endpoint: %ProbeEndpoint{pid: self()}})

    on_exit(fn ->
      if Process.alive?(pid), do: GenServer.stop(pid, :normal)
      ElixirDB.ShadowSource.close!(source_uuid, path)
    end)

    assert_receive :inspected, 1_000
    refute_receive :provisioned, 200
    assert {:ok, current} = RouteTable.get(source_uuid)
    assert current.generation == definition.generation
    assert current.shadow_uuid == definition.shadow_uuid
  end
end
