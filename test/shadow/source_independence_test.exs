defmodule VialKeeper.Shadow.SourceIndependenceTest do
  use ExUnit.Case, async: false

  alias VialKeeper.Shadow.{Definition, Reconciler, Registry}

  defmodule BlockingEndpoint do
    defstruct [:waiter]

    def capabilities(%__MODULE__{}, _timeout),
      do: {:ok, VialKeeper.Shadow.Protocol.response("00000000-0000-4000-8000-000000000001")}

    def inspect(%__MODULE__{waiter: waiter}, _request, _timeout) do
      send(waiter, {:inspect_blocked, self()})

      receive do
        :continue -> {:ok, %{"state" => "bootstrapping", "worker_node_id" => "blocked"}}
      end
    end

    def provision(%__MODULE__{}, _request, _timeout),
      do: {:ok, %{"state" => "bootstrapping"}}

    def destroy(%__MODULE__{}, _request, _timeout), do: {:ok, %{"state" => "absent"}}
  end

  test "source writes complete while shadow control is blocked on inspect" do
    {source_uuid, path} = VialKeeper.ShadowSource.open!("shadow-indep")
    attachment_location = Path.join(VialKeeper.Config.database_root(), "indep-blobs")
    File.mkdir_p!(attachment_location)

    assert {:ok, definition} =
             Definition.new(source_uuid, %{
               "location" => "local",
               "attachment_location" => attachment_location
             })

    assert {:ok, ^definition} = Registry.put_desired(definition)

    {:ok, pid} =
      Reconciler.start_link(%{
        definition: definition,
        endpoint: %BlockingEndpoint{waiter: self()}
      })

    on_exit(fn ->
      if Process.alive?(pid), do: GenServer.stop(pid, :normal)
      VialKeeper.ShadowSource.close!(source_uuid, path)
    end)

    assert_receive {:inspect_blocked, task_pid}, 1_000

    assert {:ok, %{revision: revision}} =
             VialKeeper.Documents.put(source_uuid, %{id: "live", body: %{"ok" => true}})

    assert {:ok, %{revision: ^revision}} = VialKeeper.Documents.get(source_uuid, %{id: "live"})
    send(task_pid, :continue)
  end
end
