defmodule VialKeeper.View.SupervisorTest do
  @moduledoc "Supervision tests for declarative view runtimes."
  use ExUnit.Case, async: false

  @moduletag :integration

  alias VialKeeper.Eventual
  alias VialKeeper.Runtime.DatabaseCatalog
  alias VialKeeper.View.Manager
  alias VialKeeper.Views

  @view_a %{
    "name" => "view-a",
    "key" => [%{"literal" => "a"}],
    "reducer" => "_count"
  }

  @view_b %{
    "name" => "view-b",
    "key" => [%{"literal" => "b"}],
    "reducer" => "_count"
  }

  setup do
    rel = "view-supervisor-#{System.unique_integer([:positive])}.vialkeeper"
    root = VialKeeper.Config.database_root()
    abs = Path.join(root, rel)
    VialKeeper.TempDatabase.cleanup(abs)

    assert {:ok, identity} = DatabaseCatalog.create(rel)
    uuid = identity.database_uuid
    assert {:ok, _} = DatabaseCatalog.open(uuid)

    on_exit(fn ->
      _ = DatabaseCatalog.close(uuid)
      _ = DatabaseCatalog.unregister(uuid)
      VialKeeper.TempDatabase.cleanup(abs)
    end)

    {:ok, uuid: uuid}
  end

  test "one builder crash does not stop siblings", %{uuid: uuid} do
    assert {:ok, %{"view_id" => view_a}} = Views.create(uuid, @view_a)
    assert {:ok, %{"view_id" => view_b}} = Views.create(uuid, @view_b)

    Eventual.eventually(fn ->
      match?({:ok, _}, Manager.builder_pid(uuid, view_a)) and
        match?({:ok, _}, Manager.builder_pid(uuid, view_b))
    end)

    {:ok, pid_a} = Manager.builder_pid(uuid, view_a)
    Process.exit(pid_a, :kill)

    Eventual.eventually(
      fn ->
        case Manager.builder_pid(uuid, view_a) do
          {:ok, pid} -> pid != pid_a and Process.alive?(pid)
          :error -> false
        end
      end,
      message: "crashed builder was not replaced"
    )

    assert {:ok, sibling_pid} = Manager.builder_pid(uuid, view_b)
    assert Process.alive?(sibling_pid)
  end

  test "view supervisor is registered in runtime child order", %{uuid: uuid} do
    assert [{_pid, _}] =
             Registry.lookup(VialKeeper.Runtime.DatabaseRegistry, {:view_supervisor, uuid})

    assert [{_pid, _}] =
             Registry.lookup(VialKeeper.Runtime.DatabaseRegistry, {:view_manager, uuid})

    assert [{_pid, _}] =
             Registry.lookup(VialKeeper.Runtime.DatabaseRegistry, {:view_builder_supervisor, uuid})
  end
end
