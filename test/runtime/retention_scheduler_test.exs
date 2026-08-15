defmodule VialKeeper.Runtime.RetentionSchedulerTest do
  @moduledoc "Covers scheduled retention compaction through database owners."

  use ExUnit.Case, async: false

  @moduletag :integration

  alias VialKeeper.Eventual
  alias VialKeeper.Runtime.DatabaseCatalog

  @tag :slow
  test "scheduled compaction runs through the owner without contacting peers" do
    relative = "retention-scheduler-#{System.unique_integer([:positive])}.vialkeeper"
    absolute = Path.join(VialKeeper.Config.database_root(), relative)
    VialKeeper.TempDatabase.cleanup(absolute)

    assert {:ok, identity} = DatabaseCatalog.create(relative)
    uuid = identity.database_uuid
    assert {:ok, _} = DatabaseCatalog.open(uuid)

    assert {:ok, _} =
             DatabaseCatalog.command(
               uuid,
               {:command, :update_config,
                %{
                  "retention" => %{
                    "mode" => "stable_frontier",
                    "history_depth" => 0,
                    "peer_expiry_ms" => 86_400_000,
                    "schedule" => 50
                  }
                }}
             )

    for id <- ["a", "b"] do
      assert {:ok, _} =
               DatabaseCatalog.command(
                 uuid,
                 {:command, :put, %{document_id: id, body: %{"n" => 1}}}
               )
    end

    assert Eventual.eventually(
             fn ->
               case DatabaseCatalog.command(uuid, {:command, :retention_status, %{}}) do
                 {:ok, %{floor_sequence: floor}} when floor > 0 -> true
                 _ -> false
               end
             end,
             timeout: 5_000,
             message: "scheduled compaction did not advance the retention floor"
           )

    on_exit(fn ->
      _ = DatabaseCatalog.close(uuid)
      _ = DatabaseCatalog.unregister(uuid)
      VialKeeper.TempDatabase.cleanup(absolute)
    end)
  end

  test "disabled schedule does not arm compaction timer" do
    relative = "retention-scheduler-off-#{System.unique_integer([:positive])}.vialkeeper"
    absolute = Path.join(VialKeeper.Config.database_root(), relative)
    VialKeeper.TempDatabase.cleanup(absolute)

    assert {:ok, identity} = DatabaseCatalog.create(relative)
    uuid = identity.database_uuid
    assert {:ok, _} = DatabaseCatalog.open(uuid)

    assert {:ok, %{floor_sequence: 0}} =
             DatabaseCatalog.command(uuid, {:command, :retention_status, %{}})

    assert [{pid, _}] =
             Registry.lookup(
               VialKeeper.Runtime.DatabaseRegistry,
               {:retention_scheduler, uuid}
             )

    assert %{timer_ref: nil} = :sys.get_state(pid)

    assert {:ok, %{floor_sequence: 0}} =
             DatabaseCatalog.command(uuid, {:command, :retention_status, %{}})

    on_exit(fn ->
      _ = DatabaseCatalog.close(uuid)
      _ = DatabaseCatalog.unregister(uuid)
      VialKeeper.TempDatabase.cleanup(absolute)
    end)
  end
end
