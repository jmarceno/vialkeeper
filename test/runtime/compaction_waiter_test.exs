defmodule ElixirDB.Runtime.CompactionWaiterTest do
  @moduledoc """
  ARCH-007: compaction maintenance notifications wake subscribers whose
  `since` is below the new retention floor.
  """
  use ExUnit.Case, async: false

  alias ElixirDB.Runtime.{ChangeNotifier, DatabaseCatalog}

  setup do
    relative = "compaction-waiter-#{System.unique_integer([:positive])}.elixirdb"
    absolute = Path.join(ElixirDB.Config.database_root(), relative)
    ElixirDB.TempDatabase.cleanup(absolute)

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
                    "schedule" => "disabled"
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

    on_exit(fn ->
      _ = DatabaseCatalog.close(uuid)
      _ = DatabaseCatalog.unregister(uuid)
      ElixirDB.TempDatabase.cleanup(absolute)
    end)

    {:ok, uuid: uuid}
  end

  test "maintenance notification wakes subscribers below the new floor", %{uuid: uuid} do
    assert {:ok, _ref, _} = ChangeNotifier.subscribe(uuid, 0)

    assert {:ok, %{new_floor: new_floor}} =
             DatabaseCatalog.command(uuid, {:command, :compact_retention, %{}})

    assert new_floor > 0

    assert_receive {:database_maintenance, ^uuid, event}, 2_000
    assert event.new_floor == new_floor
    assert event.event_kind == :compaction

    assert {:error, %ElixirDB.Error{code: :history_truncated, details: details}} =
             ElixirDB.Changes.read(uuid, %{since: 0, limit: 10})

    assert details.retention_floor == new_floor
  end

  test "Changes.wait re-reads to history_truncated after maintenance notification", %{uuid: uuid} do
    parent = self()
    barrier = make_ref()

    waiter =
      Task.async(fn ->
        send(parent, {:waiting, barrier})

        with {:ok, ref, _} <- ChangeNotifier.subscribe(uuid, 0) do
          receive do
            {:database_maintenance, ^uuid, _event} ->
              ChangeNotifier.unsubscribe(uuid, ref)
              Process.demonitor(ref, [:flush])
              ElixirDB.Changes.read(uuid, %{since: 0, limit: 10, wait_ms: 0})
          after
            5_000 ->
              {:error, ElixirDB.Error.internal_error("maintenance notification timeout")}
          end
        end
      end)

    assert_receive {:waiting, ^barrier}, 1_000
    assert {:ok, _} = DatabaseCatalog.command(uuid, {:command, :compact_retention, %{}})

    assert {:error, %ElixirDB.Error{code: :history_truncated}} = Task.await(waiter, 5_000)
  end
end
