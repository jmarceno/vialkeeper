defmodule ElixirDB.Runtime.WaiterTerminationTest do
  @moduledoc """
  Gap D3 / ARCH-007: closing a database terminates changes waiters with
  retryable `database_closed` (and stream subscribers with `{:database_closed, uuid}`).
  """
  use ExUnit.Case, async: false

  alias ElixirDB.Runtime.{ChangeNotifier, DatabaseCatalog}

  setup do
    relative = "waiter-#{System.unique_integer([:positive])}.db"
    absolute = Path.join(ElixirDB.Config.database_root(), relative)
    ElixirDB.TempDatabase.cleanup(absolute)

    assert {:ok, identity} = DatabaseCatalog.create(relative)
    uuid = identity.database_uuid
    assert {:ok, _} = DatabaseCatalog.open(uuid)

    on_exit(fn ->
      _ = DatabaseCatalog.close(uuid)
      _ = DatabaseCatalog.unregister(uuid)
      ElixirDB.TempDatabase.cleanup(absolute)
    end)

    {:ok, uuid: uuid}
  end

  test "Changes.wait returns database_closed when the database is closed", %{uuid: uuid} do
    parent = self()
    barrier = make_ref()

    waiter =
      Task.async(fn ->
        send(parent, {:waiting, barrier})

        ElixirDB.Changes.wait(uuid, %{
          since: 0,
          limit: 10,
          wait_ms: 30_000
        })
      end)

    assert_receive {:waiting, ^barrier}, 1_000

    ElixirDB.Eventual.eventually(
      fn ->
        case ChangeNotifier.subscriber_count(uuid) do
          n when is_integer(n) and n >= 1 -> {:ok, n}
          _ -> false
        end
      end,
      timeout: 2_000,
      message: "changes waiter did not subscribe"
    )

    assert :ok = DatabaseCatalog.close(uuid)

    assert {:error, %ElixirDB.Error{code: :database_closed, retryable: true}} =
             Task.await(waiter, 5_000)
  end

  test "stream-style ChangeNotifier subscribers receive database_closed on close", %{
    uuid: uuid
  } do
    assert {:ok, ref, _sequence} = ChangeNotifier.subscribe(uuid, 0)
    assert is_reference(ref)
    assert ChangeNotifier.subscriber_count(uuid) == 1

    assert :ok = DatabaseCatalog.close(uuid)

    assert_receive {:database_closed, ^uuid}, 2_000
    refute_receive {:database_changed, ^uuid, _}, 50
  end
end
