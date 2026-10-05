defmodule VialKeeper.Runtime.SequenceLedgerTest do
  @moduledoc """
  The sequence ledger on a disk SQLite database: reservations, the visible
  watermark under in-order and out-of-order completion, holes, dead
  reservers, block persistence of the reserved-through value, persistence
  failures, visibility waits and change notifications.
  """
  use ExUnit.Case, async: false

  alias VialKeeper.Deadline
  alias VialKeeper.Error
  alias VialKeeper.Runtime.{ChangeNotifier, SequenceLedger}
  alias VialKeeper.Storage.Services.Sequences
  alias VialKeeper.Storage.SQLite.{Adapter, Connection}

  setup do
    {:ok, bundle} = VialKeeper.TempDatabase.create(prefix: "vialkeeper-ledger")
    path = VialKeeper.TempDatabase.sqlite_path(bundle)
    {:ok, adapter} = Adapter.create(path, %{})
    context = Adapter.to_context(adapter)
    uuid = context.identity.database_uuid

    start_supervised!(ChangeNotifier.child_spec(uuid))
    start_supervised!(SequenceLedger.child_spec(uuid))
    assert :ok = SequenceLedger.initialize(uuid, context)

    on_exit(fn ->
      _ = Adapter.close(adapter)
      VialKeeper.TempDatabase.cleanup(bundle)
    end)

    {:ok, uuid: uuid, path: path}
  end

  test "a new database starts at zero with the first block persisted", %{uuid: uuid, path: path} do
    assert {:ok, %{visible: 0, data_version: 1}} = SequenceLedger.view(uuid)
    assert persisted(path) == 4096
  end

  test "completion in order advances the watermark each time", %{uuid: uuid} do
    {:ok, a, 1, 1} = SequenceLedger.reserve(uuid, 1, :infinity)
    {:ok, b, 2, 2} = SequenceLedger.reserve(uuid, 1, :infinity)

    assert :ok = SequenceLedger.complete(uuid, a, :committed)
    assert {:ok, %{visible: 1, data_version: 2}} = SequenceLedger.view(uuid)

    assert :ok = SequenceLedger.complete(uuid, b, :committed)
    assert {:ok, %{visible: 2, data_version: 3}} = SequenceLedger.view(uuid)
  end

  test "a later completion stays hidden until every earlier reservation finishes", %{uuid: uuid} do
    {:ok, a, 1, 1} = SequenceLedger.reserve(uuid, 1, :infinity)
    {:ok, b, 2, 2} = SequenceLedger.reserve(uuid, 1, :infinity)
    {:ok, c, 3, 3} = SequenceLedger.reserve(uuid, 1, :infinity)

    assert :ok = SequenceLedger.complete(uuid, c, :committed)
    assert SequenceLedger.visible(uuid) == 0
    assert SequenceLedger.data_version(uuid) == 2

    assert :ok = SequenceLedger.complete(uuid, b, :committed)
    assert SequenceLedger.visible(uuid) == 0

    assert :ok = SequenceLedger.complete(uuid, a, :committed)
    assert {:ok, %{visible: 3, data_version: 4}} = SequenceLedger.view(uuid)
  end

  test "an aborted reservation leaves a hole the watermark passes", %{uuid: uuid} do
    {:ok, a, 1, 1} = SequenceLedger.reserve(uuid, 1, :infinity)
    {:ok, b, 2, 2} = SequenceLedger.reserve(uuid, 1, :infinity)

    assert :ok = SequenceLedger.complete(uuid, a, :aborted)
    assert {:ok, %{visible: 1, data_version: 1}} = SequenceLedger.view(uuid)

    assert :ok = SequenceLedger.complete(uuid, b, :committed)
    assert {:ok, %{visible: 2, data_version: 2}} = SequenceLedger.view(uuid)

    # Holes are permanent: the next reservation never reuses one.
    assert {:ok, _c, 3, 3} = SequenceLedger.reserve(uuid, 1, :infinity)
  end

  test "bulk reservations take contiguous ranges", %{uuid: uuid} do
    {:ok, a, 1, 5} = SequenceLedger.reserve(uuid, 5, :infinity)
    {:ok, b, 6, 8} = SequenceLedger.reserve(uuid, 3, :infinity)

    assert :ok = SequenceLedger.complete(uuid, a, :committed)
    assert SequenceLedger.visible(uuid) == 5

    assert :ok = SequenceLedger.complete(uuid, b, :committed)
    assert {:ok, %{visible: 8, data_version: 3}} = SequenceLedger.view(uuid)
  end

  test "the reservation of a dead process is aborted", %{uuid: uuid} do
    test_pid = self()

    holder =
      spawn(fn ->
        send(test_pid, {:reserved, SequenceLedger.reserve(uuid, 2, :infinity)})
        Process.sleep(:infinity)
      end)

    assert_receive {:reserved, {:ok, _token, 1, 2}}
    {:ok, b, 3, 3} = SequenceLedger.reserve(uuid, 1, :infinity)
    assert :ok = SequenceLedger.complete(uuid, b, :committed)
    assert SequenceLedger.visible(uuid) == 0

    Process.exit(holder, :kill)

    VialKeeper.Eventual.eventually(fn -> SequenceLedger.visible(uuid) == 3 end,
      timeout: 2_000,
      message: "dead reservation was not aborted"
    )

    assert SequenceLedger.data_version(uuid) == 2
  end

  test "crossing the persisted value persists the next block before handing out", %{
    uuid: uuid,
    path: path
  } do
    {:ok, a, 1, 4096} = SequenceLedger.reserve(uuid, 4096, :infinity)
    assert persisted(path) == 4096

    {:ok, b, 4097, 4098} = SequenceLedger.reserve(uuid, 2, :infinity)
    assert persisted(path) == 4098 + 4096

    assert :ok = SequenceLedger.complete(uuid, a, :aborted)
    assert :ok = SequenceLedger.complete(uuid, b, :aborted)
  end

  @tag timeout: 30_000
  test "a reservation that cannot be persisted is a retryable error and hands nothing out", %{
    uuid: uuid,
    path: path
  } do
    {:ok, a, 1, 4096} = SequenceLedger.reserve(uuid, 4096, :infinity)

    # Another connection holding the write lock makes the ledger's own
    # persistence transaction give up after its busy timeout.
    {:ok, blocker} = Connection.open(path)
    assert :ok = Connection.exec(blocker, "BEGIN IMMEDIATE")

    try do
      assert {:error, %Error{code: :database_overloaded, retryable: true}} =
               SequenceLedger.reserve(uuid, 1, :infinity)
    after
      assert :ok = Connection.exec(blocker, "ROLLBACK")
      assert :ok = Connection.close(blocker)
    end

    assert persisted(path) == 4096
    assert {:ok, b, 4097, 4097} = SequenceLedger.reserve(uuid, 1, :infinity)
    assert :ok = SequenceLedger.complete(uuid, a, :committed)
    assert :ok = SequenceLedger.complete(uuid, b, :committed)
    assert SequenceLedger.visible(uuid) == 4097
  end

  test "await_visible returns once the sequence is visible", %{uuid: uuid} do
    {:ok, a, 1, 1} = SequenceLedger.reserve(uuid, 1, :infinity)
    {:ok, b, 2, 2} = SequenceLedger.reserve(uuid, 1, :infinity)
    assert :ok = SequenceLedger.complete(uuid, b, :committed)

    waiter =
      Task.async(fn -> SequenceLedger.await_visible(uuid, 2, Deadline.from_timeout(5_000)) end)

    assert Task.yield(waiter, 100) == nil

    assert :ok = SequenceLedger.complete(uuid, a, :committed)
    assert :ok = Task.await(waiter)
    assert :ok = SequenceLedger.await_visible(uuid, 2, Deadline.from_timeout(0))
  end

  test "await_visible returns at its deadline while the sequence is still hidden", %{uuid: uuid} do
    {:ok, a, 1, 1} = SequenceLedger.reserve(uuid, 1, :infinity)
    {:ok, b, 2, 2} = SequenceLedger.reserve(uuid, 1, :infinity)
    assert :ok = SequenceLedger.complete(uuid, b, :committed)

    started = System.monotonic_time(:millisecond)
    assert :ok = SequenceLedger.await_visible(uuid, 2, Deadline.from_timeout(150))
    assert System.monotonic_time(:millisecond) - started >= 150
    assert SequenceLedger.visible(uuid) == 0

    assert :ok = SequenceLedger.complete(uuid, a, :aborted)
  end

  test "change notifications are published exactly when the watermark advances", %{uuid: uuid} do
    assert {:ok, _ref, 0} = ChangeNotifier.subscribe(uuid, 0)

    {:ok, a, 1, 1} = SequenceLedger.reserve(uuid, 1, :infinity)
    {:ok, b, 2, 2} = SequenceLedger.reserve(uuid, 1, :infinity)
    {:ok, c, 3, 3} = SequenceLedger.reserve(uuid, 1, :infinity)

    assert :ok = SequenceLedger.complete(uuid, b, :committed)
    refute_receive {:database_changed, ^uuid, _sequence}, 100

    assert :ok = SequenceLedger.complete(uuid, a, :committed)
    assert_receive {:database_changed, ^uuid, 2}, 1_000

    assert :ok = SequenceLedger.complete(uuid, c, :aborted)
    assert_receive {:database_changed, ^uuid, 3}, 1_000
    refute_receive {:database_changed, ^uuid, _sequence}, 100
  end

  test "with_reservation places numbers in the process and completes by result", %{uuid: uuid} do
    uses = fn count ->
      context = %VialKeeper.Storage.BackendContext{
        backend: Adapter,
        backend_ref: nil,
        bundle_root: "",
        identity: %{database_uuid: uuid}
      }

      Sequences.take(context, count)
    end

    assert {{:ok, [1, 2]}, 2} =
             SequenceLedger.with_reservation(uuid, 2, :infinity, fn -> uses.(2) end)

    assert {:ok, %{visible: 2, data_version: 2}} = SequenceLedger.view(uuid)

    # A committed write that used fewer numbers than reserved returns the rest
    # while its reservation is the newest one.
    assert {{:ok, [3]}, 3} = SequenceLedger.with_reservation(uuid, 3, :infinity, fn -> uses.(1) end)
    assert {:ok, %{visible: 3, data_version: 3}} = SequenceLedger.view(uuid)

    # Failed and raising writes stored nothing; their numbers come back too.
    assert {{:error, :failed}, 0} =
             SequenceLedger.with_reservation(uuid, 1, :infinity, fn -> {:error, :failed} end)

    assert_raise RuntimeError, fn ->
      SequenceLedger.with_reservation(uuid, 1, :infinity, fn -> raise "boom" end)
    end

    assert {:ok, %{visible: 3, data_version: 3}} = SequenceLedger.view(uuid)

    assert {{:ok, :nothing}, 0} =
             SequenceLedger.with_reservation(uuid, 0, :infinity, fn -> {:ok, :nothing} end)

    assert {:ok, _token, 4, 4} = SequenceLedger.reserve(uuid, 1, :infinity)
  end

  test "only the newest reservation returns unused numbers", %{uuid: uuid} do
    {:ok, a, 1, 3} = SequenceLedger.reserve(uuid, 3, :infinity)
    {:ok, b, 4, 4} = SequenceLedger.reserve(uuid, 1, :infinity)

    # Not the newest: its unused numbers 2..3 stay holes.
    assert :ok = SequenceLedger.complete(uuid, a, :committed, 1)
    assert SequenceLedger.visible(uuid) == 3

    # The newest returns everything above what it used.
    assert :ok = SequenceLedger.complete(uuid, b, :aborted, 0)
    assert SequenceLedger.visible(uuid) == 3
    assert {:ok, _c, 4, 4} = SequenceLedger.reserve(uuid, 1, :infinity)
  end

  defp persisted(path) do
    {:ok, conn} = Connection.open(path, mode: [:readonly])

    try do
      {:ok, [[value]]} =
        Connection.query(conn, "SELECT sequence_reserved_through FROM db_meta WHERE id = 1")

      value
    after
      Connection.close(conn)
    end
  end
end
