defmodule VialKeeper.Runtime.WriterPoolTest do
  @moduledoc """
  The writer pool on a disk SQLite database with several writer connections:
  per-document FIFO locks, the barrier for serial commands and its cache
  epoch, queue overflow, deadlines, and close draining running writes.
  """
  use ExUnit.Case, async: false

  alias VialKeeper.Deadline
  alias VialKeeper.Eventual
  alias VialKeeper.Runtime.{DatabaseCatalog, WriterPool}
  alias VialKeeper.View.Manager

  setup context do
    previous_limits = Application.get_env(:vial_keeper, :host_limits)
    previous_writers = Application.get_env(:vial_keeper, :sqlite_max_writers)

    limits =
      (previous_limits || [])
      |> Keyword.put(:writer_pool_size, Map.get(context, :writer_pool_size, 4))
      |> Keyword.put(:write_queue_limit, Map.get(context, :write_queue_limit, 128))

    Application.put_env(:vial_keeper, :host_limits, limits)
    Application.put_env(:vial_keeper, :sqlite_max_writers, Map.get(context, :max_writers, 4))

    relative = "writer-pool-#{System.unique_integer([:positive])}.vialkeeper"
    absolute = Path.join(VialKeeper.Config.database_root(), relative)
    VialKeeper.TempDatabase.cleanup(absolute)

    assert {:ok, identity} = DatabaseCatalog.create(relative)
    uuid = identity.database_uuid
    assert {:ok, _} = DatabaseCatalog.open(uuid)
    assert :ok = Manager.await_resumed(uuid)

    on_exit(fn ->
      Application.delete_env(:vial_keeper, :writer_slot_sync)
      Application.put_env(:vial_keeper, :host_limits, previous_limits)
      restore_env(:sqlite_max_writers, previous_writers)
      _ = DatabaseCatalog.close(uuid)
      _ = DatabaseCatalog.unregister(uuid)
      VialKeeper.TempDatabase.cleanup(absolute)
    end)

    {:ok, uuid: uuid}
  end

  @tag max_writers: 1
  test "a single-writer backend starts no writer pool", %{uuid: uuid} do
    refute WriterPool.enabled?(uuid)
    assert [] == Registry.lookup(VialKeeper.Runtime.DatabaseRegistry, {:writer_pool, uuid})
    assert {:ok, %{sequence: 1}} = put(uuid, "doc", %{"n" => 1})
  end

  test "writes to one document run in arrival order while other documents proceed", %{
    uuid: uuid
  } do
    assert WriterPool.enabled?(uuid)
    gate = hold_writes(uuid)

    first = Task.async(fn -> put(uuid, "x", %{"n" => 1}) end)
    assert_receive {^gate, :before_write, first_slot, ["x"]}, 2_000

    # A put without `if_revision` succeeds only while "x" does not exist yet,
    # so it fails here exactly because it ran after the first write.
    second = Task.async(fn -> put(uuid, "x", %{"n" => 2}) end)
    await_stats(uuid, &match?(%{active: 1, queued: 1}, &1))

    other = Task.async(fn -> put(uuid, "y", %{"n" => 1}) end)
    assert_receive {^gate, :before_write, other_slot, ["y"]}, 2_000
    refute_received {^gate, :before_write, _slot, ["x"]}

    send(other_slot, {:go, gate})
    assert {:ok, %{revision: "1-" <> _}} = Task.await(other)
    refute_received {^gate, :before_write, _slot, ["x"]}

    send(first_slot, {:go, gate})
    assert {:ok, %{revision: "1-" <> _}} = Task.await(first)

    assert_receive {^gate, :before_write, second_slot, ["x"]}, 2_000
    send(second_slot, {:go, gate})
    assert {:error, %VialKeeper.Error{code: :revision_conflict}} = Task.await(second)

    await_stats(uuid, &match?(%{active: 0, queued: 0, held_documents: 0}, &1))
  end

  test "a serial command waits for running writes and bumps the cache epoch", %{uuid: uuid} do
    gate = hold_writes(uuid)
    epoch = WriterPool.cache_epoch(uuid)

    write = Task.async(fn -> put(uuid, "x", %{"n" => 1}) end)
    assert_receive {^gate, :before_write, slot, ["x"]}, 2_000

    serial =
      Task.async(fn ->
        DatabaseCatalog.command(uuid, {:command, :update_config, %{}}, 10_000)
      end)

    await_stats(uuid, &match?(%{quiescing?: true, active: 1}, &1))
    assert Task.yield(serial, 100) == nil

    send(slot, {:go, gate})
    assert {:ok, %{sequence: sequence}} = Task.await(write)
    assert {:ok, %{} = _config} = Task.await(serial)

    assert WriterPool.cache_epoch(uuid) == epoch + 1
    await_stats(uuid, &match?(%{quiescing?: false}, &1))

    Application.delete_env(:vial_keeper, :writer_slot_sync)
    assert {:ok, %{sequence: next}} = put(uuid, "y", %{"n" => 1})
    assert next > sequence
  end

  @tag writer_pool_size: 2, write_queue_limit: 1
  test "a full write queue rejects with the retryable overload error", %{uuid: uuid} do
    gate = hold_writes(uuid)

    running =
      for id <- ["a", "b"] do
        task = Task.async(fn -> put(uuid, id, %{"n" => 1}) end)
        assert_receive {^gate, :before_write, slot, [^id]}, 2_000
        {task, slot}
      end

    queued = Task.async(fn -> put(uuid, "c", %{"n" => 1}) end)
    await_stats(uuid, &match?(%{active: 2, queued: 1}, &1))

    assert {:error, %VialKeeper.Error{code: :database_overloaded, retryable: true}} =
             put(uuid, "d", %{"n" => 1})

    for {task, slot} <- running do
      send(slot, {:go, gate})
      assert {:ok, _} = Task.await(task)
    end

    assert_receive {^gate, :before_write, slot, ["c"]}, 2_000
    send(slot, {:go, gate})
    assert {:ok, _} = Task.await(queued)
  end

  @tag writer_pool_size: 2, write_queue_limit: 1
  test "writes waiting for a busy document do not count against the queue limit",
       %{uuid: uuid} do
    gate = hold_writes(uuid)

    running =
      for id <- ["a", "b"] do
        task = Task.async(fn -> put(uuid, id, %{"n" => 1}) end)
        assert_receive {^gate, :before_write, slot, [^id]}, 2_000
        {task, slot}
      end

    queued = Task.async(fn -> put(uuid, "c", %{"n" => 1}) end)
    await_stats(uuid, &match?(%{active: 2, queued: 1}, &1))

    # The queue is full, yet writes to the held documents are still accepted.
    waiting =
      for id <- ["a", "a", "b", "c"] do
        task = Task.async(fn -> put(uuid, id, %{"n" => 2}) end)
        {id, task}
      end

    await_stats(uuid, &match?(%{active: 2, queued: 5}, &1))

    assert {:error, %VialKeeper.Error{code: :database_overloaded, retryable: true}} =
             put(uuid, "d", %{"n" => 1})

    Application.delete_env(:vial_keeper, :writer_slot_sync)

    for {_task, slot} <- running, do: send(slot, {:go, gate})

    for {task, _slot} <- running, do: assert({:ok, _} = Task.await(task))
    assert {:ok, _} = Task.await(queued)

    # Each ran after the document was free; without a base revision they
    # conflict with the earlier write instead of being rejected for load.
    for {_id, task} <- waiting do
      assert {:error, %VialKeeper.Error{code: :revision_conflict}} = Task.await(task)
    end
  end

  test "a queued write past its deadline is withdrawn from the queue", %{uuid: uuid} do
    gate = hold_writes(uuid)

    running = Task.async(fn -> put(uuid, "x", %{"n" => 1}) end)
    assert_receive {^gate, :before_write, slot, ["x"]}, 2_000

    deadline = Deadline.from_timeout(150)

    assert {:error, %VialKeeper.Error{retryable: true, details: %{reason: :deadline_exhausted}}} =
             DatabaseCatalog.command_with_deadline(
               uuid,
               {:command, :put, %{document_id: "x", body: %{"n" => 2}}},
               deadline
             )

    await_stats(uuid, &match?(%{active: 1, queued: 0}, &1))

    send(slot, {:go, gate})
    assert {:ok, %{revision: "1-" <> _}} = Task.await(running)
    refute_receive {^gate, :before_write, _slot, ["x"]}, 200
  end

  test "close drains running writes before the runtime stops", %{uuid: uuid} do
    gate = hold_writes(uuid)

    write = Task.async(fn -> put(uuid, "x", %{"n" => 1}) end)
    assert_receive {^gate, :before_write, slot, ["x"]}, 2_000

    closer = Task.async(fn -> DatabaseCatalog.close(uuid) end)
    await_stats(uuid, &match?(%{closing?: true, active: 1}, &1))
    assert Task.yield(closer, 100) == nil

    send(slot, {:go, gate})
    assert {:ok, %{sequence: 1}} = Task.await(write)
    assert :ok = Task.await(closer, 30_000)
    refute WriterPool.enabled?(uuid)

    Application.delete_env(:vial_keeper, :writer_slot_sync)
    assert {:ok, _} = DatabaseCatalog.open(uuid)
    assert {:ok, %{body: %{"n" => 1}}} = VialKeeper.Documents.get(uuid, %{id: "x"})
  end

  defp put(uuid, id, body),
    do: DatabaseCatalog.command(uuid, {:command, :put, %{document_id: id, body: body}}, 10_000)

  defp hold_writes(uuid) do
    gate = make_ref()
    Application.put_env(:vial_keeper, :writer_slot_sync, {self(), gate, uuid})
    gate
  end

  defp await_stats(uuid, matcher) do
    Eventual.eventually(
      fn ->
        case WriterPool.stats(uuid) do
          {:ok, stats} -> matcher.(stats)
          _ -> false
        end
      end,
      timeout: 2_000,
      message: "writer pool did not reach the expected state"
    )
  end

  defp restore_env(key, nil), do: Application.delete_env(:vial_keeper, key)
  defp restore_env(key, value), do: Application.put_env(:vial_keeper, key, value)
end
