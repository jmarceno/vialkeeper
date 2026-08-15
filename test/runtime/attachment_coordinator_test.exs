defmodule VialKeeper.Runtime.AttachmentCoordinatorTest do
  @moduledoc "Covers attachment coordinator lifecycle and cleanup."

  use ExUnit.Case, async: false

  @moduletag :integration

  alias VialKeeper.Eventual
  alias VialKeeper.Runtime.{AttachmentCoordinator, DatabaseCatalog}

  defmodule BlockingGC do
    @moduledoc "Test GC that remains alive until its owner releases it."

    def gc(uuid) do
      parent = :persistent_term.get({__MODULE__, uuid})
      send(parent, {:blocking_gc_started, self()})

      receive do
        :release -> :ok
      end
    end
  end

  setup do
    relative = "attachment-coordinator-#{System.unique_integer([:positive])}.vialkeeper"
    absolute = Path.join(VialKeeper.Config.database_root(), relative)
    VialKeeper.TempDatabase.cleanup(absolute)

    assert {:ok, identity} = DatabaseCatalog.create(relative)
    assert {:ok, _} = DatabaseCatalog.open(identity.database_uuid)

    on_exit(fn ->
      _ = DatabaseCatalog.close(identity.database_uuid)
      _ = DatabaseCatalog.unregister(identity.database_uuid)
      VialKeeper.TempDatabase.cleanup(absolute)
    end)

    {:ok, uuid: identity.database_uuid}
  end

  defp set_limits(uuid, read_limit, write_limit) do
    assert {:ok, _} =
             DatabaseCatalog.command(
               uuid,
               {:command, :update_config,
                %{
                  "attachments" => %{
                    "max_concurrent_attachment_reads" => read_limit,
                    "max_concurrent_attachment_writes" => write_limit
                  }
                }}
             )
  end

  defp hold_read(uuid, parent, gate) do
    Task.async(fn ->
      assert {:ok, token} = AttachmentCoordinator.acquire_read(uuid, self())
      send(parent, {:held, :read, gate, token})

      receive do
        {:release, ^gate} -> :ok
      end

      assert :ok = AttachmentCoordinator.release(uuid, token)
      :released
    end)
  end

  test "configured read limit is exact", %{uuid: uuid} do
    set_limits(uuid, 2, 8)

    assert {:ok, t1} = AttachmentCoordinator.acquire_read(uuid)
    assert {:ok, t2} = AttachmentCoordinator.acquire_read(uuid)

    assert {:error, %VialKeeper.Error{code: :attachment_overloaded}} =
             AttachmentCoordinator.acquire_read(uuid)

    assert :ok = AttachmentCoordinator.release(uuid, t1)
    assert {:ok, _} = AttachmentCoordinator.acquire_read(uuid)
    assert :ok = AttachmentCoordinator.release(uuid, t2)
  end

  test "configured write limit is exact", %{uuid: uuid} do
    set_limits(uuid, 8, 2)

    assert {:ok, t1, _} = AttachmentCoordinator.acquire_write(uuid)
    assert {:ok, t2, _} = AttachmentCoordinator.acquire_write(uuid)

    assert {:error, %VialKeeper.Error{code: :attachment_overloaded}} =
             AttachmentCoordinator.acquire_write(uuid)

    assert :ok = AttachmentCoordinator.release(uuid, t1)
    assert {:ok, _, _} = AttachmentCoordinator.acquire_write(uuid)
    assert :ok = AttachmentCoordinator.release(uuid, t2)
  end

  test "guard tokens can only be released by their owning process", %{uuid: uuid} do
    assert {:ok, token} = AttachmentCoordinator.acquire_read(uuid)

    assert {:error, %VialKeeper.Error{code: :invalid_request}} =
             Task.async(fn -> AttachmentCoordinator.release(uuid, token) end)
             |> Task.await()

    assert :ok = AttachmentCoordinator.release(uuid, token)
  end

  test "reads and writes use independent counters", %{uuid: uuid} do
    set_limits(uuid, 1, 1)

    assert {:ok, read_token} = AttachmentCoordinator.acquire_read(uuid)
    assert {:ok, write_token, _} = AttachmentCoordinator.acquire_write(uuid)

    assert {:error, %VialKeeper.Error{code: :attachment_overloaded}} =
             AttachmentCoordinator.acquire_read(uuid)

    assert {:error, %VialKeeper.Error{code: :attachment_overloaded}} =
             AttachmentCoordinator.acquire_write(uuid)

    assert :ok = AttachmentCoordinator.release(uuid, read_token)
    assert :ok = AttachmentCoordinator.release(uuid, write_token)
  end

  test "reference guards do not consume read or write quota", %{uuid: uuid} do
    set_limits(uuid, 1, 1)

    assert {:ok, read_token} = AttachmentCoordinator.acquire_read(uuid)
    assert {:ok, write_token, _} = AttachmentCoordinator.acquire_write(uuid)

    assert {:error, %VialKeeper.Error{code: :attachment_overloaded}} =
             AttachmentCoordinator.acquire_read(uuid)

    assert {:error, %VialKeeper.Error{code: :attachment_overloaded}} =
             AttachmentCoordinator.acquire_write(uuid)

    assert {:ok, ref_token} = AttachmentCoordinator.acquire_reference(uuid)
    assert :ok = AttachmentCoordinator.release(uuid, ref_token)
    assert :ok = AttachmentCoordinator.release(uuid, read_token)
    assert :ok = AttachmentCoordinator.release(uuid, write_token)
  end

  test "dead caller releases guard through monitor", %{uuid: uuid} do
    set_limits(uuid, 1, 8)
    parent = self()
    gate = make_ref()

    holder =
      spawn(fn ->
        {:ok, _token} = AttachmentCoordinator.acquire_read(uuid, self())
        send(parent, {:held, gate})

        receive do
          :die -> :ok
        end
      end)

    assert_receive {:held, ^gate}, 1_000

    assert {:error, %VialKeeper.Error{code: :attachment_overloaded}} =
             AttachmentCoordinator.acquire_read(uuid)

    send(holder, :die)
    ref = Process.monitor(holder)
    assert_receive {:DOWN, ^ref, :process, ^holder, _}, 1_000

    assert {:ok, token} = AttachmentCoordinator.acquire_read(uuid)
    assert :ok = AttachmentCoordinator.release(uuid, token)
  end

  test "lowering limit does not kill active streams and rejects new ones", %{uuid: uuid} do
    parent = self()
    gate = make_ref()

    holder = hold_read(uuid, parent, gate)
    assert_receive {:held, :read, ^gate, _token}, 1_000

    set_limits(uuid, 1, 8)

    assert {:error, %VialKeeper.Error{code: :attachment_overloaded}} =
             AttachmentCoordinator.acquire_read(uuid)

    send(holder.pid, {:release, gate})
    assert :released = Task.await(holder)

    assert {:ok, new_token} = AttachmentCoordinator.acquire_read(uuid)
    assert :ok = AttachmentCoordinator.release(uuid, new_token)
  end

  test "gc waits for existing guards before granting token", %{uuid: uuid} do
    set_limits(uuid, 4, 4)
    parent = self()
    gate = make_ref()

    holder = hold_read(uuid, parent, gate)
    assert_receive {:held, :read, ^gate, _token}, 1_000

    gc_task =
      Task.async(fn ->
        result = AttachmentCoordinator.begin_gc(uuid)
        send(parent, {:gc_done, result})

        receive do
          {:finish_gc, token} ->
            AttachmentCoordinator.end_gc(uuid, token)
        after
          5_000 ->
            flunk("gc finish was not signaled")
        end
      end)

    assert Eventual.eventually(
             fn ->
               case AttachmentCoordinator.status(uuid) do
                 %{gc_barrier: true} -> true
                 _ -> false
               end
             end,
             timeout: 1_000,
             message: "gc barrier was not armed"
           )

    refute_receive {:gc_done, _}, 200

    send(holder.pid, {:release, gate})
    assert :released = Task.await(holder)

    assert_receive {:gc_done, {:ok, gc_token}}, 1_000
    send(gc_task.pid, {:finish_gc, gc_token})
    assert :ok = Task.await(gc_task)
  end

  test "concurrent close callers all drain successfully", %{uuid: uuid} do
    parent = self()
    gate = make_ref()
    holder = hold_read(uuid, parent, gate)
    assert_receive {:held, :read, ^gate, _token}, 1_000

    first = Task.async(fn -> AttachmentCoordinator.begin_close(uuid) end)
    second = Task.async(fn -> AttachmentCoordinator.begin_close(uuid) end)

    assert Eventual.eventually(
             fn ->
               case Registry.lookup(
                      VialKeeper.Runtime.DatabaseRegistry,
                      {:attachment_coordinator, uuid}
                    ) do
                 [{pid, _}] ->
                   state = :sys.get_state(pid)
                   state.closing and match?([_, _], state.close_waiters)

                 [] ->
                   false
               end
             end,
             timeout: 1_000,
             message: "both close callers did not reach the drain barrier"
           )

    refute Task.yield(first, 0)
    refute Task.yield(second, 0)

    send(holder.pid, {:release, gate})
    assert :released = Task.await(holder)
    assert :ok = Task.await(first)
    assert :ok = Task.await(second)
  end

  test "catalog close waits without blocking attachment finalization", %{uuid: uuid} do
    parent = self()
    gate = make_ref()
    holder = hold_read(uuid, parent, gate)
    assert_receive {:held, :read, ^gate, _token}, 1_000

    closer = Task.async(fn -> DatabaseCatalog.close(uuid) end)

    assert Eventual.eventually(
             fn ->
               [{pid, _}] =
                 Registry.lookup(
                   VialKeeper.Runtime.DatabaseRegistry,
                   {:attachment_coordinator, uuid}
                 )

               state = :sys.get_state(pid)
               state.closing and state.close_waiters != []
             end,
             timeout: 1_000,
             message: "catalog close did not wait at the attachment drain barrier"
           )

    refute Task.yield(closer, 0)

    send(holder.pid, {:release, gate})
    assert :released = Task.await(holder)
    assert :ok = Task.await(closer, 5_000)
  end

  test "dead gc caller releases a pending barrier", %{uuid: uuid} do
    parent = self()
    gate = make_ref()
    holder = hold_read(uuid, parent, gate)
    assert_receive {:held, :read, ^gate, _token}, 1_000

    gc_pid = spawn(fn -> AttachmentCoordinator.begin_gc(uuid) end)
    gc_ref = Process.monitor(gc_pid)

    assert Eventual.eventually(
             fn ->
               case AttachmentCoordinator.status(uuid) do
                 %{gc_barrier: true} -> true
                 _ -> false
               end
             end,
             timeout: 1_000
           )

    Process.exit(gc_pid, :kill)
    assert_receive {:DOWN, ^gc_ref, :process, ^gc_pid, _}, 1_000
    send(holder.pid, {:release, gate})
    assert :released = Task.await(holder)

    assert {:ok, token} = AttachmentCoordinator.acquire_read(uuid)
    assert :ok = AttachmentCoordinator.release(uuid, token)
  end

  test "new guards are rejected while gc barrier is active", %{uuid: uuid} do
    set_limits(uuid, 4, 4)

    assert {:ok, gc_token} = AttachmentCoordinator.begin_gc(uuid)

    assert {:error, %VialKeeper.Error{code: :attachment_overloaded}} =
             AttachmentCoordinator.acquire_read(uuid)

    assert {:error, %VialKeeper.Error{code: :attachment_overloaded}} =
             AttachmentCoordinator.acquire_write(uuid)

    assert {:error, %VialKeeper.Error{code: :attachment_overloaded}} =
             AttachmentCoordinator.acquire_reference(uuid)

    assert :ok = AttachmentCoordinator.end_gc(uuid, gc_token)

    assert {:ok, token} = AttachmentCoordinator.acquire_read(uuid)
    assert :ok = AttachmentCoordinator.release(uuid, token)
  end

  test "write guard captures max_attachment_bytes at admission", %{uuid: uuid} do
    assert {:ok, first_token, first_max} = AttachmentCoordinator.acquire_write(uuid)
    raised = first_max + 1_024

    assert {:ok, _} =
             DatabaseCatalog.command(
               uuid,
               {:command, :update_config,
                %{
                  "attachments" => %{"max_attachment_bytes" => raised},
                  "replication" => %{"max_transfer_bytes_in_flight" => raised}
                }}
             )

    assert {:ok, second_token, second_max} = AttachmentCoordinator.acquire_write(uuid)
    assert second_max == raised

    status = AttachmentCoordinator.status(uuid)
    assert status.max_attachment_bytes == raised

    assert :ok = AttachmentCoordinator.release(uuid, first_token)
    assert :ok = AttachmentCoordinator.release(uuid, second_token)
  end

  test "coordinator restart reloads limits from owner identity", %{uuid: uuid} do
    set_limits(uuid, 3, 3)

    [{pid, _}] =
      Registry.lookup(VialKeeper.Runtime.DatabaseRegistry, {:attachment_coordinator, uuid})

    Process.exit(pid, :kill)
    ref = Process.monitor(pid)
    assert_receive {:DOWN, ^ref, :process, ^pid, _}, 1_000

    assert Eventual.eventually(
             fn ->
               case AttachmentCoordinator.status(uuid) do
                 {:error, _} -> false
                 %{read_limit: 3, write_limit: 3} -> true
                 _ -> false
               end
             end,
             timeout: 5_000,
             message: "attachment coordinator did not restart with persisted limits"
           )
  end

  test "coordinator termination cannot orphan a scheduled gc task", %{uuid: uuid} do
    key = {BlockingGC, uuid}
    :persistent_term.put(key, self())
    on_exit(fn -> :persistent_term.erase(key) end)

    assert :ok = AttachmentCoordinator.schedule_gc(uuid, BlockingGC)
    assert_receive {:blocking_gc_started, gc_pid}, 1_000
    gc_ref = Process.monitor(gc_pid)

    [{coordinator_pid, _}] =
      Registry.lookup(VialKeeper.Runtime.DatabaseRegistry, {:attachment_coordinator, uuid})

    Process.exit(coordinator_pid, :kill)

    assert_receive {:DOWN, ^gc_ref, :process, ^gc_pid, _reason}, 1_000

    assert Eventual.eventually(
             fn ->
               case AttachmentCoordinator.status(uuid) do
                 %{gc_scheduled: false} -> true
                 _ -> false
               end
             end,
             timeout: 5_000,
             message: "attachment coordinator did not restart after forced termination"
           )
  end

  test "closing a coordinator classifies the killed scheduled gc exit", %{uuid: uuid} do
    key = {BlockingGC, uuid}
    :persistent_term.put(key, self())
    on_exit(fn -> :persistent_term.erase(key) end)

    assert :ok = AttachmentCoordinator.schedule_gc(uuid, BlockingGC)
    assert_receive {:blocking_gc_started, gc_pid}, 1_000
    gc_ref = Process.monitor(gc_pid)

    [{coordinator_pid, _}] =
      Registry.lookup(VialKeeper.Runtime.DatabaseRegistry, {:attachment_coordinator, uuid})

    assert :ok = AttachmentCoordinator.begin_close(uuid)
    assert_receive {:DOWN, ^gc_ref, :process, ^gc_pid, :killed}, 1_000

    assert Eventual.eventually(
             fn ->
               Process.alive?(coordinator_pid) and
                 MapSet.size(:sys.get_state(coordinator_pid).gc_task_links) == 0
             end,
             message: "scheduled GC exit was not consumed by the original coordinator"
           )

    assert [{^coordinator_pid, _}] =
             Registry.lookup(
               VialKeeper.Runtime.DatabaseRegistry,
               {:attachment_coordinator, uuid}
             )
  end
end
