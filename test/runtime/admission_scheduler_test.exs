defmodule ElixirDB.Runtime.AdmissionSchedulerTest do
  use ExUnit.Case, async: false

  alias ElixirDB.Eventual

  alias ElixirDB.Runtime.{
    AdmissionModel,
    AdmissionPolicy,
    AdmissionSupervisor,
    DatabaseAdmission,
    DatabaseCatalog,
    DatabaseOwner,
    ServiceClass
  }

  @default_keyword AdmissionPolicy.default_keyword()

  setup do
    uuid = ElixirDB.UUID.v4()
    limit = 8
    policy = policy_for_limit(limit)
    {:ok, supervisor} = AdmissionSupervisor.start_link({uuid, limit, policy})

    on_exit(fn ->
      Application.delete_env(:elixir_db, :admitted_command_sync)
      Application.delete_env(:elixir_db, :executor_drain_sync_timeout)
      Application.delete_env(:elixir_db, :executor_drain_sync_retry_ms)

      if Process.alive?(supervisor) do
        try do
          Supervisor.stop(supervisor)
        catch
          _, _ -> :ok
        end
      end
    end)

    {:ok, uuid: uuid, limit: limit, policy: policy, supervisor: supervisor}
  end

  defp policy_for_limit(limit) do
    keyword =
      Keyword.merge(@default_keyword,
        foreground_reserved_slots: 0,
        subscription_reserved_slots: 0,
        replication_reserved_slots: 0,
        maintenance_reserved_slots: 0
      )

    {:ok, policy} = AdmissionPolicy.from_keyword(keyword, limit)
    policy
  end

  defp hold_permit(uuid, timeout \\ 5_000) do
    parent = self()
    ref = make_ref()

    task =
      Task.async(fn ->
        DatabaseAdmission.with_permit(uuid, :foreground, timeout, fn ->
          send(parent, {:held, ref})
          receive(do: ({:release, ^ref} -> :ok))
          :held
        end)
      end)

    assert_receive {:held, ^ref}, timeout
    {task, ref}
  end

  defp release_holder(task, ref) do
    send(task.pid, {:release, ref})
    Task.await(task, 5_000)
  end

  defp with_sync_gate(fun) do
    ref = make_ref()
    :ok = Application.put_env(:elixir_db, :admitted_command_sync, {self(), ref})

    try do
      fun.(ref)
    after
      Application.delete_env(:elixir_db, :admitted_command_sync)
    end
  end

  defp await_stats(uuid, predicate, opts \\ []) do
    Eventual.eventually(
      fn -> poll_stats(uuid, predicate) end,
      Keyword.merge([timeout: 2_000], opts)
    )
  end

  defp poll_stats(uuid, predicate) do
    case DatabaseAdmission.stats(uuid) do
      {:ok, stats} ->
        if predicate.(stats), do: {:ok, stats}, else: false

      _ ->
        false
    end
  end

  describe "FIFO within class" do
    test "foreground requests grant in enqueue order", %{uuid: uuid} do
      parent = self()
      {holder, ref} = hold_permit(uuid)

      waiters =
        for i <- 1..3 do
          Task.async(fn ->
            DatabaseAdmission.execute_owner(uuid, :foreground, fn ->
              send(parent, {:ran, i})
              i
            end)
          end)
        end

      await_stats(uuid, &(&1.queued_foreground == 3))

      release_holder(holder, ref)

      for i <- 1..3 do
        assert_receive {:ran, ^i}, 2_000
        assert ^i = Task.await(Enum.at(waiters, i - 1), 2_000)
      end
    end
  end

  describe "capacity rejection" do
    test "rejects when admission limit is saturated" do
      small_limit = 1
      small_uuid = ElixirDB.UUID.v4()
      policy = policy_for_limit(small_limit)
      {:ok, sup} = AdmissionSupervisor.start_link({small_uuid, small_limit, policy})

      on_exit(fn ->
        if Process.alive?(sup) do
          try do
            Supervisor.stop(sup)
          catch
            _, _ -> :ok
          end
        end
      end)

      {holder, ref} = hold_permit(small_uuid)

      assert {:error, %ElixirDB.Error{code: :database_overloaded}} =
               DatabaseAdmission.execute_owner(small_uuid, :foreground, fn -> :never end)

      release_holder(holder, ref)
    end
  end

  describe "cancel and timeout" do
    test "call timeout cancels queued waiter while process stays alive", %{uuid: uuid} do
      {holder, ref} = hold_permit(uuid)

      spawn(fn ->
        try do
          DatabaseAdmission.with_permit(uuid, :foreground, 50, fn -> :should_not_run end)
        catch
          :exit, _ -> :ok
        end
      end)

      await_stats(uuid, &(&1.queued_foreground == 0))

      release_holder(holder, ref)
    end

    test "execute timeout retains permit until executor completes" do
      small_limit = 1
      small_uuid = ElixirDB.UUID.v4()
      policy = policy_for_limit(small_limit)
      {:ok, sup} = AdmissionSupervisor.start_link({small_uuid, small_limit, policy})

      on_exit(fn ->
        if Process.alive?(sup) do
          try do
            Supervisor.stop(sup)
          catch
            _, _ -> :ok
          end
        end
      end)

      parent = self()

      caller =
        spawn(fn ->
          result =
            try do
              DatabaseAdmission.execute_owner(
                small_uuid,
                :foreground,
                fn ->
                  send(parent, {:owner_started, self()})
                  receive(do: (:finish -> :done))
                end,
                200
              )
            catch
              :exit, reason -> {:exit, reason}
            end

          send(parent, {:execute_result, result})
        end)

      assert_receive {:owner_started, executor_pid}, 2_000

      assert_receive {:execute_result, result}, 2_000

      assert match?({:error, %ElixirDB.Error{}}, result) or
               match?({:exit, {:timeout, _}}, result)

      ref = Process.monitor(caller)
      assert_receive {:DOWN, ^ref, :process, ^caller, _}, 2_000

      assert {:ok, stats} = DatabaseAdmission.stats(small_uuid)
      assert stats.total_occupancy == 1

      assert {:error, %ElixirDB.Error{code: :database_overloaded}} =
               DatabaseAdmission.execute_owner(small_uuid, :foreground, fn -> :never end)

      send(executor_pid, :finish)

      await_stats(small_uuid, &(&1.total_occupancy == 0))
    end

    test "timeout racing a grant does not double grant", %{uuid: uuid} do
      parent = self()
      {holder, holder_ref} = hold_permit(uuid)

      caller =
        spawn(fn ->
          result =
            try do
              DatabaseAdmission.execute_owner(
                uuid,
                :foreground,
                fn ->
                  send(parent, :second_ran)
                  :second
                end,
                50
              )
            catch
              :exit, reason -> {:exit, reason}
            end

          send(parent, {:race_result, result})
        end)

      release_holder(holder, holder_ref)

      assert_receive {:race_result, _result}, 2_000

      ref = Process.monitor(caller)
      assert_receive {:DOWN, ^ref, :process, ^caller, _}, 2_000

      await_stats(uuid, &(&1.total_occupancy == 0))
    end
  end

  describe "caller monitoring" do
    test "dead queued caller is removed without grant", %{uuid: uuid} do
      {holder, ref} = hold_permit(uuid)

      pid =
        spawn(fn ->
          DatabaseAdmission.with_permit(uuid, :foreground, 5_000, fn -> :never end)
        end)

      mon = Process.monitor(pid)
      assert Process.alive?(pid)
      Process.exit(pid, :kill)
      assert_receive {:DOWN, ^mon, :process, ^pid, :killed}, 2_000

      await_stats(uuid, &(&1.queued_foreground == 0))

      release_holder(holder, ref)
    end

    test "active caller death before executor start abandons pre-start work", %{uuid: uuid} do
      parent = self()

      with_sync_gate(fn gate_ref ->
        caller =
          spawn(fn ->
            DatabaseAdmission.execute_owner(uuid, :foreground, fn ->
              send(parent, :owner_should_not_run)
              :never
            end)
          end)

        assert_receive {^gate_ref, :before_begin, executor_pid}, 2_000
        Process.exit(caller, :kill)
        ref = Process.monitor(caller)
        assert_receive {:DOWN, ^ref, :process, ^caller, _}, 2_000

        send(executor_pid, {:go, gate_ref})

        await_stats(uuid, &(&1.total_occupancy == 0))
      end)
    end

    test "active caller death before executor start cancels even when DOWN is still queued",
         %{uuid: uuid} do
      parent = self()

      with_sync_gate(fn gate_ref ->
        caller =
          spawn(fn ->
            DatabaseAdmission.execute_owner(uuid, :foreground, fn ->
              send(parent, :owner_should_not_run)
              :never
            end)
          end)

        assert_receive {^gate_ref, :before_begin, executor_pid}, 2_000
        Process.exit(caller, :kill)
        send(executor_pid, {:go, gate_ref})

        await_stats(uuid, &(&1.total_occupancy == 0))
      end)
    end

    test "active caller death after owner start lets executor finish", %{uuid: uuid} do
      parent = self()

      with_sync_gate(fn gate_ref ->
        caller =
          spawn(fn ->
            DatabaseAdmission.execute_owner(uuid, :foreground, fn ->
              send(parent, {:owner_started, self()})
              receive(do: (:finish -> :done))
            end)
          end)

        assert_receive {^gate_ref, :before_begin, executor_pid}, 2_000
        send(executor_pid, {:go, gate_ref})
        assert_receive {:owner_started, owner_pid}, 2_000

        Process.exit(caller, :kill)
        ref = Process.monitor(caller)
        assert_receive {:DOWN, ^ref, :process, ^caller, _}, 2_000

        assert {:ok, stats} = DatabaseAdmission.stats(uuid)
        assert stats.total_occupancy == 1

        send(owner_pid, :finish)

        await_stats(uuid, &(&1.total_occupancy == 0))
      end)
    end

    test "subscription caller death before executor start is removed", %{uuid: uuid} do
      with_sync_gate(fn gate_ref ->
        caller =
          spawn(fn ->
            DatabaseAdmission.execute_owner(uuid, :subscription, fn -> :never end)
          end)

        assert_receive {^gate_ref, :before_begin, executor_pid}, 2_000
        Process.exit(caller, :kill)
        ref = Process.monitor(caller)
        assert_receive {:DOWN, ^ref, :process, ^caller, _}, 2_000
        send(executor_pid, {:go, gate_ref})

        await_stats(uuid, &(&1.total_occupancy == 0))
      end)
    end

    test "subscription caller death while queued is removed", %{uuid: uuid} do
      {holder, ref} = hold_permit(uuid)

      pid =
        spawn(fn ->
          DatabaseAdmission.execute_owner(uuid, :subscription, fn -> :never end)
        end)

      mon = Process.monitor(pid)
      Process.exit(pid, :kill)
      assert_receive {:DOWN, ^mon, :process, ^pid, _}, 2_000

      await_stats(uuid, &(&1.queued_subscription == 0))
      release_holder(holder, ref)
    end

    test "replication caller death while queued is removed", %{uuid: uuid} do
      {holder, ref} = hold_permit(uuid)

      pid =
        spawn(fn ->
          DatabaseAdmission.execute_owner(uuid, :replication, fn -> :never end)
        end)

      mon = Process.monitor(pid)
      Process.exit(pid, :kill)
      assert_receive {:DOWN, ^mon, :process, ^pid, _}, 2_000

      await_stats(uuid, &(&1.queued_replication == 0))
      release_holder(holder, ref)
    end

    test "maintenance caller death while queued is removed", %{uuid: uuid} do
      {holder, ref} = hold_permit(uuid)

      pid =
        spawn(fn ->
          DatabaseAdmission.execute_owner(uuid, :maintenance, fn -> :never end)
        end)

      mon = Process.monitor(pid)
      Process.exit(pid, :kill)
      assert_receive {:DOWN, ^mon, :process, ^pid, _}, 2_000

      await_stats(uuid, &(&1.queued_maintenance == 0))
      release_holder(holder, ref)
    end
  end

  describe "owner restart" do
    test "owner crash recreates admission with no stale permit or queued work" do
      relative = "admission-owner-restart-#{System.unique_integer([:positive])}.elixirdb"
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

      [{owner_before, _}] =
        Registry.lookup(ElixirDB.Runtime.DatabaseRegistry, {:owner, uuid})

      [{admission_before, _}] =
        Registry.lookup(ElixirDB.Runtime.DatabaseRegistry, {:admission, uuid})

      parent = self()
      gate = make_ref()

      holder =
        spawn(fn ->
          try do
            DatabaseAdmission.execute_owner(uuid, :foreground, fn ->
              send(parent, {:owner_blocked, gate, self()})
              receive(do: (:finish -> :done))
            end)
          catch
            :exit, _ -> :ok
          end
        end)

      assert_receive {:owner_blocked, ^gate, owner_executor}, 2_000
      assert {:ok, 1} = DatabaseAdmission.active_count(uuid)

      ref = Process.monitor(owner_before)
      Process.exit(owner_before, :kill)
      assert_receive {:DOWN, ^ref, :process, ^owner_before, :killed}, 2_000

      Eventual.eventually(
        fn ->
          case Registry.lookup(ElixirDB.Runtime.DatabaseRegistry, {:admission, uuid}) do
            [{pid, _}] when pid != admission_before -> Process.alive?(pid)
            _ -> false
          end
        end,
        timeout: 5_000,
        message: "admission did not restart after owner kill"
      )

      send(owner_executor, :finish)
      ref = Process.monitor(holder)
      assert_receive {:DOWN, ^ref, :process, ^holder, _}, 5_000

      Eventual.eventually(
        fn ->
          case DatabaseAdmission.stats(uuid) do
            {:ok, %{total_occupancy: 0, queued_foreground: 0}} -> true
            _ -> false
          end
        end,
        timeout: 5_000,
        message: "admission did not drain after owner restart"
      )
    end
  end

  describe "executor failures" do
    setup %{uuid: uuid} do
      {:ok, fake_owner} = FakeOwner.start_link(uuid)

      on_exit(fn ->
        if Process.alive?(fake_owner) do
          try do
            GenServer.stop(fake_owner)
          catch
            _, _ -> :ok
          end
        end
      end)

      {:ok, fake_owner: fake_owner}
    end

    test "executor crash before owner call releases permit and grants next", %{uuid: uuid} do
      parent = self()

      with_sync_gate(fn gate_ref ->
        caller =
          spawn(fn ->
            DatabaseAdmission.execute_owner(uuid, :foreground, fn -> :never end)
          end)

        assert_receive {^gate_ref, :before_begin, executor_pid}, 2_000

        successor_ref = make_ref()

        successor =
          spawn(fn ->
            DatabaseAdmission.execute_owner(uuid, :foreground, fn ->
              send(parent, {:successor, successor_ref})
              :successor
            end)
          end)

        await_stats(uuid, &(&1.queued_foreground == 1))

        Application.delete_env(:elixir_db, :admitted_command_sync)

        Process.exit(executor_pid, :kill)
        ref = Process.monitor(executor_pid)
        assert_receive {:DOWN, ^ref, :process, ^executor_pid, _}, 2_000
        send(executor_pid, {:go, gate_ref})

        ref = Process.monitor(caller)
        assert_receive {:DOWN, ^ref, :process, ^caller, _}, 2_000

        assert_receive {:successor, ^successor_ref}, 2_000
        ref = Process.monitor(successor)
        assert_receive {:DOWN, ^ref, :process, ^successor, _}, 2_000

        await_stats(uuid, &(&1.total_occupancy == 0))
      end)
    end

    test "executor crash during owner call blocks successor until owner sync", %{uuid: uuid} do
      parent = self()

      with_sync_gate(fn gate_ref ->
        caller =
          spawn(fn ->
            DatabaseAdmission.execute_owner(uuid, :foreground, fn ->
              DatabaseOwner.command(uuid, {:block, parent})
            end)
          end)

        assert_receive {^gate_ref, :before_begin, executor_pid}, 2_000

        successor_ref = make_ref()

        successor =
          spawn(fn ->
            DatabaseAdmission.execute_owner(uuid, :foreground, fn ->
              send(parent, {:successor, successor_ref})
              :successor
            end)
          end)

        await_stats(uuid, &(&1.queued_foreground == 1))

        send(executor_pid, {:go, gate_ref})
        assert_receive :owner_blocked, 2_000

        assert {:ok, stats} = DatabaseAdmission.stats(uuid)
        assert stats.queued_foreground == 1

        Process.exit(executor_pid, :kill)
        ref = Process.monitor(executor_pid)
        assert_receive {:DOWN, ^ref, :process, ^executor_pid, _}, 2_000

        assert {:ok, stats} = DatabaseAdmission.stats(uuid)
        assert stats.queued_foreground == 1

        refute_receive {:successor, ^successor_ref}, 0

        Application.delete_env(:elixir_db, :admitted_command_sync)
        GenServer.cast(FakeOwner.via(uuid), :release_block)

        assert_receive {:successor, ^successor_ref}, 2_000

        ref = Process.monitor(successor)
        assert_receive {:DOWN, ^ref, :process, ^successor, _}, 2_000
        ref = Process.monitor(caller)
        assert_receive {:DOWN, ^ref, :process, ^caller, _}, 2_000

        await_stats(uuid, &(&1.total_occupancy == 0))
      end)
    end

    test "drain sync timeout retries without granting successor until owner completes", %{
      uuid: uuid
    } do
      :ok = Application.put_env(:elixir_db, :executor_drain_sync_timeout, 1)
      :ok = Application.put_env(:elixir_db, :executor_drain_sync_retry_ms, 5)
      parent = self()

      with_sync_gate(fn gate_ref ->
        caller =
          spawn(fn ->
            DatabaseAdmission.execute_owner(uuid, :foreground, fn ->
              DatabaseOwner.command(uuid, {:block, parent})
            end)
          end)

        assert_receive {^gate_ref, :before_begin, executor_pid}, 2_000

        successor_ref = make_ref()

        successor =
          spawn(fn ->
            DatabaseAdmission.execute_owner(uuid, :foreground, fn ->
              send(parent, {:successor, successor_ref})
              :successor
            end)
          end)

        await_stats(uuid, &(&1.queued_foreground == 1))

        send(executor_pid, {:go, gate_ref})
        assert_receive :owner_blocked, 2_000

        Process.exit(executor_pid, :kill)
        ref = Process.monitor(executor_pid)
        assert_receive {:DOWN, ^ref, :process, ^executor_pid, _}, 2_000

        assert {:ok, stats} = DatabaseAdmission.stats(uuid)
        assert stats.queued_foreground == 1

        refute_receive {:successor, ^successor_ref}, 50

        Application.delete_env(:elixir_db, :admitted_command_sync)
        Application.delete_env(:elixir_db, :executor_drain_sync_timeout)
        Application.delete_env(:elixir_db, :executor_drain_sync_retry_ms)
        GenServer.cast(FakeOwner.via(uuid), :release_block)

        assert_receive {:successor, ^successor_ref}, 2_000

        ref = Process.monitor(successor)
        assert_receive {:DOWN, ^ref, :process, ^successor, _}, 2_000
        ref = Process.monitor(caller)
        assert_receive {:DOWN, ^ref, :process, ^caller, _}, 2_000

        await_stats(uuid, &(&1.total_occupancy == 0))
      end)
    end

    test "admitted_command_done makes executor_drain a no-op", %{uuid: uuid} do
      parent = self()

      assert :done =
               DatabaseAdmission.execute_owner(uuid, :foreground, fn ->
                 send(parent, :owner_ran)
                 :done
               end)

      assert_receive :owner_ran, 2_000

      [{admission_pid, _}] =
        Registry.lookup(ElixirDB.Runtime.DatabaseRegistry, {:admission, uuid})

      send(admission_pid, {:executor_drain, make_ref()})

      successor_ref = make_ref()

      successor =
        spawn(fn ->
          DatabaseAdmission.execute_owner(uuid, :foreground, fn ->
            send(parent, {:successor, successor_ref})
            :successor
          end)
        end)

      assert_receive {:successor, ^successor_ref}, 2_000
      ref = Process.monitor(successor)
      assert_receive {:DOWN, ^ref, :process, ^successor, _}, 2_000

      await_stats(uuid, &(&1.total_occupancy == 0))
    end

    test "executor crash after owner call releases permit", %{uuid: uuid} do
      assert {:error, %ElixirDB.Error{}} =
               DatabaseAdmission.execute_owner(uuid, :foreground, fn ->
                 raise "boom"
               end)

      await_stats(uuid, &(&1.total_occupancy == 0))
    end
  end

  describe "supervision restart" do
    setup %{uuid: uuid} do
      {:ok, fake_owner} = FakeOwner.start_link(uuid)

      on_exit(fn ->
        if Process.alive?(fake_owner) do
          try do
            GenServer.stop(fake_owner)
          catch
            _, _ -> :ok
          end
        end
      end)

      {:ok, fake_owner: fake_owner}
    end

    test "admission crash during owner call cannot double-grant before owner sync", %{uuid: uuid} do
      parent = self()

      with_sync_gate(fn gate_ref ->
        caller =
          spawn(fn ->
            DatabaseAdmission.execute_owner(uuid, :foreground, fn ->
              DatabaseOwner.command(uuid, {:block, parent})
            end)
          end)

        assert_receive {^gate_ref, :before_begin, executor_pid}, 2_000
        send(executor_pid, {:go, gate_ref})
        assert_receive :owner_blocked, 2_000

        [{admission_pid, _}] =
          Registry.lookup(ElixirDB.Runtime.DatabaseRegistry, {:admission, uuid})

        Process.exit(admission_pid, :kill)
        ref = Process.monitor(admission_pid)
        assert_receive {:DOWN, ^ref, :process, ^admission_pid, _}, 2_000

        assert {:error, %ElixirDB.Error{code: :database_closed}} = DatabaseAdmission.stats(uuid)

        refute GenServer.call(FakeOwner.via(uuid), :owner_idle?, 1_000)

        Application.delete_env(:elixir_db, :admitted_command_sync)
        GenServer.cast(FakeOwner.via(uuid), :release_block)

        Eventual.eventually(
          fn ->
            case DatabaseAdmission.stats(uuid) do
              {:ok, %{total_occupancy: 0}} -> true
              _ -> false
            end
          end,
          timeout: 5_000,
          message: "admission did not restart after owner sync"
        )

        successor_ref = make_ref()

        successor =
          spawn(fn ->
            DatabaseAdmission.execute_owner(uuid, :foreground, fn ->
              send(parent, {:successor, successor_ref})
              :successor
            end)
          end)

        assert_receive {:successor, ^successor_ref}, 2_000

        ref = Process.monitor(successor)
        assert_receive {:DOWN, ^ref, :process, ^successor, _}, 2_000
        ref = Process.monitor(caller)
        assert_receive {:DOWN, ^ref, :process, ^caller, _}, 2_000

        await_stats(uuid, &(&1.total_occupancy == 0))
      end)
    end

    test "admission supervisor restart clears permits", %{uuid: uuid, supervisor: supervisor} do
      {holder, holder_ref} = hold_permit(uuid)
      assert {:ok, 1} = DatabaseAdmission.active_count(uuid)

      [{admission_pid, _}] =
        Registry.lookup(ElixirDB.Runtime.DatabaseRegistry, {:admission, uuid})

      Process.exit(admission_pid, :kill)
      ref = Process.monitor(admission_pid)
      assert_receive {:DOWN, ^ref, :process, ^admission_pid, _}, 2_000

      Eventual.eventually(
        fn ->
          case Registry.lookup(ElixirDB.Runtime.DatabaseRegistry, {:admission, uuid}) do
            [{pid, _}] when pid != admission_pid -> Process.alive?(pid)
            _ -> false
          end
        end,
        timeout: 5_000,
        message: "admission did not restart"
      )

      release_holder(holder, holder_ref)

      await_stats(uuid, &(&1.total_occupancy == 0))

      assert Process.alive?(supervisor)
    end
  end

  describe "release idempotency" do
    test "repeated release for the same permit is harmless", %{uuid: uuid} do
      deadline = System.monotonic_time(:millisecond) + 5_000
      request_ref = make_ref()

      assert {:ok, token} =
               GenServer.call(
                 DatabaseAdmission.via(uuid),
                 {:acquire, request_ref, :foreground, deadline, :permit},
                 5_000
               )

      assert :ok = DatabaseAdmission.release(uuid, request_ref, token)
      assert :ok = DatabaseAdmission.release(uuid, request_ref, token)

      assert {:ok, stats} = DatabaseAdmission.stats(uuid)
      assert stats.total_occupancy == 0
    end

    test "mismatched token does not release another permit", %{uuid: uuid} do
      deadline = System.monotonic_time(:millisecond) + 5_000
      request_ref = make_ref()
      other_token = make_ref()

      assert {:ok, token} =
               GenServer.call(
                 DatabaseAdmission.via(uuid),
                 {:acquire, request_ref, :foreground, deadline, :permit},
                 5_000
               )

      assert :ok = DatabaseAdmission.release(uuid, request_ref, other_token)
      assert {:ok, stats} = DatabaseAdmission.stats(uuid)
      assert stats.total_occupancy == 1

      assert :ok = DatabaseAdmission.release(uuid, request_ref, token)
      assert {:ok, stats} = DatabaseAdmission.stats(uuid)
      assert stats.total_occupancy == 0
    end
  end

  describe "no double grant" do
    test "only one active permit at a time", %{uuid: uuid} do
      parent = self()

      first =
        Task.async(fn ->
          DatabaseAdmission.execute_owner(uuid, :foreground, fn ->
            send(parent, {:blocked, self()})
            receive(do: (:go -> :first))
          end)
        end)

      assert_receive {:blocked, first_executor}, 2_000

      second =
        Task.async(fn ->
          DatabaseAdmission.execute_owner(uuid, :foreground, fn ->
            send(parent, :second_active)
            :second
          end)
        end)

      await_stats(uuid, fn stats ->
        stats.total_occupancy == 2 and stats.queued_foreground == 1
      end)

      send(first_executor, :go)
      assert_receive :second_active, 2_000
      assert :second = Task.await(second, 2_000)
      assert :first = Task.await(first, 2_000)
    end
  end

  describe "close drain" do
    test "begin_close rejects new work and drains active executor", %{uuid: uuid} do
      parent = self()

      active =
        Task.async(fn ->
          DatabaseAdmission.execute_owner(uuid, :foreground, fn ->
            send(parent, {:blocked, self()})
            receive(do: (:finish -> :done))
          end)
        end)

      assert_receive {:blocked, executor_pid}, 2_000

      assert :ok = DatabaseAdmission.begin_close(uuid)
      assert {:ok, true} = DatabaseAdmission.closing?(uuid)

      assert {:error, %ElixirDB.Error{code: :database_closed}} =
               DatabaseAdmission.execute_owner(uuid, :foreground, fn -> :never end)

      queued =
        Task.async(fn ->
          DatabaseAdmission.execute_owner(uuid, :foreground, fn -> :queued end)
        end)

      assert {:error, %ElixirDB.Error{code: :database_closed}} = Task.await(queued, 2_000)

      send(executor_pid, :finish)
      assert :done = Task.await(active, 2_000)

      assert :ok = DatabaseAdmission.await_idle(uuid, 2_000)
      assert {:ok, stats} = DatabaseAdmission.stats(uuid)
      assert stats.total_occupancy == 0
    end

    test "await_idle unblocks after permit release during close", %{uuid: uuid} do
      {holder, ref} = hold_permit(uuid)

      assert :ok = DatabaseAdmission.begin_close(uuid)

      idle_waiter =
        Task.async(fn ->
          DatabaseAdmission.await_idle(uuid, 5_000)
        end)

      refute match?({:ok, :ok}, Task.yield(idle_waiter, 200))

      release_holder(holder, ref)

      assert :ok = Task.await(idle_waiter, 2_000)
      assert {:ok, stats} = DatabaseAdmission.stats(uuid)
      assert stats.total_occupancy == 0
    end

    test "await_idle unblocks after permit cancel during close", %{uuid: uuid} do
      request_ref = make_ref()
      parent = self()
      hold_ref = make_ref()

      holder =
        Task.async(fn ->
          case GenServer.call(
                 DatabaseAdmission.via(uuid),
                 {:acquire, request_ref, :foreground, :infinity, :permit},
                 5_000
               ) do
            {:ok, _token} ->
              send(parent, {:permit_held, hold_ref})
              receive(do: ({:release, ^hold_ref} -> :ok))
          end
        end)

      assert_receive {:permit_held, ^hold_ref}, 2_000

      assert :ok = DatabaseAdmission.begin_close(uuid)

      idle_waiter =
        Task.async(fn ->
          DatabaseAdmission.await_idle(uuid, 5_000)
        end)

      refute match?({:ok, :ok}, Task.yield(idle_waiter, 200))

      assert :ok = DatabaseAdmission.cancel(uuid, request_ref)

      assert :ok = Task.await(idle_waiter, 2_000)
      assert {:ok, stats} = DatabaseAdmission.stats(uuid)
      assert stats.total_occupancy == 0

      send(holder.pid, {:release, hold_ref})
      Task.await(holder, 2_000)
    end

    test "await_idle unblocks after pre-start abandonment during close", %{uuid: uuid} do
      with_sync_gate(fn gate_ref ->
        caller =
          spawn(fn ->
            DatabaseAdmission.execute_owner(uuid, :foreground, fn -> :never end)
          end)

        assert_receive {^gate_ref, :before_begin, executor_pid}, 2_000

        assert :ok = DatabaseAdmission.begin_close(uuid)

        idle_waiter =
          Task.async(fn ->
            DatabaseAdmission.await_idle(uuid, 5_000)
          end)

        refute match?({:ok, :ok}, Task.yield(idle_waiter, 200))

        Process.exit(caller, :kill)
        down_ref = Process.monitor(caller)
        assert_receive {:DOWN, ^down_ref, :process, ^caller, _}, 2_000

        send(executor_pid, {:go, gate_ref})

        assert :ok = Task.await(idle_waiter, 2_000)
        assert {:ok, stats} = DatabaseAdmission.stats(uuid)
        assert stats.total_occupancy == 0
      end)
    end
  end

  describe "occupancy bounds" do
    test "concurrent enqueue occupancy never exceeds limit" do
      small_limit = 3
      small_uuid = ElixirDB.UUID.v4()
      policy = policy_for_limit(small_limit)
      {:ok, sup} = AdmissionSupervisor.start_link({small_uuid, small_limit, policy})

      on_exit(fn ->
        if Process.alive?(sup) do
          try do
            Supervisor.stop(sup)
          catch
            _, _ -> :ok
          end
        end
      end)

      parent = self()
      deadline = System.monotonic_time(:millisecond) + 30_000

      holder =
        spawn(fn ->
          DatabaseAdmission.execute_owner(small_uuid, :foreground, fn ->
            send(parent, {:holder_ready, self()})
            receive(do: (:release -> :done))
          end)
        end)

      assert_receive {:holder_ready, holder_executor}, 2_000

      queued =
        for _ <- 1..(small_limit - 1) do
          request_ref = make_ref()

          spawn(fn ->
            GenServer.call(
              DatabaseAdmission.via(small_uuid),
              {:acquire, request_ref, :foreground, deadline, :permit},
              5_000
            )
          end)
        end

      await_stats(small_uuid, &(&1.total_occupancy == small_limit))

      assert {:error, %ElixirDB.Error{code: :database_overloaded}} =
               GenServer.call(
                 DatabaseAdmission.via(small_uuid),
                 {:acquire, make_ref(), :foreground, deadline, :permit},
                 5_000
               )

      send(holder_executor, :release)
      ref = Process.monitor(holder)
      assert_receive {:DOWN, ^ref, :process, ^holder, _}, 2_000

      Enum.each(queued, fn pid ->
        ref = Process.monitor(pid)
        assert_receive {:DOWN, ^ref, :process, ^pid, _}, 2_000
      end)

      await_stats(small_uuid, &(&1.total_occupancy == 0))
    end
  end

  describe "deadline budget" do
    test "execute_owner fails immediately when timeout budget is exhausted", %{uuid: uuid} do
      assert {:error, %ElixirDB.Error{}} =
               DatabaseAdmission.execute_owner(uuid, :foreground, fn -> :should_not_run end, 0)
    end
  end

  describe "trace context" do
    test "executor attaches trace context captured at enqueue", %{uuid: uuid} do
      parent = self()
      expected = OpenTelemetry.Ctx.new()
      token = OpenTelemetry.Ctx.attach(expected)

      try do
        assert :done =
                 DatabaseAdmission.execute_owner(uuid, :foreground, fn ->
                   send(parent, {:trace_active, OpenTelemetry.Ctx.get_current() != :undefined})
                   :done
                 end)

        assert_receive {:trace_active, true}, 2_000
      after
        OpenTelemetry.Ctx.detach(token)
      end
    end
  end

  describe "weighted grant order" do
    test "grant sequence matches AdmissionModel for one request per class", %{uuid: uuid} do
      {:ok, policy} = AdmissionPolicy.from_keyword(@default_keyword, 8)
      model = AdmissionModel.new(8, policy)

      ids =
        Map.new(ServiceClass.classes(), fn class ->
          {class, make_ref()}
        end)

      model =
        Enum.reduce(ServiceClass.classes(), model, fn class, acc ->
          {:ok, acc} = AdmissionModel.enqueue(acc, class, Map.fetch!(ids, class))
          acc
        end)

      expected_classes =
        1..4
        |> Enum.reduce({model, []}, fn _, {current, classes} ->
          granted = AdmissionModel.grant_next(current)
          assert granted.active != nil
          released = AdmissionModel.release(granted)
          {released, [granted.active.class | classes]}
        end)
        |> elem(1)
        |> Enum.reverse()

      parent = self()
      {holder, holder_ref} = hold_permit(uuid)

      tasks =
        Map.new(ServiceClass.classes(), fn class ->
          {class,
           Task.async(fn ->
             DatabaseAdmission.execute_owner(uuid, class, fn ->
               send(parent, {:granted, class})
               class
             end)
           end)}
        end)

      await_stats(uuid, &(&1.total_occupancy == 5))

      release_holder(holder, holder_ref)

      actual_classes =
        for _ <- 1..4 do
          assert_receive {:granted, class}, 5_000
          class
        end

      assert expected_classes == actual_classes

      Enum.each(tasks, fn {_class, task} -> assert is_atom(Task.await(task, 5_000)) end)
    end
  end
end

defmodule FakeOwner do
  @moduledoc false
  use GenServer

  alias ElixirDB.Runtime.DatabaseRegistry

  def start_link(uuid) do
    GenServer.start_link(__MODULE__, uuid, name: via(uuid))
  end

  def via(uuid), do: {:via, Registry, {DatabaseRegistry, {:owner, uuid}}}

  @impl true
  def init(uuid), do: {:ok, %{uuid: uuid, block_from: nil, sync_waiters: []}}

  @impl true
  def handle_call(:sync, _from, %{block_from: nil} = state) do
    {:reply, :ok, state}
  end

  def handle_call(:sync, from, state) do
    {:noreply, %{state | sync_waiters: [from | state.sync_waiters]}}
  end

  def handle_call(:owner_idle?, _from, state) do
    {:reply, state.block_from == nil, state}
  end

  def handle_call({:block, notify_pid}, from, state) do
    send(notify_pid, :owner_blocked)
    {:noreply, %{state | block_from: from}}
  end

  @impl true
  def handle_cast(:release_block, %{block_from: from} = state) when not is_nil(from) do
    GenServer.reply(from, :blocked_done)

    sync_waiters = Enum.reverse(state.sync_waiters)
    Enum.each(sync_waiters, &GenServer.reply(&1, :ok))

    {:noreply, %{state | block_from: nil, sync_waiters: []}}
  end

  def handle_cast(:release_block, state), do: {:noreply, state}
end
