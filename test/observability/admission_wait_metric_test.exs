defmodule VialKeeper.Observability.AdmissionWaitMetricTest do
  @moduledoc """
  `vial_keeper.database.admission.wait` histogram with bounded attributes.

  Queue depth semantics (queued waiters only, excluding the active permit):

  - `queue_depth_at_enqueue` — total queued waiters after the request is accepted
    into the wait queue, including itself.
  - `queue_depth_at_grant` — remaining queued waiters after this request is
    dequeued (granted, cancelled, or closed). Zero for immediate grants and for
    outcomes that never leave the queue (rejected, closed acquire while closing).
  """

  use VialKeeper.Observability.OtelCase, async: false

  @moduletag :integration

  alias VialKeeper.Eventual
  alias VialKeeper.Observability.TestMetricExporter
  alias VialKeeper.Runtime.{AdmissionPolicy, AdmissionSupervisor, DatabaseAdmission}

  @metric "vial_keeper.database.admission.wait"

  setup do
    uuid = VialKeeper.UUID.v4()
    limit = 1

    {:ok, policy} =
      AdmissionPolicy.from_keyword(
        Keyword.merge(
          AdmissionPolicy.default_keyword(),
          foreground_reserved_slots: 0,
          subscription_reserved_slots: 0,
          replication_reserved_slots: 0,
          maintenance_reserved_slots: 0
        ),
        limit
      )

    {:ok, supervisor} = AdmissionSupervisor.start_link({uuid, limit, policy})

    on_exit(fn ->
      if Process.alive?(supervisor) do
        try do
          Supervisor.stop(supervisor)
        catch
          _, _ -> :ok
        end
      end
    end)

    {:ok, uuid: uuid}
  end

  test "granted and rejected outcomes emit admission.wait with bounded attributes", %{uuid: uuid} do
    parent = self()
    gate = make_ref()

    holder =
      spawn_link(fn ->
        DatabaseAdmission.with_token(uuid, fn ->
          send(parent, {:held, gate})
          receive(do: ({:release, ^gate} -> :ok))
        end)
      end)

    assert_receive {:held, ^gate}, 1_000

    assert {:error, %VialKeeper.Error{code: :database_overloaded}} =
             DatabaseAdmission.with_token(uuid, fn -> :never end)

    send(holder, {:release, gate})
    ref = Process.monitor(holder)
    assert_receive {:DOWN, ^ref, :process, _, _}, 2_000

    assert :done =
             DatabaseAdmission.execute_owner(uuid, :foreground, fn -> :done end)

    Eventual.eventually(
      fn ->
        granted? =
          TestMetricExporter.datapoints_matching(@metric, %{:"db.uuid" => uuid})
          |> Enum.any?(fn dp ->
            TestMetricExporter.datapoint_attr(dp, :"admission.class") == :foreground and
              TestMetricExporter.datapoint_attr(dp, :outcome) == :granted
          end)

        rejected? =
          TestMetricExporter.datapoints_matching(@metric, %{:"db.uuid" => uuid})
          |> Enum.any?(fn dp ->
            TestMetricExporter.datapoint_attr(dp, :"admission.class") == :foreground and
              TestMetricExporter.datapoint_attr(dp, :outcome) == :rejected
          end)

        granted? and rejected?
      end,
      timeout: 2_000,
      message: "expected granted and rejected admission.wait datapoints"
    )

    for dp <- TestMetricExporter.datapoints_matching(@metric, %{:"db.uuid" => uuid}) do
      assert TestMetricExporter.datapoint_attr(dp, :"db.uuid") == uuid

      assert TestMetricExporter.datapoint_attr(dp, :"admission.class") in [
               :foreground,
               :subscription,
               :replication,
               :maintenance
             ]

      assert TestMetricExporter.datapoint_attr(dp, :outcome) in [
               :granted,
               :rejected,
               :cancelled,
               :closed
             ]

      for key <- [:queue_depth_at_enqueue, :queue_depth_at_grant] do
        depth = TestMetricExporter.datapoint_attr(dp, key)
        assert is_integer(depth) and depth >= 0 and depth <= 1
      end
    end
  end

  test "queue_depth_at_enqueue includes self; queue_depth_at_grant is post-dequeue" do
    uuid = VialKeeper.UUID.v4()
    limit = 3
    {:ok, policy} = policy_for_limit(limit)
    {:ok, supervisor} = AdmissionSupervisor.start_link({uuid, limit, policy})

    on_exit(fn ->
      if Process.alive?(supervisor) do
        try do
          Supervisor.stop(supervisor)
        catch
          _, _ -> :ok
        end
      end
    end)

    parent = self()
    gate = make_ref()

    holder =
      spawn_link(fn ->
        DatabaseAdmission.with_token(uuid, fn ->
          send(parent, {:held, gate})
          receive(do: ({:release, ^gate} -> :ok))
        end)
      end)

    assert_receive {:held, ^gate}, 1_000

    first_queued =
      Task.async(fn ->
        DatabaseAdmission.with_token(uuid, fn -> :first end)
      end)

    second_queued =
      Task.async(fn ->
        DatabaseAdmission.with_token(uuid, fn -> :second end)
      end)

    Eventual.eventually(
      fn ->
        case DatabaseAdmission.stats(uuid) do
          {:ok, %{queued_foreground: 2}} -> :ok
          _ -> false
        end
      end,
      timeout: 2_000,
      message: "expected two queued foreground waiters"
    )

    TestMetricExporter.reset()

    send(holder, {:release, gate})
    ref = Process.monitor(holder)
    assert_receive {:DOWN, ^ref, :process, _, _}, 2_000

    assert :first = Task.await(first_queued, 2_000)
    assert :second = Task.await(second_queued, 2_000)

    Eventual.eventually(
      fn ->
        granted_depths =
          TestMetricExporter.datapoints_matching(@metric, %{:"db.uuid" => uuid})
          |> Enum.filter(fn dp ->
            TestMetricExporter.datapoint_attr(dp, :outcome) == :granted
          end)
          |> Enum.map(fn dp ->
            {TestMetricExporter.datapoint_attr(dp, :queue_depth_at_enqueue),
             TestMetricExporter.datapoint_attr(dp, :queue_depth_at_grant)}
          end)

        {1, 1} in granted_depths and {2, 0} in granted_depths
      end,
      timeout: 2_000,
      message: "expected granted admission.wait depths enqueue=1 grant=1 and enqueue=2 grant=0"
    )
  end

  test "immediate grant records enqueue depth 1 and grant depth 0", %{uuid: uuid} do
    assert :done =
             DatabaseAdmission.execute_owner(uuid, :foreground, fn -> :done end)

    Eventual.eventually(
      fn ->
        TestMetricExporter.datapoints_matching(@metric, %{:"db.uuid" => uuid})
        |> Enum.any?(fn dp ->
          TestMetricExporter.datapoint_attr(dp, :outcome) == :granted and
            TestMetricExporter.datapoint_attr(dp, :queue_depth_at_enqueue) == 1 and
            TestMetricExporter.datapoint_attr(dp, :queue_depth_at_grant) == 0
        end)
      end,
      timeout: 2_000,
      message: "expected immediate grant with enqueue=1 grant=0"
    )
  end

  test "cancelled outcome is recorded when a queued caller times out" do
    uuid = VialKeeper.UUID.v4()
    limit = 2
    {:ok, policy} = policy_for_limit(limit)
    {:ok, supervisor} = AdmissionSupervisor.start_link({uuid, limit, policy})

    on_exit(fn ->
      if Process.alive?(supervisor) do
        try do
          Supervisor.stop(supervisor)
        catch
          _, _ -> :ok
        end
      end
    end)

    parent = self()
    gate = make_ref()

    holder =
      spawn_link(fn ->
        DatabaseAdmission.with_token(uuid, fn ->
          send(parent, {:held, gate})
          receive(do: ({:release, ^gate} -> :ok))
        end)
      end)

    assert_receive {:held, ^gate}, 1_000

    spawn(fn ->
      try do
        DatabaseAdmission.with_permit(uuid, :foreground, 50, fn -> :never end)
      catch
        :exit, _ -> :ok
      end
    end)

    Eventual.eventually(
      fn ->
        TestMetricExporter.datapoints_matching(@metric, %{:"db.uuid" => uuid})
        |> Enum.any?(fn dp ->
          TestMetricExporter.datapoint_attr(dp, :outcome) == :cancelled and
            TestMetricExporter.datapoint_attr(dp, :queue_depth_at_enqueue) == 1 and
            TestMetricExporter.datapoint_attr(dp, :queue_depth_at_grant) == 0
        end)
      end,
      timeout: 2_000,
      message: "expected cancelled admission.wait datapoint with enqueue=1 grant=0"
    )

    send(holder, {:release, gate})
    ref = Process.monitor(holder)
    assert_receive {:DOWN, ^ref, :process, _, _}, 2_000
  end

  test "closed outcome is recorded when begin_close drains queued work" do
    uuid = VialKeeper.UUID.v4()
    limit = 2
    {:ok, policy} = policy_for_limit(limit)
    {:ok, supervisor} = AdmissionSupervisor.start_link({uuid, limit, policy})

    on_exit(fn ->
      if Process.alive?(supervisor) do
        try do
          Supervisor.stop(supervisor)
        catch
          _, _ -> :ok
        end
      end
    end)

    parent = self()
    gate = make_ref()

    holder =
      Task.async(fn ->
        DatabaseAdmission.execute_owner(uuid, :foreground, fn ->
          send(parent, {:blocked, gate, self()})
          receive(do: (:finish -> :done))
        end)
      end)

    assert_receive {:blocked, ^gate, executor_pid}, 2_000

    queued =
      Task.async(fn ->
        DatabaseAdmission.execute_owner(uuid, :foreground, fn -> :queued end)
      end)

    Eventual.eventually(
      fn ->
        case DatabaseAdmission.stats(uuid) do
          {:ok, %{queued_foreground: 1}} -> :ok
          _ -> false
        end
      end,
      timeout: 2_000,
      message: "expected queued foreground waiter"
    )

    assert :ok = DatabaseAdmission.begin_close(uuid)

    assert {:error, %VialKeeper.Error{code: :database_closed}} = Task.await(queued, 2_000)

    Eventual.eventually(
      fn ->
        TestMetricExporter.datapoints_matching(@metric, %{:"db.uuid" => uuid})
        |> Enum.any?(fn dp ->
          TestMetricExporter.datapoint_attr(dp, :outcome) == :closed and
            TestMetricExporter.datapoint_attr(dp, :queue_depth_at_enqueue) == 1 and
            TestMetricExporter.datapoint_attr(dp, :queue_depth_at_grant) == 0
        end)
      end,
      timeout: 2_000,
      message: "expected closed admission.wait datapoint with enqueue=1 grant=0"
    )

    send(executor_pid, :finish)
    assert :done = Task.await(holder, 2_000)
  end

  test "closed outcome is recorded when acquire is rejected while closing", %{uuid: uuid} do
    assert :ok = DatabaseAdmission.begin_close(uuid)

    assert {:error, %VialKeeper.Error{code: :database_closed}} =
             DatabaseAdmission.execute_owner(uuid, :foreground, fn -> :never end)

    Eventual.eventually(
      fn ->
        TestMetricExporter.datapoints_matching(@metric, %{:"db.uuid" => uuid})
        |> Enum.any?(fn dp ->
          TestMetricExporter.datapoint_attr(dp, :outcome) == :closed and
            TestMetricExporter.datapoint_attr(dp, :"admission.class") == :foreground
        end)
      end,
      timeout: 2_000,
      message: "expected closed admission.wait datapoint for closing acquire rejection"
    )
  end

  test "queue_depth attributes count queued waiters only", %{uuid: uuid} do
    parent = self()
    gate = make_ref()

    holder =
      spawn_link(fn ->
        DatabaseAdmission.with_token(uuid, fn ->
          send(parent, {:held, gate})
          receive(do: ({:release, ^gate} -> :ok))
        end)
      end)

    assert_receive {:held, ^gate}, 1_000

    overloaded =
      Task.async(fn ->
        DatabaseAdmission.with_token(uuid, fn -> :never end)
      end)

    assert {:error, %VialKeeper.Error{code: :database_overloaded}} = Task.await(overloaded, 2_000)

    Eventual.eventually(
      fn ->
        TestMetricExporter.datapoints_matching(@metric, %{:"db.uuid" => uuid})
        |> Enum.any?(fn dp ->
          TestMetricExporter.datapoint_attr(dp, :outcome) == :rejected and
            TestMetricExporter.datapoint_attr(dp, :queue_depth_at_enqueue) == 0 and
            TestMetricExporter.datapoint_attr(dp, :queue_depth_at_grant) == 0
        end)
      end,
      timeout: 2_000,
      message: "expected rejected admission.wait with queue-only depth attributes"
    )

    send(holder, {:release, gate})
    ref = Process.monitor(holder)
    assert_receive {:DOWN, ^ref, :process, _, _}, 2_000
  end

  defp policy_for_limit(limit) do
    keyword =
      Keyword.merge(AdmissionPolicy.default_keyword(),
        foreground_reserved_slots: 0,
        subscription_reserved_slots: 0,
        replication_reserved_slots: 0,
        maintenance_reserved_slots: 0
      )

    AdmissionPolicy.from_keyword(keyword, limit)
  end
end
