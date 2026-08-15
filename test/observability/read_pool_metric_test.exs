defmodule VialKeeper.Observability.ReadPoolMetricTest do
  @moduledoc """
  Read-pool wait, occupancy, and exclusive-quiesce metrics with bounded,
  payload-free attributes.
  """
  use VialKeeper.Observability.OtelCase, async: false

  @moduletag :integration

  alias VialKeeper.Eventual
  alias VialKeeper.Observability.TestMetricExporter
  alias VialKeeper.Runtime.DatabaseCatalog
  alias VialKeeper.View.Manager

  @active_metric "vial_keeper.database.read_pool.active"
  @queued_metric "vial_keeper.database.read_pool.queued"
  @wait_metric "vial_keeper.database.read_pool.wait"
  @quiesce_metric "vial_keeper.database.read_pool.quiesce.duration"

  setup do
    previous_limits = Application.get_env(:vial_keeper, :host_limits)

    limits =
      (previous_limits || [])
      |> Keyword.put(:read_pool_size, 1)
      |> Keyword.put(:read_queue_limit, 1)

    Application.put_env(:vial_keeper, :host_limits, limits)

    relative = "read-pool-metric-#{System.unique_integer([:positive])}.vialkeeper"
    absolute = Path.join(VialKeeper.Config.database_root(), relative)
    VialKeeper.TempDatabase.cleanup(absolute)

    assert {:ok, identity} = DatabaseCatalog.create(relative)
    assert {:ok, _} = DatabaseCatalog.open(identity.database_uuid)
    assert :ok = Manager.await_resumed(identity.database_uuid)

    on_exit(fn ->
      Application.delete_env(:vial_keeper, :read_pool_sync)
      Application.put_env(:vial_keeper, :host_limits, previous_limits)
      _ = DatabaseCatalog.close(identity.database_uuid)
      _ = DatabaseCatalog.unregister(identity.database_uuid)
      VialKeeper.TempDatabase.cleanup(absolute)
    end)

    {:ok, uuid: identity.database_uuid}
  end

  test "emits occupancy, wait outcomes, and quiesce duration", %{uuid: uuid} do
    assert {:ok, _} = VialKeeper.Documents.put(uuid, %{id: "doc", body: %{"n" => 1}})

    gate = make_ref()
    Application.put_env(:vial_keeper, :read_pool_sync, {self(), gate, uuid, :get_document})

    holder = Task.async(fn -> VialKeeper.Documents.get(uuid, %{id: "doc"}) end)
    assert_receive {^gate, :before_begin, worker}, 2_000

    waiter = Task.async(fn -> VialKeeper.Documents.get(uuid, %{id: "doc"}) end)

    Eventual.eventually(
      fn ->
        TestMetricExporter.counter_sum(@active_metric, %{:"db.uuid" => uuid}) >= 1 and
          TestMetricExporter.counter_sum(@queued_metric, %{:"db.uuid" => uuid}) >= 1
      end,
      timeout: 2_000,
      message: "expected active and queued read-pool occupancy datapoints"
    )

    assert {:error, %VialKeeper.Error{code: :database_overloaded}} =
             VialKeeper.Documents.get(uuid, %{id: "doc"})

    Application.delete_env(:vial_keeper, :read_pool_sync)
    send(worker, {:go, gate})
    assert {:ok, %{id: "doc"}} = Task.await(holder, 5_000)
    assert {:ok, %{id: "doc"}} = Task.await(waiter, 5_000)

    Eventual.eventually(
      fn ->
        outcomes =
          TestMetricExporter.datapoints_matching(@wait_metric, %{:"db.uuid" => uuid})
          |> Enum.map(&TestMetricExporter.datapoint_attr(&1, :outcome))

        :granted in outcomes and :rejected in outcomes
      end,
      timeout: 2_000,
      message: "expected granted and rejected read-pool wait datapoints"
    )

    for datapoint <- TestMetricExporter.datapoints_matching(@wait_metric, %{:"db.uuid" => uuid}) do
      assert TestMetricExporter.datapoint_attr(datapoint, :"admission.class") in [
               :foreground,
               :subscription,
               :replication,
               :maintenance
             ]

      assert TestMetricExporter.datapoint_attr(datapoint, :outcome) in [
               :granted,
               :rejected,
               :cancelled,
               :closed
             ]

      for key <- [:queue_depth_at_enqueue, :queue_depth_at_grant] do
        depth = TestMetricExporter.datapoint_attr(datapoint, key)
        assert is_integer(depth) and depth >= 0 and depth <= 1
      end
    end

    assert {:ok, _} =
             DatabaseCatalog.command(uuid, {:command, :integrity_check, %{}})

    Eventual.eventually(
      fn ->
        TestMetricExporter.datapoints_matching(@quiesce_metric, %{:"db.uuid" => uuid}) != []
      end,
      timeout: 2_000,
      message: "expected exclusive quiesce duration datapoint"
    )
  end
end
