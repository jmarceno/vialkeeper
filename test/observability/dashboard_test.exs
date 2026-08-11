defmodule ElixirDB.Observability.DashboardTest do
  @moduledoc "Covers observability dashboard state and HTTP rendering."

  use ExUnit.Case, async: false

  @moduletag :integration

  alias ElixirDB.Eventual
  alias ElixirDB.HTTP.Router
  alias ElixirDB.JSON.StrictDecoder
  alias ElixirDB.Observability.Dashboard
  alias ElixirDB.Runtime.{DatabaseAdmission, DatabaseCatalog}

  setup do
    Dashboard.reset()
    on_exit(fn -> Dashboard.reset() end)
    :ok
  end

  test "aggregates delta reader exports and retains the last non-empty sample" do
    Dashboard.export(
      :metrics,
      [
        histogram_metric(1, 1_000_000, [1, 0, 0], 1_000_000, 1_000_000),
        counter_metric(5),
        admission_histogram_metric(1, 500_000, [1, 0, 0], 500_000, 500_000)
      ],
      %{},
      %{}
    )

    Dashboard.export(
      :metrics,
      [
        histogram_metric(2, 3_000_000, [0, 2, 0], 2_000_000, 2_000_000),
        counter_metric(2),
        empty_histogram_metric()
      ],
      %{},
      %{}
    )

    snapshot = Dashboard.snapshot()
    http = get_in(snapshot, ["otel", "http"])

    assert http["count"] == 3
    assert http["avg_ms"] == 1.33
    assert http["p95_ms"] == 2.0
    assert get_in(snapshot, ["otel", "checkpoints", "count"]) == 7
    assert get_in(snapshot, ["otel", "admission", "count"]) == 1

    Dashboard.export(:metrics, [empty_histogram_metric()], %{}, %{})

    assert get_in(Dashboard.snapshot(), ["otel", "http", "count"]) == 3
  end

  test "snapshot HTTP route stays gated unless explicitly enabled" do
    previous = Application.get_env(:elixir_db, :observability_dashboard)

    on_exit(fn ->
      if is_nil(previous),
        do: Application.delete_env(:elixir_db, :observability_dashboard),
        else: Application.put_env(:elixir_db, :observability_dashboard, previous)
    end)

    Application.delete_env(:elixir_db, :observability_dashboard)

    disabled =
      Plug.Test.conn(:get, "/v1/observability/snapshot")
      |> Router.call([])

    assert disabled.status == 400

    Application.put_env(:elixir_db, :observability_dashboard, true)

    enabled =
      Plug.Test.conn(:get, "/v1/observability/snapshot")
      |> Router.call([])

    assert enabled.status == 200

    assert {:ok, %{"data" => %{"runtime" => runtime}}} =
             StrictDecoder.decode(enabled.resp_body)

    assert is_integer(runtime["memory_bytes"])
  end

  test "runtime snapshot exposes admission active class and per-class queue depths" do
    rel = "dashboard-admission-#{System.unique_integer([:positive])}.elixirdb"
    root = ElixirDB.Config.database_root()
    ElixirDB.TempDatabase.cleanup(Path.join(root, rel))

    assert {:ok, %{database_uuid: uuid}} = DatabaseCatalog.create(rel)
    assert {:ok, _} = DatabaseCatalog.open(uuid)

    on_exit(fn ->
      _ = DatabaseCatalog.close(uuid)
      _ = DatabaseCatalog.unregister(uuid)
      ElixirDB.TempDatabase.cleanup(Path.join(root, rel))
    end)

    parent = self()
    gate = make_ref()

    holder =
      Task.async(fn ->
        DatabaseAdmission.with_token(uuid, fn ->
          send(parent, {:held, gate})
          receive(do: ({:release, ^gate} -> :ok))
        end)
      end)

    assert_receive {:held, ^gate}, 2_000

    queued =
      Task.async(fn ->
        DatabaseAdmission.execute_owner(uuid, :subscription, fn -> :never end)
      end)

    Eventual.eventually(
      fn ->
        case DatabaseAdmission.stats(uuid) do
          {:ok, %{queued_subscription: 1, active_class: :foreground}} -> :ok
          _ -> false
        end
      end,
      timeout: 2_000,
      message: "expected foreground active permit with queued subscription waiter"
    )

    snapshot = Dashboard.snapshot()
    [entry] = get_in(snapshot, ["runtime", "admission_queues"])

    assert entry["database_uuid"] == uuid
    assert entry["active_class"] == "foreground"
    assert entry["queued_foreground"] == 0
    assert entry["queued_subscription"] == 1
    assert entry["queued_replication"] == 0
    assert entry["queued_maintenance"] == 0
    assert entry["total_occupancy"] == 2
    refute entry["closing"]

    send(holder.pid, {:release, gate})
    Task.shutdown(queued, :brutal_kill)
    Task.await(holder, 2_000)
  end

  defp histogram_metric(count, sum, bucket_counts, min, max) do
    metric(
      :"elixir_db.http.request.duration",
      {:histogram,
       [
         {:histogram_datapoint, %{}, 0, 0, count, sum, bucket_counts, [1_000_000.0, 2_000_000.0],
          [], [], min, max}
       ], :temporality_delta}
    )
  end

  defp empty_histogram_metric do
    metric(:"elixir_db.http.request.duration", {:histogram, [], :temporality_delta})
  end

  defp counter_metric(value) do
    metric(
      :"elixir_db.replication.checkpoint.count",
      {:sum, [{:datapoint, %{}, 0, 0, value, [], []}], :temporality_delta, true}
    )
  end

  defp admission_histogram_metric(count, sum, bucket_counts, min, max) do
    metric(
      :"elixir_db.database.admission.wait",
      {:histogram,
       [
         {:histogram_datapoint, %{}, 0, 0, count, sum, bucket_counts, [500_000.0, 1_000_000.0], [],
          [], min, max}
       ], :temporality_delta}
    )
  end

  defp metric(name, data), do: {:metric, name, nil, nil, nil, data}
end
