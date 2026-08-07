defmodule ElixirDB.Observability.DashboardTest do
  use ExUnit.Case, async: false

  alias ElixirDB.HTTP.Router
  alias ElixirDB.JSON.StrictDecoder
  alias ElixirDB.Observability.Dashboard

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
        counter_metric(5)
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

  defp metric(name, data), do: {:metric, name, nil, nil, nil, data}
end
