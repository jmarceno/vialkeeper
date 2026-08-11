defmodule ElixirDB.Observability.CompactMetricTest do
  use ElixirDB.Observability.OtelCase, async: false

  @moduletag :integration

  alias ElixirDB.Eventual
  alias ElixirDB.Eventual
  alias ElixirDB.Observability.{TestExporter, TestMetricExporter}
  alias ElixirDB.Runtime.DatabaseCatalog

  @compact_metric "elixir_db.database.compact.count"

  setup do
    rel = "obs-compact-#{System.unique_integer([:positive])}.elixirdb"
    {:ok, %{database_uuid: uuid}} = DatabaseCatalog.create(rel)

    on_exit(fn ->
      _ = DatabaseCatalog.close(uuid)
      _ = DatabaseCatalog.unregister(uuid)
      root = ElixirDB.Config.database_root()
      ElixirDB.TempDatabase.cleanup(Path.join(root, rel))
    end)

    [uuid: uuid]
  end

  test "compact emits span and counter with noop outcome", %{uuid: uuid} do
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

    assert {:ok, _} =
             DatabaseCatalog.command(
               uuid,
               {:command, :put, %{document_id: "doc", body: %{"n" => 1}}}
             )

    assert {:ok, stats} =
             DatabaseCatalog.command(uuid, {:command, :compact_retention, %{}})

    outcome = if Map.get(stats, :noop?, false), do: :noop, else: :committed

    Eventual.eventually(
      fn ->
        TestMetricExporter.counter_sum(@compact_metric, %{:"db.uuid" => uuid, :outcome => outcome}) >=
          1
      end,
      timeout: 2_000,
      message: "compact noop counter missing"
    )

    spans = TestExporter.spans_named("elixir_db.database.compact")
    assert Enum.any?(spans, fn s -> TestExporter.span_attr(s, :"db.uuid") == uuid end)
  end
end
