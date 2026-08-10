defmodule ElixirDB.Observability.AdmissionPrivacyTest do
  @moduledoc """
  Plan §20 privacy gate for admission telemetry: no payloads, PIDs, document ids,
  or query text in `elixir_db.database.admission.wait` attributes.
  """

  use ElixirDB.Observability.OtelCase, async: false

  alias ElixirDB.Documents
  alias ElixirDB.Eventual
  alias ElixirDB.Observability.TestMetricExporter
  alias ElixirDB.Runtime.{AdmissionPolicy, DatabaseAdmission, DatabaseCatalog}

  @metric "elixir_db.database.admission.wait"
  @doc_secret "PRIVACY_ADMISSION_DOC_SENTINEL_42"
  @query_secret "PRIVACY_ADMISSION_QUERY_SENTINEL_99"

  setup do
    previous_limits = Application.get_env(:elixir_db, :host_limits)
    previous_policy = Application.get_env(:elixir_db, :admission_policy)

    limits = Keyword.put(previous_limits || [], :admission_limit, 1)

    policy =
      Keyword.merge(
        previous_policy || AdmissionPolicy.default_keyword(),
        foreground_reserved_slots: 0,
        subscription_reserved_slots: 0,
        replication_reserved_slots: 0,
        maintenance_reserved_slots: 0
      )

    Application.put_env(:elixir_db, :host_limits, limits)
    Application.put_env(:elixir_db, :admission_policy, policy)

    on_exit(fn ->
      Application.put_env(:elixir_db, :host_limits, previous_limits)
      Application.put_env(:elixir_db, :admission_policy, previous_policy)
    end)

    rel = "obs-admission-privacy-#{System.unique_integer([:positive])}.elixirdb"
    {:ok, %{database_uuid: uuid}} = DatabaseCatalog.create(rel)

    on_exit(fn ->
      _ = DatabaseCatalog.close(uuid)
      _ = DatabaseCatalog.unregister(uuid)
      root = ElixirDB.Config.database_root()
      ElixirDB.TempDatabase.cleanup(Path.join(root, rel))
    end)

    assert {:ok, _} = DatabaseCatalog.open(uuid)

    assert {:ok, _} =
             Documents.put(uuid, %{id: @doc_secret, body: %{"marker" => @query_secret}})

    [uuid: uuid]
  end

  test "admission.wait attributes do not leak document ids, query text, or caller pids", %{
    uuid: uuid
  } do
    assert {:ok, _} =
             ElixirDB.Query.execute(uuid, %{
               "selector" => %{"/marker" => @query_secret},
               "limit" => 1
             })

    assert {:ok, _} = Documents.get(uuid, %{id: @doc_secret})

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

    assert {:error, %ElixirDB.Error{code: :database_overloaded}} =
             DatabaseAdmission.with_token(uuid, fn -> :never end)

    send(holder, {:release, gate})
    ref = Process.monitor(holder)
    assert_receive {:DOWN, ^ref, :process, _, _}, 2_000

    Eventual.eventually(
      fn ->
        datapoints_for_db(uuid) |> Enum.any?()
      end,
      timeout: 2_000,
      message: "expected admission.wait datapoints for database"
    )

    forbidden = [@doc_secret, @query_secret, inspect(holder)]

    leaks =
      for dp <- datapoints_for_db(uuid),
          value <- metric_attr_values(dp),
          secret <- forbidden,
          String.contains?(inspect(value), secret) do
        {secret, value}
      end

    assert leaks == [],
           "forbidden values leaked into admission.wait attributes: #{inspect(leaks)}"

    for dp <- datapoints_for_db(uuid) do
      assert TestMetricExporter.datapoint_attr(dp, :"db.uuid") == uuid

      for value <- metric_attr_values(dp) do
        refute is_pid(value)
        refute match?({:ok, _}, value)
      end
    end
  end

  defp datapoints_for_db(uuid) do
    TestMetricExporter.datapoints_matching(@metric, %{:"db.uuid" => uuid})
  end

  defp metric_attr_values(datapoint) do
    case datapoint[:attributes] do
      {:attributes, _, _, _, map} when is_map(map) -> Map.values(map)
      map when is_map(map) -> Map.values(map)
      _ -> []
    end
  end
end
