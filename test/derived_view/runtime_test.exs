defmodule ElixirDB.DerivedView.RuntimeTest do
  @moduledoc "Covers runtime materialization, source changes, and derived-session recovery."
  use ExUnit.Case, async: false

  alias ElixirDB.DerivedView.Worker
  alias ElixirDB.Eventual
  alias ElixirDB.JSON.Canonical
  alias ElixirDB.MaterializedViews
  alias ElixirDB.Runtime.DatabaseCatalog
  alias ElixirDB.TempDatabase

  setup do
    path = "derived-runtime-source-#{System.unique_integer([:positive])}.elixirdb"
    absolute = Path.join(ElixirDB.Config.database_root(), path)
    TempDatabase.cleanup(absolute)

    {:ok, source} = DatabaseCatalog.create(path)
    on_exit(fn -> cleanup(source.database_uuid, absolute) end)

    {:ok, source_uuid: source.database_uuid}
  end

  test "rebuilds a source and follows later updates and removals", %{source_uuid: source_uuid} do
    assert {:ok, first} =
             ElixirDB.Documents.put(source_uuid, %{
               id: "one",
               body: %{"kind" => "sale", "amount" => 2}
             })

    {:ok, derived, derived_bundle} = create_derived(source_uuid)
    on_exit(fn -> cleanup(derived.database_uuid, derived_bundle) end)

    generated_id = map_id(source_uuid, "one")

    assert Eventual.eventually(
             fn ->
               case ElixirDB.Documents.get(derived.database_uuid, %{id: generated_id}) do
                 {:ok, %{body: %{"value" => 2}}} -> true
                 _ -> false
               end
             end,
             message: "materialized snapshot was not generated"
           )

    assert {:ok, updated} =
             ElixirDB.Documents.put(source_uuid, %{
               id: "one",
               if_revision: first.revision,
               body: %{"kind" => "sale", "amount" => 7}
             })

    assert Eventual.eventually(
             fn ->
               case ElixirDB.Documents.get(derived.database_uuid, %{id: generated_id}) do
                 {:ok, %{body: %{"value" => 7}}} -> true
                 _ -> false
               end
             end,
             message: "materialized update was not generated"
           )

    assert {:ok, _deleted} =
             ElixirDB.Documents.delete(source_uuid, %{id: "one", if_revision: updated.revision})

    assert Eventual.eventually(
             fn ->
               match?(
                 {:error, %ElixirDB.Error{code: :document_not_found}},
                 ElixirDB.Documents.get(derived.database_uuid, %{id: generated_id})
               )
             end,
             message: "materialized removal was not generated"
           )

    assert Eventual.eventually(
             fn ->
               case DatabaseCatalog.command(
                      derived.database_uuid,
                      {:command, :get_derived_view, %{}}
                    ) do
                 {:ok, %{status: :current}} -> true
                 _ -> false
               end
             end,
             message: "derived view did not become current"
           )
  end

  test "reopens a derived session from durable source state", %{source_uuid: source_uuid} do
    {:ok, derived, derived_bundle} = create_derived(source_uuid)
    on_exit(fn -> cleanup(derived.database_uuid, derived_bundle) end)

    assert {:ok, first} =
             ElixirDB.Documents.put(source_uuid, %{
               id: "restart",
               body: %{"kind" => "sale", "amount" => 3}
             })

    generated_id = map_id(source_uuid, "restart")

    assert Eventual.eventually(
             fn ->
               match?({:ok, _}, ElixirDB.Documents.get(derived.database_uuid, %{id: generated_id}))
             end,
             message: "materializer did not process the first change"
           )

    assert {:ok, %{enabled: false}} =
             DatabaseCatalog.command(
               derived.database_uuid,
               {:command, :set_derived_enabled,
                %{materialization_id: derived.materialization_id, enabled: false}}
             )

    assert :ok = DatabaseCatalog.close(derived.database_uuid)
    assert :error = Worker.pid(derived.database_uuid)
    assert {:ok, _} = DatabaseCatalog.open(derived.database_uuid)

    assert {:ok, %{enabled: true}} =
             DatabaseCatalog.command(
               derived.database_uuid,
               {:command, :set_derived_enabled,
                %{materialization_id: derived.materialization_id, enabled: true}}
             )

    assert {:ok, _second} =
             ElixirDB.Documents.put(source_uuid, %{
               id: "restart",
               if_revision: first.revision,
               body: %{"kind" => "sale", "amount" => 9}
             })

    assert Eventual.eventually(
             fn ->
               case ElixirDB.Documents.get(derived.database_uuid, %{id: generated_id}) do
                 {:ok, %{body: %{"value" => 9}}} -> true
                 _ -> false
               end
             end,
             message: "materializer did not recover after reopening"
           )
  end

  test "restarting a worker also replaces its source-task supervisor", %{source_uuid: source_uuid} do
    {:ok, derived, derived_bundle} = create_derived(source_uuid)
    on_exit(fn -> cleanup(derived.database_uuid, derived_bundle) end)

    assert Eventual.eventually(
             fn -> match?({:ok, _pid}, Worker.pid(derived.database_uuid)) end,
             message: "derived worker did not start"
           )

    {:ok, old_worker} = Worker.pid(derived.database_uuid)

    [{old_tasks, _}] =
      Registry.lookup(
        ElixirDB.Runtime.DatabaseRegistry,
        {:derived_task_supervisor, derived.database_uuid}
      )

    Process.exit(old_worker, :kill)

    assert Eventual.eventually(
             fn ->
               with {:ok, worker} <- Worker.pid(derived.database_uuid),
                    [{tasks, _}] <-
                      Registry.lookup(
                        ElixirDB.Runtime.DatabaseRegistry,
                        {:derived_task_supervisor, derived.database_uuid}
                      ) do
                 worker != old_worker and tasks != old_tasks
               else
                 _ -> false
               end
             end,
             message: "derived worker restart did not replace its task supervisor"
           )
  end

  test "processes multiple sources in stable order and refreshes grouped output", %{
    source_uuid: first_source_uuid
  } do
    second_path = "derived-runtime-second-#{System.unique_integer([:positive])}.elixirdb"
    second_bundle = Path.join(ElixirDB.Config.database_root(), second_path)
    TempDatabase.cleanup(second_bundle)
    {:ok, second_source} = DatabaseCatalog.create(second_path)

    on_exit(fn -> cleanup(second_source.database_uuid, second_bundle) end)

    assert {:ok, _} =
             ElixirDB.Documents.put(first_source_uuid, %{
               id: "first",
               body: %{"kind" => "sale", "slot" => 1, "amount" => 2}
             })

    assert {:ok, second_put} =
             ElixirDB.Documents.put(second_source.database_uuid, %{
               id: "second",
               body: %{"kind" => "sale", "slot" => 2, "amount" => 5}
             })

    request = %{
      name: "Grouped Runtime Materialization #{System.unique_integer([:positive])}",
      sources: [first_source_uuid, second_source.database_uuid],
      map: %{
        key: [%{"path" => "/kind"}, %{"path" => "/slot"}],
        value: %{"path" => "/amount"}
      },
      reduce: "_sum",
      group_level: 1
    }

    {:ok, derived} = MaterializedViews.create(request)
    derived_bundle = Path.join(ElixirDB.Config.database_root(), derived.database_path)
    on_exit(fn -> cleanup(derived.database_uuid, derived_bundle) end)

    generated_id = group_id(["sale"])

    assert Eventual.eventually(
             fn ->
               case ElixirDB.Documents.get(derived.database_uuid, %{id: generated_id}) do
                 {:ok, %{body: %{"value" => 7.0}}} -> true
                 _ -> false
               end
             end,
             message: "grouped output did not include both sources"
           )

    assert {:ok, _} =
             ElixirDB.Documents.put(second_source.database_uuid, %{
               id: "second",
               if_revision: second_put.revision,
               body: %{"kind" => "sale", "slot" => 2, "amount" => 8}
             })

    assert Eventual.eventually(
             fn ->
               case ElixirDB.Documents.get(derived.database_uuid, %{id: generated_id}) do
                 {:ok, %{body: %{"value" => 10.0}}} -> true
                 _ -> false
               end
             end,
             message: "grouped output did not refresh after a source update"
           )
  end

  test "pauses and resumes source processing when disabled", %{source_uuid: source_uuid} do
    {:ok, derived, derived_bundle} = create_derived(source_uuid)
    on_exit(fn -> cleanup(derived.database_uuid, derived_bundle) end)

    assert {:ok, first} =
             ElixirDB.Documents.put(source_uuid, %{
               id: "paused",
               body: %{"kind" => "sale", "amount" => 1}
             })

    generated_id = map_id(source_uuid, "paused")

    assert Eventual.eventually(
             fn ->
               case ElixirDB.Documents.get(derived.database_uuid, %{id: generated_id}) do
                 {:ok, %{body: %{"value" => 1}}} -> true
                 _ -> false
               end
             end,
             message: "materializer did not generate the initial value"
           )

    assert {:ok, %{enabled: false}} =
             DatabaseCatalog.command(
               derived.database_uuid,
               {:command, :set_derived_enabled,
                %{materialization_id: derived.materialization_id, enabled: false}}
             )

    assert {:ok, _paused_update} =
             ElixirDB.Documents.put(source_uuid, %{
               id: "paused",
               if_revision: first.revision,
               body: %{"kind" => "sale", "amount" => 4}
             })

    assert Eventual.eventually_equal(
             1,
             fn ->
               case ElixirDB.Documents.get(derived.database_uuid, %{id: generated_id}) do
                 {:ok, %{body: %{"value" => value}}} -> value
                 _ -> nil
               end
             end,
             message: "disabled materializer changed its output"
           )

    assert {:ok, %{enabled: true}} =
             DatabaseCatalog.command(
               derived.database_uuid,
               {:command, :set_derived_enabled,
                %{materialization_id: derived.materialization_id, enabled: true}}
             )

    assert Eventual.eventually(
             fn ->
               case ElixirDB.Documents.get(derived.database_uuid, %{id: generated_id}) do
                 {:ok, %{body: %{"value" => 4}}} -> true
                 _ -> false
               end
             end,
             message: "materializer did not resume after being enabled"
           )
  end

  defp create_derived(source_uuid) do
    request = %{
      name: "Runtime Materialization #{System.unique_integer([:positive])}",
      sources: [source_uuid],
      map: %{key: [%{"path" => "/kind"}], value: %{"path" => "/amount"}}
    }

    {:ok, identity} = MaterializedViews.create(request)
    bundle = Path.join(ElixirDB.Config.database_root(), identity.database_path)
    {:ok, identity, bundle}
  end

  defp map_id(source_uuid, document_id),
    do: "m-" <> digest([source_uuid, document_id])

  defp group_id(group_key), do: "g-" <> digest(group_key)

  defp digest(value),
    do: :crypto.hash(:sha256, Canonical.encode!(value)) |> Base.encode16(case: :lower)

  defp cleanup(uuid, bundle) do
    disable_derived(uuid)
    _ = DatabaseCatalog.close(uuid)
    _ = DatabaseCatalog.unregister(uuid)
    TempDatabase.cleanup(bundle)
  end

  defp disable_derived(uuid) do
    case DatabaseCatalog.info(uuid) do
      {:ok, %{database_kind: :derived}} ->
        _ =
          DatabaseCatalog.command(
            uuid,
            {:command, :set_derived_enabled, %{enabled: false}}
          )

        :ok

      _ ->
        :ok
    end
  end
end
