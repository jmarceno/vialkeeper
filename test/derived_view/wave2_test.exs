defmodule ElixirDB.DerivedView.Wave2Test do
  @moduledoc "Covers atomic derived contributions, generated documents, and exact grouped output."
  use ExUnit.Case, async: false

  alias ElixirDB.Error
  alias ElixirDB.JSON.Canonical
  alias ElixirDB.MaterializedViews
  alias ElixirDB.Runtime.DatabaseCatalog
  alias ElixirDB.Storage.FaultAdapter
  alias ElixirDB.Storage.SQLite.Adapter
  alias ElixirDB.TempDatabase

  setup do
    path = "derived-wave2-source-#{System.unique_integer([:positive])}.elixirdb"
    absolute = Path.join(ElixirDB.Config.database_root(), path)
    TempDatabase.cleanup(absolute)

    {:ok, source} = DatabaseCatalog.create(path)

    {:ok, source_identity} =
      DatabaseCatalog.command(source.database_uuid, {:command, :identity, %{}})

    on_exit(fn -> cleanup(source.database_uuid, absolute) end)

    {:ok, source: source_identity}
  end

  test "map contributions create, update, replay, and remove generated documents", %{source: source} do
    {:ok, derived, bundle} =
      create_derived(source.database_uuid, %{
        map: %{key: [%{"path" => "/kind"}], value: %{"path" => "/amount"}}
      })

    on_exit(fn -> cleanup(derived.database_uuid, bundle) end)

    batch = %{
      materialization_id: derived.materialization_id,
      source_database_uuid: source.database_uuid,
      source_history_epoch: source.history_epoch,
      expected_checkpoint_sequence: 0,
      through_sequence: 1,
      rows: [
        %{source_document_id: "one", source_revision_id: "1-rev", key: ["alpha"], value: 2}
      ],
      removals: []
    }

    assert {:ok, %{applied: true, last_sequence: first_sequence}} = apply_batch(derived, batch)
    assert first_sequence > 0

    generated_id = map_id(source.database_uuid, "one")
    assert {:ok, %{body: body}} = ElixirDB.Documents.get(derived.database_uuid, %{id: generated_id})

    assert body == %{
             "key" => ["alpha"],
             "value" => 2,
             "source_database_uuid" => source.database_uuid,
             "source_document_id" => "one"
           }

    assert {:ok, %{applied: false, last_sequence: 0}} = apply_batch(derived, batch)
    assert {:ok, %{current_sequence: ^first_sequence}} = identity(derived)

    row = hd(batch.rows)

    update = %{
      batch
      | expected_checkpoint_sequence: 1,
        through_sequence: 2,
        rows: [%{row | source_revision_id: "2-rev", value: 4}]
    }

    assert {:ok, %{applied: true}} = apply_batch(derived, update)

    assert {:ok, %{body: %{"value" => 4}}} =
             ElixirDB.Documents.get(derived.database_uuid, %{id: generated_id})

    removal = %{
      batch
      | expected_checkpoint_sequence: 2,
        through_sequence: 3,
        rows: [],
        removals: ["one"]
    }

    assert {:ok, %{applied: true}} = apply_batch(derived, removal)

    assert {:error, %ElixirDB.Error{code: :document_not_found}} =
             ElixirDB.Documents.get(derived.database_uuid, %{id: generated_id})
  end

  test "grouped reducers maintain exact output as contributions move and disappear", %{
    source: source
  } do
    {:ok, derived, bundle} =
      create_derived(source.database_uuid, %{
        map: %{key: [%{"path" => "/kind"}, %{"path" => "/month"}], value: %{"path" => "/amount"}},
        reduce: "_sum",
        group_level: 1
      })

    on_exit(fn -> cleanup(derived.database_uuid, bundle) end)

    first = %{
      materialization_id: derived.materialization_id,
      source_database_uuid: source.database_uuid,
      source_history_epoch: source.history_epoch,
      expected_checkpoint_sequence: 0,
      through_sequence: 1,
      rows: [
        %{source_document_id: "one", source_revision_id: "1-one", key: ["alpha", 1], value: 2},
        %{source_document_id: "two", source_revision_id: "1-two", key: ["alpha", 2], value: 3}
      ],
      removals: []
    }

    assert {:ok, %{applied: true}} = apply_batch(derived, first)
    alpha_id = group_id(["alpha"])

    assert {:ok, %{body: %{"key" => ["alpha"], "value" => 5.0}}} =
             ElixirDB.Documents.get(derived.database_uuid, %{id: alpha_id})

    moved = %{
      first
      | expected_checkpoint_sequence: 1,
        through_sequence: 2,
        rows: [
          %{source_document_id: "two", source_revision_id: "2-two", key: ["beta", 2], value: 3}
        ]
    }

    assert {:ok, %{applied: true}} = apply_batch(derived, moved)

    assert {:ok, %{body: %{"value" => 2.0}}} =
             ElixirDB.Documents.get(derived.database_uuid, %{id: alpha_id})

    assert {:ok, %{body: %{"value" => 3.0}}} =
             ElixirDB.Documents.get(derived.database_uuid, %{id: group_id(["beta"])})

    removed = %{
      first
      | expected_checkpoint_sequence: 2,
        through_sequence: 3,
        rows: [],
        removals: ["one", "two"]
    }

    assert {:ok, %{applied: true}} = apply_batch(derived, removed)

    assert {:error, %ElixirDB.Error{code: :document_not_found}} =
             ElixirDB.Documents.get(derived.database_uuid, %{id: alpha_id})

    assert {:error, %ElixirDB.Error{code: :document_not_found}} =
             ElixirDB.Documents.get(derived.database_uuid, %{id: group_id(["beta"])})
  end

  test "grouped sum preserves exact cancellation order", %{source: source} do
    {:ok, derived, bundle} =
      create_derived(source.database_uuid, %{
        map: %{key: [%{"path" => "/kind"}, %{"path" => "/slot"}], value: %{"path" => "/amount"}},
        reduce: "_sum",
        group_level: 1
      })

    on_exit(fn -> cleanup(derived.database_uuid, bundle) end)

    batch = %{
      materialization_id: derived.materialization_id,
      source_database_uuid: source.database_uuid,
      source_history_epoch: source.history_epoch,
      expected_checkpoint_sequence: 0,
      through_sequence: 1,
      rows: [
        %{
          source_document_id: "a",
          source_revision_id: "1-a",
          key: ["alpha", 1],
          value: 1.0
        },
        %{
          source_document_id: "b",
          source_revision_id: "1-b",
          key: ["alpha", 2],
          value: 1.1102230246251565e-16
        },
        %{
          source_document_id: "c",
          source_revision_id: "1-c",
          key: ["alpha", 3],
          value: -1.0
        }
      ],
      removals: []
    }

    assert {:ok, %{applied: true}} = apply_batch(derived, batch)

    assert {:ok, %{body: %{"key" => ["alpha"], "value" => 1.1102230246251565e-16}}} =
             ElixirDB.Documents.get(derived.database_uuid, %{id: group_id(["alpha"])})
  end

  test "rebuild generations prune stale contributions before becoming ready", %{source: source} do
    {:ok, derived, bundle} =
      create_derived(source.database_uuid, %{
        map: %{key: [%{"path" => "/kind"}]}
      })

    on_exit(fn -> cleanup(derived.database_uuid, bundle) end)

    initial = %{
      materialization_id: derived.materialization_id,
      source_database_uuid: source.database_uuid,
      source_history_epoch: source.history_epoch,
      expected_checkpoint_sequence: 0,
      through_sequence: 1,
      rows: [%{source_document_id: "old", source_revision_id: "1-old", key: ["old"]}],
      removals: []
    }

    assert {:ok, %{applied: true}} = apply_batch(derived, initial)
    old_id = map_id(source.database_uuid, "old")
    new_id = map_id(source.database_uuid, "new")

    assert {:ok, %{generation: generation}} =
             DatabaseCatalog.command(
               derived.database_uuid,
               {:command, :begin_derived_source_rebuild,
                %{
                  materialization_id: derived.materialization_id,
                  source_database_uuid: source.database_uuid,
                  start_sequence: 1,
                  catchup_sequence: 1
                }}
             )

    assert {:ok, %{changed_rows: 1}} =
             DatabaseCatalog.command(
               derived.database_uuid,
               {:command, :apply_derived_rebuild_page,
                %{
                  materialization_id: derived.materialization_id,
                  source_database_uuid: source.database_uuid,
                  generation: generation,
                  after_document_id: "new",
                  catchup_sequence: 1,
                  rows: [%{source_document_id: "new", source_revision_id: "1-new", key: ["new"]}],
                  removals: []
                }}
             )

    assert {:ok, %{removed: 1, has_more: false}} =
             DatabaseCatalog.command(
               derived.database_uuid,
               {:command, :prune_derived_rebuild_stale_page,
                %{
                  materialization_id: derived.materialization_id,
                  source_database_uuid: source.database_uuid,
                  generation: generation,
                  limit: 10
                }}
             )

    assert {:ok, %{status: :active}} =
             DatabaseCatalog.command(
               derived.database_uuid,
               {:command, :finish_derived_source_rebuild,
                %{
                  materialization_id: derived.materialization_id,
                  source_database_uuid: source.database_uuid,
                  generation: generation,
                  catchup_sequence: 1,
                  source_history_epoch: source.history_epoch
                }}
             )

    assert {:ok, %{status: :ready}} =
             DatabaseCatalog.command(derived.database_uuid, {:command, :get_derived_view, %{}})

    assert {:error, %ElixirDB.Error{code: :document_not_found}} =
             ElixirDB.Documents.get(derived.database_uuid, %{id: old_id})

    assert {:ok, %{body: %{"key" => ["new"]}}} =
             ElixirDB.Documents.get(derived.database_uuid, %{id: new_id})
  end

  test "a generated-write failure rolls back contributions and checkpoint", %{source: source} do
    {:ok, derived, bundle} =
      create_derived(source.database_uuid, %{
        map: %{key: [%{"path" => "/kind"}]}
      })

    on_exit(fn -> cleanup(derived.database_uuid, bundle) end)
    assert :ok = DatabaseCatalog.close(derived.database_uuid)
    assert :ok = DatabaseCatalog.unregister(derived.database_uuid)

    {:ok, adapter} =
      Adapter.open(Path.join(bundle, "database.sqlite3"))

    on_exit(fn -> Adapter.close(adapter) end)

    batch = %{
      materialization_id: derived.materialization_id,
      source_database_uuid: source.database_uuid,
      source_history_epoch: source.history_epoch,
      expected_checkpoint_sequence: 0,
      through_sequence: 1,
      rows: [%{source_document_id: "failed", source_revision_id: "1-failed", key: ["failed"]}],
      removals: []
    }

    fault =
      FaultAdapter.wrap(adapter)
      |> FaultAdapter.inject(
        :derived_generated_mutation,
        {:once, Error.internal_error("injected derived write failure")}
      )

    assert {:error, %Error{code: :internal_error}} =
             FaultAdapter.apply_derived_source_batch(fault, batch)

    assert {:ok, [%{checkpoint_sequence: 0}]} =
             Adapter.list_derived_sources(adapter)

    assert {:error, %Error{code: :document_not_found}} =
             Adapter.get_document(adapter, %{
               document_id: map_id(source.database_uuid, "failed")
             })
  end

  defp create_derived(source_uuid, overrides) do
    request =
      %{
        name: "Wave 2 #{System.unique_integer([:positive])}",
        sources: [source_uuid],
        map: %{key: [%{"path" => "/kind"}]}
      }
      |> deep_merge(overrides)

    {:ok, identity} = MaterializedViews.create(request)
    bundle = Path.join(ElixirDB.Config.database_root(), identity.database_path)
    {:ok, identity, bundle}
  end

  defp apply_batch(derived, request),
    do:
      DatabaseCatalog.command(
        derived.database_uuid,
        {:command, :apply_derived_source_batch, request}
      )

  defp identity(derived),
    do: DatabaseCatalog.command(derived.database_uuid, {:command, :identity, %{}})

  defp map_id(source_uuid, source_document_id),
    do: "m-" <> digest([source_uuid, source_document_id])

  defp group_id(group_key), do: "g-" <> digest(group_key)

  defp digest(value),
    do: :crypto.hash(:sha256, Canonical.encode!(value)) |> Base.encode16(case: :lower)

  defp deep_merge(left, right) do
    Map.merge(left, right, fn _key, left_value, right_value ->
      if is_map(left_value) and is_map(right_value),
        do: deep_merge(left_value, right_value),
        else: right_value
    end)
  end

  defp cleanup(uuid, bundle) do
    _ = DatabaseCatalog.close(uuid)
    _ = DatabaseCatalog.unregister(uuid)
    TempDatabase.cleanup(bundle)
  end
end
