defmodule VialKeeper.Storage.Contracts.Derived do
  @moduledoc """
  Shared derived-materialization contract tests for storage adapters.
  """

  defmodule Support do
    @moduledoc """
    Fixtures and assertions shared by derived-materialization contract tests.
    """

    import ExUnit.Assertions

    alias VialKeeper.DerivedView.{Definition, Engine}
    alias VialKeeper.Storage.AdapterCase
    alias VialKeeper.{TempDatabase, UUID}

    def open_derived(adapter_mod) do
      source_uuid = UUID.v4()
      history_epoch = UUID.v4()
      {:ok, bundle_path} = TempDatabase.create(prefix: "vialkeeper-derived-contract")
      path = AdapterCase.adapter_path(adapter_mod, bundle_path)

      {:ok, definition} =
        Definition.normalize(
          %{
            name: "contract-view",
            sources: [source_uuid],
            map: %{key: [%{"path" => "/kind"}], value: %{"path" => "/amount"}},
            enabled: true
          },
          enforce_host_limits: false
        )

      materialization_id = UUID.v4()

      initial = Definition.initial_metadata(definition, materialization_id)

      {:ok, adapter} =
        adapter_mod.create(path, %{
          database_kind: :derived,
          initial_derived_view: initial
        })

      ExUnit.Callbacks.on_exit(fn ->
        _ = adapter_mod.close(adapter)
        TempDatabase.cleanup(bundle_path)
      end)

      %{
        adapter: adapter,
        adapter_mod: adapter_mod,
        path: path,
        source_uuid: source_uuid,
        history_epoch: history_epoch,
        materialization_id: materialization_id
      }
    end

    def open_stats_derived(adapter_mod, source_uuid) do
      {:ok, definition} =
        Definition.normalize(
          %{
            name: "stats-view",
            sources: [source_uuid],
            map: %{
              key: [%{"path" => "/kind"}, %{"path" => "/slot"}],
              value: %{"path" => "/amount"}
            },
            reduce: "_stats",
            group_level: 1,
            enabled: true
          },
          enforce_host_limits: false
        )

      materialization_id = UUID.v4()

      initial = Definition.initial_metadata(definition, materialization_id)

      {:ok, bundle_path} = TempDatabase.create(prefix: "vialkeeper-derived-stats")
      path = AdapterCase.adapter_path(adapter_mod, bundle_path)

      {:ok, adapter} =
        adapter_mod.create(path, %{database_kind: :derived, initial_derived_view: initial})

      ExUnit.Callbacks.on_exit(fn ->
        _ = adapter_mod.close(adapter)
        TempDatabase.cleanup(bundle_path)
      end)

      {adapter, materialization_id}
    end

    def map_cases(adapter_mod, ctx) do
      batch =
        Engine.batch_request(
          ctx.materialization_id,
          ctx.source_uuid,
          ctx.history_epoch,
          0,
          1,
          [Engine.source_row("one", "1-rev", ["alpha"], 2)],
          []
        )

      assert {:ok, %{applied: true, last_sequence: first_sequence}} =
               adapter_mod.apply_derived_source_batch(ctx.adapter, batch)

      assert first_sequence > 0

      generated_id = Engine.map_document_id(ctx.source_uuid, "one")

      assert {:ok, %{body: body}} =
               adapter_mod.get_document(ctx.adapter, %{document_id: generated_id})

      assert body == %{
               "key" => ["alpha"],
               "value" => 2,
               "source_database_uuid" => ctx.source_uuid,
               "source_document_id" => "one"
             }

      assert {:ok, %{applied: false, last_sequence: 0}} =
               adapter_mod.apply_derived_source_batch(ctx.adapter, batch)

      removal = %{
        batch
        | expected_checkpoint_sequence: 1,
          through_sequence: 2,
          rows: [],
          removals: ["one"]
      }

      assert {:ok, %{applied: true}} = adapter_mod.apply_derived_source_batch(ctx.adapter, removal)

      case adapter_mod.get_document(ctx.adapter, %{document_id: generated_id}) do
        {:error, %VialKeeper.Error{code: :document_not_found}} -> :ok
        {:ok, %{deleted: true}} -> :ok
        other -> flunk("expected deleted document, got: #{inspect(other)}")
      end
    end

    def stats_cases(adapter_mod, source_uuid, history_epoch) do
      {adapter, materialization_id} = open_stats_derived(adapter_mod, source_uuid)

      rows = [
        Engine.source_row("low", "1-low", ["alpha", 1], -1),
        Engine.source_row("high", "1-high", ["alpha", 2], 3),
        Engine.source_row("text", "1-text", ["alpha", 3], "skip"),
        Engine.source_row("mid", "1-mid", ["alpha", 4], 0)
      ]

      batch =
        Engine.batch_request(
          materialization_id,
          source_uuid,
          history_epoch,
          0,
          1,
          Enum.shuffle(rows),
          []
        )

      assert {:ok, %{applied: true}} = adapter_mod.apply_derived_source_batch(adapter, batch)

      {:ok, group_id} = Engine.group_document_id(["alpha"])
      assert {:ok, %{body: body}} = adapter_mod.get_document(adapter, %{document_id: group_id})

      assert body == %{
               "key" => ["alpha"],
               "value" => %{
                 "count" => 3,
                 "max" => 3.0,
                 "min" => -1.0,
                 "sum" => 2.0,
                 "sumsqr" => 10.0
               }
             }

      body
    end

    def rebuild_cases(adapter_mod, ctx) do
      batch =
        Engine.batch_request(
          ctx.materialization_id,
          ctx.source_uuid,
          ctx.history_epoch,
          0,
          1,
          [
            Engine.source_row("keep", "1-keep", ["alpha"], 1),
            Engine.source_row("drop", "1-drop", ["beta"], 2)
          ],
          []
        )

      assert {:ok, %{applied: true}} = adapter_mod.apply_derived_source_batch(ctx.adapter, batch)

      assert {:error, %VialKeeper.Error{code: :source_history_reset}} =
               adapter_mod.apply_derived_source_batch(ctx.adapter, %{
                 batch
                 | source_history_epoch: UUID.v4(),
                   expected_checkpoint_sequence: 1,
                   through_sequence: 2
               })

      assert {:ok, %{generation: generation}} =
               adapter_mod.begin_derived_source_rebuild(ctx.adapter, %{
                 materialization_id: ctx.materialization_id,
                 source_database_uuid: ctx.source_uuid,
                 start_sequence: 0
               })

      assert {:ok, _} =
               adapter_mod.apply_derived_rebuild_page(ctx.adapter, %{
                 materialization_id: ctx.materialization_id,
                 source_database_uuid: ctx.source_uuid,
                 generation: generation,
                 rows: [
                   Engine.source_row("keep", "2-keep", ["alpha"], 9)
                 ],
                 removals: [],
                 after_document_id: "keep"
               })

      assert {:ok, %{removed: 1, has_more: false}} =
               adapter_mod.prune_derived_rebuild_stale_page(ctx.adapter, %{
                 materialization_id: ctx.materialization_id,
                 source_database_uuid: ctx.source_uuid,
                 generation: generation,
                 limit: 10
               })

      new_epoch = UUID.v4()

      assert {:ok, _} =
               adapter_mod.finish_derived_source_rebuild(ctx.adapter, %{
                 materialization_id: ctx.materialization_id,
                 source_database_uuid: ctx.source_uuid,
                 generation: generation,
                 catchup_sequence: 1,
                 source_history_epoch: new_epoch
               })

      keep_id = Engine.map_document_id(ctx.source_uuid, "keep")
      drop_id = Engine.map_document_id(ctx.source_uuid, "drop")

      assert {:ok, %{body: %{"value" => 9}}} =
               adapter_mod.get_document(ctx.adapter, %{document_id: keep_id})

      case adapter_mod.get_document(ctx.adapter, %{document_id: drop_id}) do
        {:error, %VialKeeper.Error{code: :document_not_found}} -> :ok
        {:ok, %{deleted: true}} -> :ok
        other -> flunk("expected deleted document, got: #{inspect(other)}")
      end

      reopened = AdapterCase.reopen!(adapter_mod, ctx.adapter, ctx.path)

      assert {:ok, %{body: %{"value" => 9}}} =
               adapter_mod.get_document(reopened, %{document_id: keep_id})

      assert :ok = adapter_mod.close(reopened)
    end
  end

  defmacro __using__(opts) do
    quote do
      use VialKeeper.Storage.AdapterCase, unquote(opts)

      alias VialKeeper.Storage.Contracts.Derived.Support

      @adapter unquote(Keyword.fetch!(opts, :adapter))

      setup do
        {:ok, Support.open_derived(@adapter)}
      end

      test "map contributions create, replay, and remove generated documents", ctx do
        Support.map_cases(@adapter, ctx)
      end

      test "grouped reducers and numeric extremes stay stable", ctx do
        Support.stats_cases(@adapter, ctx.source_uuid, ctx.history_epoch)
      end

      test "history reset, rebuild, stale pruning, and reload", ctx do
        Support.rebuild_cases(@adapter, ctx)
      end
    end
  end
end
