for {name, adapter_module} <- [
      {"SQLite", VialKeeper.Storage.SQLite.Adapter},
      {"Memory", VialKeeper.Storage.Memory.Adapter}
    ] do
  defmodule Module.concat([VialKeeper.Contract, "#{name}RevisionAdapterPropertiesTest"]) do
    @moduledoc """
    Random operation histories produce identical revision trees,
    winners, active conflicts, tombstones, and replay results in the pure model and
    the #{name} adapter (including after close/reopen or Memory snapshot reload).
    """

    use ExUnit.Case, async: false
    use ExUnitProperties

    alias VialKeeper.ModelGenerators
    alias VialKeeper.RevisionFixtures
    alias VialKeeper.RevisionHistoryModel
    alias VialKeeper.Revisions.{Id, Winner}
    alias VialKeeper.Storage.AdapterCase
    alias VialKeeper.Storage.Services
    alias VialKeeper.Storage.Services.Facts

    @adapter adapter_module
    @moduletag :property

    property "adapter and pure model agree on trees, winners, conflicts, tombstones, and replay" do
      check all(
              history <- ModelGenerators.revision_operation_history(),
              max_runs: 40
            ) do
        {:ok, bundle_path} = VialKeeper.TempDatabase.create(prefix: "vialkeeper-rev-props")
        path = AdapterCase.adapter_path(@adapter, bundle_path)
        {:ok, adapter} = @adapter.create(path, %{})

        try do
          model = RevisionHistoryModel.new()
          {model, _adapter, _last_req} = run_history(history.operations, model, adapter, nil)

          model_snap = RevisionHistoryModel.snapshot(model)

          reopened = AdapterCase.reopen!(@adapter, adapter, path)

          try do
            adapter_snap = adapter_snapshot(reopened, history.document_id)
            assert_equivalent_snapshots(model_snap, adapter_snap)
            assert_materialized_document(reopened, model_snap, history.document_id)
          after
            _ = @adapter.close(reopened)
          end
        after
          VialKeeper.TempDatabase.cleanup(bundle_path)
        end
      end
    end

    property "sibling import then resolve modes match Winner and ConflictResolution projections" do
      check all(
              history <-
                StreamData.one_of([
                  sibling_history(:surviving_body),
                  sibling_history(:delete_all)
                ]),
              max_runs: 25
            ) do
        {:ok, bundle_path} = VialKeeper.TempDatabase.create(prefix: "vialkeeper-rev-resolve")
        path = AdapterCase.adapter_path(@adapter, bundle_path)
        {:ok, adapter} = @adapter.create(path, %{})

        try do
          model = RevisionHistoryModel.new()
          {model, adapter, _} = run_history(history.operations, model, adapter, nil)
          model_snap = RevisionHistoryModel.snapshot(model)

          reopened = AdapterCase.reopen!(@adapter, adapter, path)

          try do
            adapter_snap = adapter_snapshot(reopened, history.document_id)

            assert adapter_snap.leaf_set == model_snap.leaf_set
            assert adapter_snap.live_leaf_set == model_snap.live_leaf_set
            assert adapter_snap.winner == model_snap.winner
            assert adapter_snap.winner_deleted == model_snap.winner_deleted
            assert adapter_snap.conflicts == model_snap.conflicts
            assert adapter_snap.tombstones == model_snap.tombstones
            assert adapter_snap.tree == model_snap.tree
            assert_materialized_document(reopened, model_snap, history.document_id)
          after
            _ = @adapter.close(reopened)
          end
        after
          VialKeeper.TempDatabase.cleanup(bundle_path)
        end
      end
    end

    property "stale if_revision and wrong CAS expected set are rejected consistently" do
      check all(
              document_id <- ModelGenerators.document_id(),
              body <- ModelGenerators.document_body(),
              next_body <- ModelGenerators.document_body(),
              stale_body <- ModelGenerators.document_body(),
              max_runs: 20
            ) do
        body = Map.put(body, "_v", 1)
        next_body = Map.put(next_body, "_v", 2)
        stale_body = Map.put(stale_body, "_v", 3)
        {:ok, bundle_path} = VialKeeper.TempDatabase.create(prefix: "vialkeeper-rev-stale")
        path = AdapterCase.adapter_path(@adapter, bundle_path)
        {:ok, adapter} = @adapter.create(path, %{})

        try do
          model = RevisionHistoryModel.new()

          {model, adapter, _} =
            run_history(
              [%{op: :put, document_id: document_id, body: body, if_revision: nil}],
              model,
              adapter,
              nil
            )

          assert {:ok, %{revision: first}} = model.last_result

          {model, adapter, _} =
            run_history(
              [%{op: :put, document_id: document_id, body: next_body, if_revision: :winner}],
              model,
              adapter,
              nil
            )

          assert match?({:ok, _}, model.last_result)

          {model, adapter, _} =
            run_history(
              [%{op: :put, document_id: document_id, body: stale_body, if_revision: first}],
              model,
              adapter,
              nil
            )

          assert match?({:error, :revision_conflict}, model.last_result)

          model_snap = RevisionHistoryModel.snapshot(model)
          reopened = AdapterCase.reopen!(@adapter, adapter, path)

          try do
            assert_materialized_document(reopened, model_snap, document_id)
            adapter_snap = adapter_snapshot(reopened, document_id)
            assert_equivalent_snapshots(model_snap, adapter_snap)
          after
            _ = @adapter.close(reopened)
          end
        after
          VialKeeper.TempDatabase.cleanup(bundle_path)
        end
      end
    end

    test "import order of the same revision multiset does not change the resulting tree" do
      document_id = "order-indep"
      fixture = order_independence_fixture(document_id)

      snapshots =
        Enum.map(import_order_permutations(), fn order ->
          {:ok, bundle_path} = VialKeeper.TempDatabase.create(prefix: "vialkeeper-rev-order")
          path = AdapterCase.adapter_path(@adapter, bundle_path)
          {:ok, adapter} = @adapter.create(path, %{})

          try do
            Enum.each(order, fn chain_key ->
              chain = Map.fetch!(fixture.chains, chain_key)

              assert {:ok, _} =
                       Services.import_revision_chains(@adapter.to_context(adapter), %{
                         chains: [chain]
                       })
            end)

            reopened = AdapterCase.reopen!(@adapter, adapter, path)

            try do
              adapter_snapshot(reopened, document_id)
            after
              _ = @adapter.close(reopened)
            end
          after
            VialKeeper.TempDatabase.cleanup(bundle_path)
          end
        end)

      assert Enum.count_until(snapshots, 25) == 24
      [first | rest] = snapshots

      assert first.winner == fixture.expected_winner
      assert first.winner_deleted == false
      assert fixture.tombstone in first.tombstones
      refute first.winner == fixture.tombstone

      history_id = RevisionFixtures.shared_history_id()

      assert MapSet.new(first.leaf_set) ==
               MapSet.new([
                 %{revision: fixture.left, history_id: history_id, deleted: false},
                 %{revision: fixture.right, history_id: history_id, deleted: false},
                 %{revision: fixture.tombstone, history_id: history_id, deleted: true}
               ])

      assert MapSet.new(first.live_leaf_set) == MapSet.new([fixture.left, fixture.right])

      assert MapSet.new(first.conflicts) ==
               MapSet.new([fixture.left, fixture.right] -- [fixture.expected_winner])

      for snap <- rest do
        assert_equivalent_snapshots(first, snap)
      end
    end

    test "list_ancestors and get_revision_chains agree on depth-8 chain" do
      {:ok, bundle_path} = VialKeeper.TempDatabase.create(prefix: "vialkeeper-rev-depth8")
      path = AdapterCase.adapter_path(@adapter, bundle_path)
      {:ok, adapter} = @adapter.create(path, %{})

      try do
        document_id = "depth-8"
        {leaf, revisions} = build_linear_chain(adapter, document_id, 8)
        context = @adapter.to_context(adapter)

        {:ok, %{chains: [chain]}} =
          @adapter.get_revision_chains(adapter, %{
            documents: [%{document_id: document_id, leaf_revisions: [leaf]}]
          })

        expected_ids = revisions

        assert Enum.map(chain.revisions, & &1.revision_id) == expected_ids
        assert chain.truncated == false
        assert chain_via_find(context, document_id, leaf) == expected_ids

        {:ok, %{chains: [complete_truncated_request]}} =
          @adapter.get_revision_chains(adapter, %{
            documents: [
              %{document_id: document_id, leaf_revisions: [leaf], truncated: true}
            ]
          })

        assert complete_truncated_request.truncated == false
        assert Enum.map(complete_truncated_request.revisions, & &1.revision_id) == expected_ids

        {:ok, ancestors} = Facts.list_ancestors(context, document_id, leaf)

        assert Enum.map(Enum.reverse(ancestors), & &1.revision_id) ++ [leaf] == expected_ids
      after
        _ = @adapter.close(adapter)
        VialKeeper.TempDatabase.cleanup(bundle_path)
      end
    end

    defp sibling_history(mode) do
      StreamData.bind(ModelGenerators.document_id(), fn document_id ->
        StreamData.bind(
          StreamData.tuple({
            ModelGenerators.document_body(),
            ModelGenerators.document_body(),
            ModelGenerators.document_body(),
            ModelGenerators.document_body(),
            StreamData.member_of([:left, :right, :winner])
          }),
          fn tuple ->
            sibling_history_case(document_id, tuple, mode)
          end
        )
      end)
    end

    defp sibling_history_case(document_id, {root, left, right, body, side}, mode) do
      left = distinct_left_body(left, right)
      right = Map.put(right, "_side", "right")

      StreamData.constant(%{
        document_id: document_id,
        operations: [
          %{
            op: :import_siblings,
            document_id: document_id,
            root_body: root,
            left_body: left,
            right_body: right
          },
          %{
            op: :resolve,
            document_id: document_id,
            mode: mode,
            chosen_side: side,
            body: body,
            expected: :current_live
          }
        ]
      })
    end

    defp run_history(operations, model, adapter, last_request) do
      Enum.reduce(operations, {model, adapter, last_request}, fn op, {model, adapter, last_req} ->
        case op do
          %{op: :replay_last} ->
            apply_both(model, adapter, %{op: :replay_last}, last_req)

          other ->
            apply_both(model, adapter, other, last_req)
        end
      end)
    end

    defp apply_both(model, adapter, %{op: :replay_last}, last_req) do
      model_next = RevisionHistoryModel.apply_operation(model, %{op: :replay_last})
      adapter_result = apply_adapter_op(adapter, %{op: :replay_last}, last_req)
      assert_matching_results(model_next.last_result, adapter_result)
      {model_next, adapter, last_req}
    end

    defp apply_both(model, adapter, op, _last_req) do
      model_next = RevisionHistoryModel.apply_operation(model, op)

      concrete =
        case model_next.last_concrete_op do
          %{op: op_name} = concrete when op_name == op.op ->
            enrich_concrete_for_adapter(model_next, concrete)

          _ ->
            materialize_for_adapter(model_next, op)
        end

      adapter_result = apply_adapter_op(adapter, concrete, concrete)
      assert_matching_results(model_next.last_result, adapter_result)
      {model_next, adapter, concrete}
    end

    defp enrich_concrete_for_adapter(model, %{op: :put} = concrete) do
      if is_nil(concrete[:if_revision]) do
        case root_history_id(model, concrete.document_id) do
          nil -> concrete
          history_id -> Map.put(concrete, :history_id, history_id)
        end
      else
        concrete
      end
    end

    defp enrich_concrete_for_adapter(_model, concrete), do: concrete

    defp materialize_for_adapter(model, %{op: :put} = op) do
      parent =
        case op[:if_revision] do
          :winner -> RevisionHistoryModel.snapshot(model).winner
          other -> other
        end

      base = %{op: :put, document_id: op.document_id, body: op.body, if_revision: parent}

      if is_nil(parent) do
        case root_history_id(model, op.document_id) do
          nil -> base
          history_id -> Map.put(base, :history_id, history_id)
        end
      else
        base
      end
    end

    defp materialize_for_adapter(model, %{op: :delete} = op) do
      parent =
        case op[:if_revision] do
          :winner -> RevisionHistoryModel.snapshot(model).winner
          other -> other
        end

      %{op: :delete, document_id: op.document_id, if_revision: parent}
    end

    defp materialize_for_adapter(_model, op), do: op

    defp root_history_id(model, document_id) do
      model.revisions
      |> Map.values()
      |> Enum.filter(&(&1.document_id == document_id))
      |> Enum.max_by(& &1.generation, fn -> nil end)
      |> case do
        nil -> nil
        %{history_id: history_id} -> history_id
      end
    end

    defp apply_adapter_op(adapter, %{op: :replay_last}, last_req) when is_map(last_req) do
      apply_adapter_op(adapter, last_req, last_req)
    end

    defp apply_adapter_op(_adapter, %{op: :replay_last}, nil),
      do: {:error, :invalid_request}

    defp apply_adapter_op(adapter, %{op: :put} = op, _) do
      request = %{
        operation: :put,
        document_id: op.document_id,
        body: op.body,
        if_revision: op[:if_revision]
      }

      request =
        if is_binary(op[:history_id]) do
          Map.put(request, :history_id, op.history_id)
        else
          request
        end

      normalize_adapter_result(Services.apply_local_mutation(@adapter.to_context(adapter), request))
    end

    defp apply_adapter_op(adapter, %{op: :delete} = op, _) do
      request = %{
        operation: :delete,
        document_id: op.document_id,
        if_revision: op[:if_revision]
      }

      normalize_adapter_result(Services.apply_local_mutation(@adapter.to_context(adapter), request))
    end

    defp apply_adapter_op(adapter, %{op: :import_siblings} = op, _) do
      document_id = op.document_id
      history_id = RevisionFixtures.shared_history_id()
      {:ok, root} = Id.calculate(document_id, history_id, nil, false, op.root_body, %{})
      {:ok, left} = Id.calculate(document_id, history_id, root, false, op.left_body, %{})
      {:ok, right} = Id.calculate(document_id, history_id, root, false, op.right_body, %{})

      left_chain = %{
        document_id: document_id,
        leaf_revision: left,
        revisions: [
          wire(document_id, root, nil, false, op.root_body),
          wire(document_id, left, root, false, op.left_body)
        ]
      }

      right_chain = %{
        document_id: document_id,
        leaf_revision: right,
        revisions: [
          wire(document_id, root, nil, false, op.root_body),
          wire(document_id, right, root, false, op.right_body)
        ]
      }

      context = @adapter.to_context(adapter)

      with {:ok, _} <- Services.import_revision_chains(context, %{chains: [left_chain]}),
           {:ok, _} <- Services.import_revision_chains(context, %{chains: [right_chain]}) do
        {:ok, %{imported: [root, left, right], replayed: false}}
      else
        {:error, %VialKeeper.Error{code: code}} -> {:error, code}
      end
    end

    defp apply_adapter_op(adapter, %{op: :resolve} = op, _) do
      leaves = current_adapter_leaves(adapter, op.document_id)
      live = Winner.live_leaves(leaves)

      expected = expected_adapter_revisions(live, op[:expected])
      chosen = chosen_adapter_revision(op, live)
      request = resolution_request(op, expected, chosen)

      normalize_adapter_result(Services.resolve_conflict(@adapter.to_context(adapter), request))
    end

    defp expected_adapter_revisions(_live, expected) when is_list(expected), do: expected
    defp expected_adapter_revisions(live, _expected), do: Enum.map(live, & &1.revision_id)

    defp chosen_adapter_revision(%{chosen_parent_revision: chosen}, _live) when is_binary(chosen),
      do: chosen

    defp chosen_adapter_revision(%{mode: :delete_all}, _live), do: nil

    defp chosen_adapter_revision(%{chosen_side: :left}, live),
      do: live |> Enum.map(& &1.revision_id) |> Enum.min()

    defp chosen_adapter_revision(%{chosen_side: :right}, live),
      do: live |> Enum.map(& &1.revision_id) |> Enum.max()

    defp chosen_adapter_revision(_op, live) do
      {:ok, winner} = Winner.select(live)
      winner.revision_id
    end

    defp resolution_request(op, expected, chosen) do
      request = %{
        document_id: op.document_id,
        expected_live_revisions: expected,
        delete_all: op.mode == :delete_all
      }

      if op.mode == :delete_all,
        do: request,
        else: Map.merge(request, %{chosen_parent_revision: chosen, body: op.body})
    end

    defp normalize_adapter_result({:ok, result}) when is_map(result) do
      {:ok,
       %{
         revision: Map.get(result, :revision),
         replayed: Map.get(result, :replayed, false),
         conflicts: Map.get(result, :conflicts)
       }
       |> Map.reject(fn {_k, v} -> is_nil(v) end)}
    end

    defp normalize_adapter_result({:error, %VialKeeper.Error{code: code}}), do: {:error, code}

    defp assert_matching_results({:ok, model_result}, {:ok, adapter_result}) do
      assert is_map(model_result)
      assert is_map(adapter_result)

      assert Map.get(model_result, :replayed, false) == Map.get(adapter_result, :replayed, false)

      # Put/delete/resolve success must expose matching revision ids (not soft/optional).
      if Map.has_key?(model_result, :revision) or Map.has_key?(adapter_result, :revision) do
        assert Map.has_key?(model_result, :revision)
        assert Map.has_key?(adapter_result, :revision)
        assert model_result.revision == adapter_result.revision
      end

      if is_list(Map.get(model_result, :conflicts)) or is_list(Map.get(adapter_result, :conflicts)) do
        assert Enum.sort(List.wrap(model_result[:conflicts])) ==
                 Enum.sort(List.wrap(adapter_result[:conflicts]))
      end
    end

    defp assert_matching_results({:error, code}, {:error, code}), do: :ok

    defp assert_matching_results(model_result, adapter_result) do
      flunk("""
      model and adapter diverged on operation result
      model: #{inspect(model_result)}
      adapter: #{inspect(adapter_result)}
      """)
    end

    defp assert_equivalent_snapshots(model, adapter) do
      assert adapter.tree == model.tree, """
      revision trees differ
      model: #{inspect(model.tree)}
      adapter: #{inspect(adapter.tree)}
      """

      assert adapter.leaf_set == model.leaf_set
      assert adapter.live_leaf_set == model.live_leaf_set
      assert adapter.winner == model.winner
      assert adapter.winner_deleted == model.winner_deleted
      assert adapter.conflicts == model.conflicts
      assert adapter.tombstones == model.tombstones
    end

    defp assert_materialized_document(adapter, model_snap, document_id) do
      case @adapter.get_document(adapter, %{document_id: document_id}) do
        {:ok, doc} ->
          assert doc.revision == model_snap.winner
          assert doc.deleted == model_snap.winner_deleted
          assert_document_body(doc, model_snap)

        {:error, %VialKeeper.Error{code: :document_not_found}} ->
          # Adapter may surface deleted winners as document_not_found.
          assert is_nil(model_snap.winner) or model_snap.winner_deleted == true
      end
    end

    defp assert_document_body(%{body: body}, %{winner_deleted: true}),
      do: assert(is_nil(body) or body == %{})

    defp assert_document_body(%{body: body}, model_snap),
      do: assert(body == model_body(model_snap))

    defp model_body(%{tree: tree, winner: winner}) do
      case Enum.find(tree, fn rev -> rev.revision_id == winner end) do
        nil -> nil
        rev -> rev.body
      end
    end

    defp distinct_left_body(left, right) when left == right,
      do: Map.put(left, "_side", "left")

    defp distinct_left_body(left, _right), do: left

    defp adapter_snapshot(adapter, document_id) do
      context = @adapter.to_context(adapter)

      case Facts.find_document(context, document_id) do
        {:ok, nil} ->
          empty_snapshot(document_id)

        {:ok, _doc} ->
          case Facts.list_leaves(context, document_id) do
            {:ok, leaves} ->
              revisions = collect_revisions(context, document_id, leaves)
              build_snapshot(document_id, revisions, leaves)

            {:error, _} ->
              empty_snapshot(document_id)
          end

        {:error, _} ->
          empty_snapshot(document_id)
      end
    end

    defp empty_snapshot(document_id) do
      %{
        document_id: document_id,
        tree: [],
        leaf_set: [],
        live_leaf_set: [],
        winner: nil,
        winner_deleted: nil,
        conflicts: [],
        tombstones: []
      }
    end

    defp build_snapshot(document_id, revisions, leaves) do
      {:ok, winner} = Winner.select(leaves)
      live = Winner.live_leaves(leaves)

      %{
        document_id: document_id,
        tree: encode_tree(revisions),
        leaf_set: encode_leaf_set(leaves),
        live_leaf_set: live |> Enum.map(& &1.revision_id) |> Enum.sort(),
        winner: winner.revision_id,
        winner_deleted: winner.deleted,
        conflicts: Winner.conflicts(leaves, winner),
        tombstones:
          revisions
          |> Enum.filter(& &1.deleted)
          |> Enum.map(& &1.revision_id)
          |> Enum.sort()
      }
    end

    defp collect_revisions(context, document_id, leaves) do
      leaves
      |> Enum.reduce(%{}, fn leaf, acc ->
        {:ok, ancestors} = Facts.list_ancestors(context, document_id, leaf.revision_id)

        [leaf | ancestors]
        |> Enum.reduce(acc, fn rev, map -> Map.put(map, rev.revision_id, rev) end)
      end)
      |> Map.values()
      |> Enum.sort_by(& &1.revision_id)
    end

    defp current_adapter_leaves(adapter, document_id) do
      context = @adapter.to_context(adapter)

      case Facts.list_leaves(context, document_id) do
        {:ok, leaves} -> leaves
        _ -> []
      end
    end

    defp encode_tree(revisions) do
      revisions
      |> Enum.map(fn rev ->
        %{
          revision_id: rev.revision_id,
          parent_revision: rev.parent_revision,
          deleted: rev.deleted,
          body: rev.body,
          generation: rev.generation
        }
      end)
      |> Enum.sort_by(& &1.revision_id)
    end

    defp encode_leaf_set(leaves) do
      leaves
      |> Enum.map(fn leaf ->
        %{revision: leaf.revision_id, history_id: leaf.history_id, deleted: leaf.deleted}
      end)
      |> Enum.sort_by(& &1.revision)
    end

    defp wire(document_id, revision_id, parent, deleted, body) do
      AdapterCase.wire_revision(document_id, revision_id, parent, deleted, body)
    end

    defp build_linear_chain(adapter, document_id, depth) when is_integer(depth) and depth > 0 do
      {revisions, leaf} =
        Enum.reduce(1..depth, {[], nil}, fn n, {revs, parent} ->
          body = %{"n" => n}

          mutation =
            if parent do
              %{operation: :put, document_id: document_id, if_revision: parent, body: body}
            else
              %{operation: :put, document_id: document_id, body: body}
            end

          {:ok, %{revision: revision}} = @adapter.apply_local_mutation(adapter, mutation)
          {[revision | revs], revision}
        end)

      {leaf, Enum.reverse(revisions)}
    end

    defp chain_via_find(context, document_id, leaf_id) do
      context
      |> chain_via_find_walk(document_id, leaf_id, [])
      |> Enum.map(& &1.revision_id)
    end

    defp chain_via_find_walk(_context, _document_id, nil, acc), do: acc

    defp chain_via_find_walk(context, document_id, revision_id, acc) do
      {:ok, revision} = Facts.find_revision(context, document_id, revision_id)
      chain_via_find_walk(context, document_id, revision.parent_revision, [revision | acc])
    end

    defp import_order_permutations do
      # Four import units: root alone, then each leaf chain. 4! = 24 orders.
      [:root, :left, :right, :tombstone]
      |> permutations()
    end

    defp permutations([]), do: [[]]

    defp permutations(list) do
      for head <- list, tail <- permutations(list -- [head]), do: [head | tail]
    end

    defp order_independence_fixture(document_id) do
      root_body = %{"role" => "root"}
      left_body = %{"role" => "left"}
      right_body = %{"role" => "right"}

      history_id = RevisionFixtures.shared_history_id()

      {:ok, root} = Id.calculate(document_id, history_id, nil, false, root_body, %{})
      {:ok, left} = Id.calculate(document_id, history_id, root, false, left_body, %{})
      {:ok, right} = Id.calculate(document_id, history_id, root, false, right_body, %{})
      {:ok, tombstone} = Id.calculate(document_id, history_id, root, true, nil, %{})

      leaves = [
        %VialKeeper.Domain.Revision{
          document_id: document_id,
          history_id: history_id,
          revision_id: left,
          generation: 2,
          parent_revision: root,
          deleted: false,
          body: left_body,
          attachments: %{}
        },
        %VialKeeper.Domain.Revision{
          document_id: document_id,
          history_id: history_id,
          revision_id: right,
          generation: 2,
          parent_revision: root,
          deleted: false,
          body: right_body,
          attachments: %{}
        },
        %VialKeeper.Domain.Revision{
          document_id: document_id,
          history_id: history_id,
          revision_id: tombstone,
          generation: 2,
          parent_revision: root,
          deleted: true,
          body: nil,
          attachments: %{}
        }
      ]

      {:ok, winner} = Winner.select(leaves)

      %{
        root: root,
        left: left,
        right: right,
        tombstone: tombstone,
        expected_winner: winner.revision_id,
        chains: %{
          root: %{
            document_id: document_id,
            leaf_revision: root,
            revisions: [wire(document_id, root, nil, false, root_body)]
          },
          left: %{
            document_id: document_id,
            leaf_revision: left,
            revisions: [
              wire(document_id, root, nil, false, root_body),
              wire(document_id, left, root, false, left_body)
            ]
          },
          right: %{
            document_id: document_id,
            leaf_revision: right,
            revisions: [
              wire(document_id, root, nil, false, root_body),
              wire(document_id, right, root, false, right_body)
            ]
          },
          tombstone: %{
            document_id: document_id,
            leaf_revision: tombstone,
            revisions: [
              wire(document_id, root, nil, false, root_body),
              wire(document_id, tombstone, root, true, nil)
            ]
          }
        }
      }
    end
  end
end
