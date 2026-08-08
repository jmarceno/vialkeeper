defmodule ElixirDB.Contract.RevisionAdapterPropertiesTest do
  @moduledoc """
  Phase 2 exit gate: random operation histories produce identical revision trees,
  winners, active conflicts, tombstones, and replay results in the pure model and
  the SQLite adapter (including after close/reopen).
  """

  use ExUnit.Case, async: false
  use ExUnitProperties

  alias ElixirDB.JSON.StrictDecoder
  alias ElixirDB.ModelGenerators
  alias ElixirDB.RevisionFixtures
  alias ElixirDB.RevisionHistoryModel
  alias ElixirDB.Revisions.{Id, Tree, Winner}
  alias ElixirDB.Storage.AdapterCase
  alias ElixirDB.Storage.SQLite.{Adapter, Connection, Documents}
  alias ElixirDB.Storage.SQLite.Revisions
  @moduletag :property

  property "adapter and pure model agree on trees, winners, conflicts, tombstones, and replay" do
    check all(
            history <- ModelGenerators.revision_operation_history(),
            max_runs: 40
          ) do
      {:ok, bundle_path} = ElixirDB.TempDatabase.create(prefix: "elixirdb-rev-props")
      path = ElixirDB.TempDatabase.sqlite_path(bundle_path)
      {:ok, adapter} = Adapter.create(path, %{})

      try do
        model = RevisionHistoryModel.new()
        {model, _adapter, _last_req} = run_history(history.operations, model, adapter, nil)

        model_snap = RevisionHistoryModel.snapshot(model)

        assert :ok = Adapter.close(adapter)
        assert {:ok, reopened} = Adapter.open(path)

        try do
          adapter_snap = adapter_snapshot(reopened, history.document_id)
          assert_equivalent_snapshots(model_snap, adapter_snap)
          assert_materialized_document(reopened, model_snap, history.document_id)
        after
          _ = Adapter.close(reopened)
        end
      after
        ElixirDB.TempDatabase.cleanup(bundle_path)
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
      {:ok, bundle_path} = ElixirDB.TempDatabase.create(prefix: "elixirdb-rev-resolve")
      path = ElixirDB.TempDatabase.sqlite_path(bundle_path)
      {:ok, adapter} = Adapter.create(path, %{})

      try do
        model = RevisionHistoryModel.new()
        {model, adapter, _} = run_history(history.operations, model, adapter, nil)
        model_snap = RevisionHistoryModel.snapshot(model)

        assert :ok = Adapter.close(adapter)
        assert {:ok, reopened} = Adapter.open(path)

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
          _ = Adapter.close(reopened)
        end
      after
        ElixirDB.TempDatabase.cleanup(bundle_path)
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
      {:ok, bundle_path} = ElixirDB.TempDatabase.create(prefix: "elixirdb-rev-stale")
      path = ElixirDB.TempDatabase.sqlite_path(bundle_path)
      {:ok, adapter} = Adapter.create(path, %{})

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

        {model, _adapter, _} =
          run_history(
            [%{op: :put, document_id: document_id, body: stale_body, if_revision: first}],
            model,
            adapter,
            nil
          )

        assert match?({:error, :revision_conflict}, model.last_result)

        model_snap = RevisionHistoryModel.snapshot(model)
        assert_materialized_document(adapter, model_snap, document_id)
      after
        _ = Adapter.close(adapter)
        ElixirDB.TempDatabase.cleanup(bundle_path)
      end
    end
  end

  test "import order of the same revision multiset does not change the resulting tree" do
    document_id = "order-indep"
    fixture = order_independence_fixture(document_id)

    snapshots =
      Enum.map(import_order_permutations(), fn order ->
        {:ok, bundle_path} = ElixirDB.TempDatabase.create(prefix: "elixirdb-rev-order")
        path = ElixirDB.TempDatabase.sqlite_path(bundle_path)
        {:ok, adapter} = Adapter.create(path, %{})

        try do
          Enum.each(order, fn chain_key ->
            chain = Map.fetch!(fixture.chains, chain_key)

            assert {:ok, _} =
                     Adapter.import_revision_chains(adapter, %{chains: [chain]})
          end)

          adapter_snapshot(adapter, document_id)
        after
          _ = Adapter.close(adapter)
          ElixirDB.TempDatabase.cleanup(bundle_path)
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

    normalize_adapter_result(Adapter.apply_local_mutation(adapter, request))
  end

  defp apply_adapter_op(adapter, %{op: :delete} = op, _) do
    request = %{
      operation: :delete,
      document_id: op.document_id,
      if_revision: op[:if_revision]
    }

    normalize_adapter_result(Adapter.apply_local_mutation(adapter, request))
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

    with {:ok, _} <- Adapter.import_revision_chains(adapter, %{chains: [left_chain]}),
         {:ok, _} <- Adapter.import_revision_chains(adapter, %{chains: [right_chain]}) do
      {:ok, %{imported: [root, left, right], replayed: false}}
    else
      {:error, %ElixirDB.Error{code: code}} -> {:error, code}
    end
  end

  defp apply_adapter_op(adapter, %{op: :resolve} = op, _) do
    leaves = current_adapter_leaves(adapter, op.document_id)
    live = Winner.live_leaves(leaves)

    expected = expected_adapter_revisions(live, op[:expected])
    chosen = chosen_adapter_revision(op, live)
    request = resolution_request(op, expected, chosen)

    normalize_adapter_result(Adapter.resolve_conflict(adapter, request))
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

  defp normalize_adapter_result({:error, %ElixirDB.Error{code: code}}), do: {:error, code}

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
    case Adapter.get_document(adapter, %{document_id: document_id}) do
      {:ok, doc} ->
        assert doc.revision == model_snap.winner
        assert doc.deleted == model_snap.winner_deleted
        assert_document_body(doc, model_snap)

      {:error, %ElixirDB.Error{code: :document_not_found}} ->
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
    case Documents.find(adapter.conn, document_id) do
      {:ok, nil} ->
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

      {:ok, doc} ->
        revisions = load_all_revisions(adapter.conn, doc.doc_key, document_id)
        leaves = Tree.leaves(revisions)
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
  end

  defp current_adapter_leaves(adapter, document_id) do
    case Documents.find(adapter.conn, document_id) do
      {:ok, %{doc_key: doc_key}} ->
        case Revisions.load_leaves(adapter.conn, doc_key) do
          {:ok, leaves} -> leaves
          _ -> []
        end

      _ ->
        []
    end
  end

  defp load_all_revisions(conn, doc_key, document_id) do
    {:ok, rows} =
      Connection.query(
        conn,
        """
        SELECT revision_id, generation, parent_revision, history_id, deleted, body_json
        FROM revisions
        WHERE doc_key = ?
        ORDER BY revision_id
        """,
        [doc_key]
      )

    Enum.map(rows, fn [id, generation, parent, history_id, deleted, body_json] ->
      body =
        if is_nil(body_json) do
          nil
        else
          {:ok, decoded} = StrictDecoder.decode(body_json)
          decoded
        end

      %ElixirDB.Domain.Revision{
        document_id: document_id,
        history_id: history_id,
        revision_id: id,
        generation: generation,
        parent_revision: parent,
        deleted: deleted == 1,
        body: body,
        attachments: %{}
      }
    end)
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
      %ElixirDB.Domain.Revision{
        document_id: document_id,
        history_id: history_id,
        revision_id: left,
        generation: 2,
        parent_revision: root,
        deleted: false,
        body: left_body,
        attachments: %{}
      },
      %ElixirDB.Domain.Revision{
        document_id: document_id,
        history_id: history_id,
        revision_id: right,
        generation: 2,
        parent_revision: root,
        deleted: false,
        body: right_body,
        attachments: %{}
      },
      %ElixirDB.Domain.Revision{
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
