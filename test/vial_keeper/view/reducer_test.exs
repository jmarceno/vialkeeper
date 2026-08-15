defmodule VialKeeper.View.ReducerTest do
  @moduledoc "Aggregation behavior tests for declarative view reducers."
  use ExUnit.Case, async: true

  alias VialKeeper.View.Reducer

  test "count reducer aggregates grouped rows" do
    rows = [
      %{key: ["a"], value: 1, id: "1"},
      %{key: ["a"], value: 2, id: "2"},
      %{key: ["b"], value: 3, id: "3"}
    ]

    assert {:ok, results} = Reducer.reduce_grouped(rows, :_count, 1, 1)
    values = Map.new(results, fn row -> {row.key, row.value} end)
    assert values[["a"]] == 2
    assert values[["b"]] == 1
  end

  test "numeric representations of one key share a reduced group" do
    rows = [
      %{key: [1], value: 1, id: "integer"},
      %{key: [1.0], value: 2, id: "float"}
    ]

    assert {:ok, [%{key: [1], value: 2}]} = Reducer.reduce_grouped(rows, :_count, 1, 1)
  end

  test "empty reduced query returns no synthetic zero group" do
    assert {:ok, []} = Reducer.reduce_grouped([], :_count, 1, 1)
  end

  test "stats reducer returns expected shape" do
    assert {:ok, stats} = Reducer.fold_values([1, 2], :_stats)
    assert stats.count == 2
    assert stats.sum == 3.0
    assert stats.min == 1
    assert stats.max == 2
    assert stats.sumsqr == 5.0
  end

  test "incremental add/remove permutations match full rebuild for sum" do
    values = [0.125, -3.5, 42.0, 1.0e10, -1.0e-20]

    rebuild = sum_with_updates(values, values)

    for add_order <- permutations(values),
        remove_order <- permutations(values) do
      incremental =
        sum_with_updates(add_order, remove_order)

      assert incremental == rebuild
    end
  end

  test "group_level 0 aggregates into a single empty key" do
    rows = [
      %{key: ["a"], value: 1, id: "1"},
      %{key: ["b"], value: 2, id: "2"}
    ]

    assert {:ok, [result]} = Reducer.reduce_grouped(rows, :_count, 0, 1)
    assert result.key == []
    assert result.value == 2
  end

  test "group_level greater than key length is rejected" do
    rows = [%{key: ["a"], value: 1, id: "1"}]

    assert {:error, %VialKeeper.Error{code: :invalid_request}} =
             Reducer.reduce_grouped(rows, :_count, 2, 1)
  end

  test "map-only rows are sorted by key_sort and document id" do
    rows = [
      %{key: ["b"], value: 2, id: "2"},
      %{key: ["a"], value: 1, id: "1"},
      %{key: ["a"], value: 3, id: "0"}
    ]

    assert {:ok, [%{key: ["a"], id: "0"}, %{key: ["a"], id: "1"}, %{key: ["b"], id: "2"}]} =
             Reducer.reduce_rows(rows, nil)
  end

  test "min and max ignore non-numeric values" do
    assert {:ok, nil} = Reducer.fold_values([:nan, :infinity], :_min)
    assert {:ok, nil} = Reducer.fold_values([:nan, :infinity], :_max)
    assert {:ok, -1.0} = Reducer.fold_values([:nan, -1, 2], :_min)
    assert {:ok, 2.0} = Reducer.fold_values([:infinity, 1, 2], :_max)
  end

  test "oversized integer values return resource_limit without raising" do
    assert {:error, %VialKeeper.Error{code: :resource_limit}} =
             Reducer.fold_values([10 ** 400], :_sum)
  end

  test "map-only rows propagate key codec errors" do
    assert {:error, %VialKeeper.Error{code: :invalid_request}} =
             Reducer.reduce_rows([%{key: [10 ** 400], value: 1, id: "1"}], nil)
  end

  defp permutations([]), do: [[]]

  defp permutations([head | tail]) do
    for perm <- permutations(tail),
        index <- 0..length(perm) do
      List.insert_at(perm, index, head)
    end
  end

  defp sum_with_updates(adds, removals) do
    alias VialKeeper.View.NumericAccumulator

    {:ok, acc} =
      Enum.reduce_while(adds, {:ok, NumericAccumulator.new()}, fn value, {:ok, acc} ->
        case NumericAccumulator.add(acc, value) do
          {:ok, next} -> {:cont, {:ok, next}}
          error -> {:halt, error}
        end
      end)

    {:ok, acc} =
      Enum.reduce_while(removals, {:ok, acc}, fn value, {:ok, acc} ->
        case NumericAccumulator.remove(acc, value) do
          {:ok, next} -> {:cont, {:ok, next}}
          error -> {:halt, error}
        end
      end)

    {:ok, sum} = NumericAccumulator.sum(acc)
    sum
  end
end
