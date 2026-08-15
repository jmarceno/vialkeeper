defmodule VialKeeper.View.NumericAccumulatorTest do
  @moduledoc "Numerical accuracy tests for view reducers."
  use ExUnit.Case, async: true

  alias VialKeeper.View.NumericAccumulator

  @float_cases [1.0, 0.1, -3.5, 5.0e-324]

  test "sum of a single float matches binary64" do
    for value <- @float_cases do
      assert {:ok, acc} = NumericAccumulator.add(NumericAccumulator.new(), value)
      assert {:ok, ^value} = NumericAccumulator.sum(acc)
    end
  end

  test "sum rounds an exact halfway value to the even binary64" do
    assert {:ok, acc} = NumericAccumulator.add(NumericAccumulator.new(), 1.0)
    assert {:ok, acc} = NumericAccumulator.add(acc, 2.0 ** -53)
    assert {:ok, 1.0} = NumericAccumulator.sum(acc)
  end

  test "sum carries into the next exponent after mantissa rounding" do
    acc = NumericAccumulator.new()
    assert {:ok, acc} = NumericAccumulator.add(acc, 1.0)
    assert {:ok, acc} = NumericAccumulator.add(acc, 1.0 - 2.0 ** -52)
    assert {:ok, acc} = NumericAccumulator.add(acc, 2.0 ** -53)
    assert {:ok, 2.0} = NumericAccumulator.sum(acc)
  end

  @sumsqr_cases [1.0, 0.1, -3.5]

  test "sumsqr of a single float matches binary64 square" do
    for value <- @sumsqr_cases do
      assert {:ok, acc} = NumericAccumulator.add(NumericAccumulator.new(), value)
      expected = value * value
      assert {:ok, result} = NumericAccumulator.sumsqr(acc)
      assert result == expected
    end
  end

  test "subnormal sumsqr below binary64 range returns resource_limit" do
    assert {:ok, acc} = NumericAccumulator.add(NumericAccumulator.new(), 5.0e-324)

    assert {:error, %VialKeeper.Error{code: :resource_limit}} = NumericAccumulator.sumsqr(acc)
  end

  test "overflow returns resource_limit" do
    huge = Float.max_finite()
    assert {:ok, acc} = NumericAccumulator.add(NumericAccumulator.new(), huge)
    assert {:ok, acc} = NumericAccumulator.add(acc, huge)

    assert {:error, %VialKeeper.Error{code: :resource_limit}} = NumericAccumulator.sum(acc)
    assert {:error, %VialKeeper.Error{code: :resource_limit}} = NumericAccumulator.sumsqr(acc)
  end

  test "oversized integer input returns resource_limit without raising" do
    assert {:error, %VialKeeper.Error{code: :resource_limit}} =
             NumericAccumulator.add(NumericAccumulator.new(), 10 ** 400)

    near_limit = Bitwise.bsl(1, 1024) - 1

    assert {:error, %VialKeeper.Error{code: :resource_limit}} =
             NumericAccumulator.add(NumericAccumulator.new(), near_limit)
  end

  @tag timeout: 120_000
  test "incremental add/remove permutations match full rebuild for sum" do
    values = [0.125, -3.5, 42.0, 1.0e10, -1.0e-20]

    rebuild = sum_with_updates(values, values)

    for add_order <- permutations(values),
        remove_order <- permutations(values) do
      incremental = sum_with_updates(add_order, remove_order)
      assert incremental == rebuild
    end
  end

  test "incremental add/remove permutations match full rebuild for stats sumsqr" do
    values = [0.125, -3.5, 42.0, 1.0, 0.1]

    rebuild = stats_with_updates(values, values)

    for add_order <- permutations(values),
        remove_order <- permutations(values) do
      incremental = stats_with_updates(add_order, remove_order)
      assert incremental == rebuild
    end
  end

  defp permutations([]), do: [[]]

  defp permutations([head | tail]) do
    for perm <- permutations(tail),
        index <- 0..length(perm) do
      List.insert_at(perm, index, head)
    end
  end

  defp sum_with_updates(adds, removals) do
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

  defp stats_with_updates(adds, removals) do
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

    {:ok, sumsqr} = NumericAccumulator.sumsqr(acc)
    sumsqr
  end
end
