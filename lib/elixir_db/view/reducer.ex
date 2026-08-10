defmodule ElixirDB.View.Reducer do
  @moduledoc "Fixed map/reduce reducers and grouped aggregation for declarative views."

  alias ElixirDB.View.{KeyCodec, Number, NumericAccumulator}

  @reducers ~w(_count _sum _min _max _stats)

  @type reducer :: nil | :_count | :_sum | :_min | :_max | :_stats
  @type row :: %{
          required(:key) => KeyCodec.key(),
          optional(:value) => term(),
          optional(:id) => binary()
        }

  @spec known_reducers() :: [binary()]
  def known_reducers, do: @reducers

  @spec normalize_reducer(term()) :: {:ok, reducer()} | {:error, ElixirDB.Error.t()}
  def normalize_reducer(nil), do: {:ok, nil}
  def normalize_reducer("_count"), do: {:ok, :_count}
  def normalize_reducer("_sum"), do: {:ok, :_sum}
  def normalize_reducer("_min"), do: {:ok, :_min}
  def normalize_reducer("_max"), do: {:ok, :_max}
  def normalize_reducer("_stats"), do: {:ok, :_stats}
  def normalize_reducer(:_count), do: {:ok, :_count}
  def normalize_reducer(:_sum), do: {:ok, :_sum}
  def normalize_reducer(:_min), do: {:ok, :_min}
  def normalize_reducer(:_max), do: {:ok, :_max}
  def normalize_reducer(:_stats), do: {:ok, :_stats}

  def normalize_reducer(_),
    do: {:error, ElixirDB.Error.invalid_request("view reducer is invalid")}

  @spec reduce_rows([row()], reducer()) :: {:ok, [map()]} | {:error, ElixirDB.Error.t()}
  def reduce_rows([], _reducer), do: {:ok, []}

  def reduce_rows(rows, nil) do
    Enum.reduce_while(rows, {:ok, []}, fn row, {:ok, acc} ->
      case row_result(row) do
        {:ok, result} -> {:cont, {:ok, [result | acc]}}
        {:error, _} = error -> {:halt, error}
      end
    end)
    |> case do
      {:ok, results} -> {:ok, Enum.sort_by(results, &map_row_sort_key/1)}
      error -> error
    end
  end

  def reduce_rows(rows, reducer) do
    with {:ok, grouped} <- group_rows(rows, & &1.key) do
      aggregate_grouped_map(grouped, reducer)
    end
  end

  @spec reduce_grouped([row()], reducer(), non_neg_integer(), non_neg_integer()) ::
          {:ok, [map()]} | {:error, ElixirDB.Error.t()}
  def reduce_grouped(rows, reducer, group_level, key_length)
      when is_integer(group_level) and group_level >= 0 do
    if group_level > key_length do
      {:error, ElixirDB.Error.invalid_request("group_level exceeds key length")}
    else
      with {:ok, grouped} <- group_rows(rows, &Enum.take(&1.key, group_level)) do
        aggregate_grouped_map(grouped, reducer)
      end
    end
  end

  @spec fold_values([term()], reducer()) :: {:ok, term()} | {:error, ElixirDB.Error.t()}
  def fold_values(values, reducer) do
    case reducer do
      nil -> {:ok, values}
      :_count -> {:ok, length(values)}
      :_sum -> fold_sum(values)
      :_min -> fold_min(values)
      :_max -> fold_max(values)
      :_stats -> fold_stats(values)
    end
  end

  defp aggregate_grouped_map(grouped, reducer) do
    grouped
    |> Enum.reduce_while({:ok, []}, fn {_group_sort, {group_key, group_rows}}, {:ok, acc} ->
      case aggregate_group(group_key, group_rows, reducer) do
        {:ok, result} -> {:cont, {:ok, [result | acc]}}
        {:error, _} = error -> {:halt, error}
      end
    end)
    |> case do
      {:ok, results} -> {:ok, Enum.sort_by(results, & &1.key_sort)}
      error -> error
    end
  end

  defp group_rows(rows, key_fun) do
    Enum.reduce_while(rows, {:ok, %{}}, fn row, {:ok, groups} ->
      group_key = key_fun.(row)

      case KeyCodec.encode(group_key) do
        {:ok, group_sort} ->
          group_rows = Map.get(groups, group_sort, {group_key, []})
          {existing_key, existing_rows} = group_rows
          groups = Map.put(groups, group_sort, {existing_key, [row | existing_rows]})
          {:cont, {:ok, groups}}

        {:error, _} = error ->
          {:halt, error}
      end
    end)
  end

  defp aggregate_group(key, rows, reducer) do
    rows = Enum.reverse(rows)
    values = Enum.map(rows, &Map.get(&1, :value))

    with {:ok, encoded} <- KeyCodec.encode(List.wrap(key)),
         {:ok, value} <- fold_values(values, reducer) do
      {:ok,
       %{
         key: List.wrap(key),
         key_sort: encoded,
         ids: Enum.map(rows, &Map.get(&1, :id)),
         value: value
       }}
    end
  end

  defp row_result(%{key: key, value: value} = row) do
    with {:ok, key_sort} <- row_key_sort(row, key) do
      {:ok,
       %{
         id: Map.get(row, :id),
         key: key,
         key_sort: key_sort,
         value: value
       }}
    end
  end

  defp row_key_sort(row, key) do
    case Map.get(row, :key_sort) do
      encoded when is_binary(encoded) -> {:ok, encoded}
      _ -> KeyCodec.encode(key)
    end
  end

  defp map_row_sort_key(%{key_sort: key_sort, id: id}), do: {key_sort, id || ""}
  defp map_row_sort_key(%{key_sort: key_sort}), do: {key_sort, ""}

  defp fold_sum(values) do
    with {:ok, acc} <- accumulate_values(values) do
      NumericAccumulator.sum(acc)
    end
  end

  defp fold_min(values) do
    with {:ok, floats} <- binary64_values(values) do
      case floats do
        [] -> {:ok, nil}
        [first | rest] -> {:ok, Enum.min([first | rest])}
      end
    end
  end

  defp fold_max(values) do
    with {:ok, floats} <- binary64_values(values) do
      case floats do
        [] -> {:ok, nil}
        [first | rest] -> {:ok, Enum.max([first | rest])}
      end
    end
  end

  defp fold_stats(values) do
    with {:ok, acc} <- accumulate_values(values),
         {:ok, floats} <- binary64_values(values),
         {:ok, sum} <- NumericAccumulator.sum(acc),
         {:ok, sumsqr} <- NumericAccumulator.sumsqr(acc),
         {:ok, min} <- fold_min(values),
         {:ok, max} <- fold_max(values) do
      {:ok, %{count: length(floats), sum: sum, min: min, max: max, sumsqr: sumsqr}}
    end
  end

  defp accumulate_values(values) do
    Enum.reduce_while(values, {:ok, NumericAccumulator.new()}, &accumulate_value/2)
  end

  defp accumulate_value(nil, {:ok, acc}), do: {:cont, {:ok, acc}}

  defp accumulate_value(number, {:ok, acc}) when is_number(number) do
    case NumericAccumulator.add(acc, number) do
      {:ok, next} -> {:cont, {:ok, next}}
      {:error, _} = error -> {:halt, error}
    end
  end

  defp accumulate_value(_other, {:ok, acc}), do: {:cont, {:ok, acc}}

  defp binary64_values(values) do
    case Enum.reduce_while(values, {:ok, []}, &collect_binary64/2) do
      {:ok, floats} -> {:ok, Enum.reverse(floats)}
      error -> error
    end
  end

  defp collect_binary64(nil, {:ok, acc}), do: {:cont, {:ok, acc}}

  defp collect_binary64(number, {:ok, acc}) when is_number(number) do
    case Number.to_binary64(number) do
      {:ok, float} ->
        {:cont, {:ok, [float | acc]}}

      :overflow ->
        {:halt, {:error, ElixirDB.Error.resource_limit("numeric value is not representable")}}
    end
  end

  defp collect_binary64(_other, {:ok, acc}), do: {:cont, {:ok, acc}}
end
