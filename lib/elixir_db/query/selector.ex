defmodule ElixirDB.Query.Selector do
  @moduledoc "The bounded storage-neutral selector subset."

  alias ElixirDB.JSON.Pointer

  @spec matches?(map(), map()) :: {:ok, boolean()} | {:error, ElixirDB.Error.t()}
  def matches?(document, selector) when is_map(document) and is_map(selector) do
    Enum.reduce_while(selector, {:ok, true}, fn
      {"$and", clauses}, {:ok, true} when is_list(clauses) and clauses != [] ->
        case Enum.reduce_while(clauses, {:ok, true}, fn
               clause, {:ok, true} ->
                 case matches?(document, clause) do
                   {:ok, true} -> {:cont, {:ok, true}}
                   {:ok, false} -> {:halt, {:ok, false}}
                   {:error, _} = error -> {:halt, error}
                 end

               _clause, {:ok, false} = acc ->
                 {:halt, acc}

               _clause, {:error, _} = error ->
                 {:halt, error}
             end) do
          {:ok, value} -> {:cont, {:ok, value}}
          {:error, _} = error -> {:halt, error}
        end

      {"$and", _}, _ ->
        {:halt, {:error, ElixirDB.Error.invalid_request("$and must contain a non-empty array")}}

      {path, condition}, {:ok, true} when is_binary(path) ->
        case match_field(document, path, condition) do
          {:ok, value} -> {:cont, {:ok, value}}
          {:error, _} = error -> {:halt, error}
        end

      {_path, _condition}, _ ->
        {:cont, {:ok, false}}
    end)
  end

  def matches?(_, _), do: {:error, ElixirDB.Error.invalid_request("selector must be an object")}

  defp match_field(document, path, condition) do
    with {:ok, _tokens} <- Pointer.parse(path), false <- path == "" do
      value = Pointer.get(document, path)
      evaluate(value, condition)
    else
      true -> {:error, ElixirDB.Error.invalid_request("selector paths must not be empty")}
      {:error, _} = error -> error
    end
  end

  defp evaluate(:missing, %{"$exists" => expected}) when is_boolean(expected),
    do: {:ok, not expected}

  defp evaluate(:missing, %{"$exists" => _}),
    do: {:error, ElixirDB.Error.invalid_request("$exists requires a boolean")}

  defp evaluate(:missing, _), do: {:ok, false}

  defp evaluate({:ok, value}, condition) when is_map(condition),
    do: evaluate_operators(value, condition)

  defp evaluate({:ok, value}, condition), do: {:ok, value == condition}
  defp evaluate(value, condition), do: evaluate({:ok, value}, condition)

  defp evaluate_operators(value, operators) do
    Enum.reduce_while(operators, {:ok, true}, fn
      {"$eq", expected}, {:ok, true} ->
        continue_compare(value, expected, &(&1 == &2))

      {"$gt", expected}, {:ok, true} ->
        continue_compare(value, expected, &(&1 > &2))

      {"$gte", expected}, {:ok, true} ->
        continue_compare(value, expected, &(&1 >= &2))

      {"$lt", expected}, {:ok, true} ->
        continue_compare(value, expected, &(&1 < &2))

      {"$lte", expected}, {:ok, true} ->
        continue_compare(value, expected, &(&1 <= &2))

      {"$in", expected}, {:ok, true} when is_list(expected) and expected != [] ->
        {:cont, {:ok, Enum.any?(expected, &(same_type?(&1, value) and &1 == value))}}

      {"$in", _}, _ ->
        {:halt, {:error, ElixirDB.Error.invalid_request("$in requires a non-empty array")}}

      {"$exists", expected}, {:ok, true} when is_boolean(expected) ->
        {:cont, {:ok, expected}}

      {"$exists", _}, _ ->
        {:halt, {:error, ElixirDB.Error.invalid_request("$exists requires a boolean")}}

      {_operator, _value}, _ ->
        {:halt, {:error, ElixirDB.Error.invalid_request("unsupported selector operator")}}
    end)
  end

  defp continue_compare(value, expected, comparator) do
    if same_type?(value, expected),
      do: {:cont, {:ok, comparator.(value, expected)}},
      else: {:cont, {:ok, false}}
  end

  defp same_type?(a, b) when is_number(a) and is_number(b), do: true
  defp same_type?(a, b) when is_binary(a) and is_binary(b), do: true
  defp same_type?(a, b) when is_boolean(a) and is_boolean(b), do: true
  defp same_type?(nil, nil), do: true
  defp same_type?(_, _), do: false
end
