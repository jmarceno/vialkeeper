defmodule VialKeeper.View.Program do
  @moduledoc "Shared pure evaluator for local and materialized view map programs."

  alias VialKeeper.Query.Selector
  alias VialKeeper.View.Expression

  @type row :: %{
          required(:document_id) => binary(),
          required(:revision_id) => binary(),
          required(:key) => [term()],
          optional(:value) => term()
        }

  @spec map(map(), binary(), binary(), map()) ::
          {:ok, row() | :remove} | {:error, VialKeeper.Error.t()}
  def map(definition, document_id, revision_id, body) when is_map(body) do
    case Selector.matches?(body, Map.fetch!(definition, :predicate)) do
      {:ok, true} -> evaluate_row(definition, document_id, revision_id, body)
      {:ok, false} -> {:ok, :remove}
      {:error, _} = error -> error
    end
  end

  def map(_definition, _document_id, _revision_id, _body),
    do: {:error, VialKeeper.Error.invalid_request("document body must be an object")}

  defp evaluate_row(definition, document_id, revision_id, body) do
    with {:ok, key} <- evaluate_key(Map.fetch!(definition, :key), body),
         :ok <- ensure_key(key),
         {:ok, value} <- evaluate_value(definition, body) do
      row = %{document_id: document_id, revision_id: revision_id, key: key}
      row = if value == :omit, do: row, else: Map.put(row, :value, value)
      {:ok, row}
    else
      {:error, _} = error -> error
      :skip -> {:ok, :remove}
    end
  end

  defp evaluate_key(expressions, body) do
    Enum.reduce_while(expressions, {:ok, []}, fn expression, {:ok, acc} ->
      case Expression.evaluate(expression, body) do
        {:ok, component} -> {:cont, {:ok, [component | acc]}}
        :missing -> {:halt, :skip}
        {:error, _} = error -> {:halt, error}
      end
    end)
    |> then(fn
      {:ok, key} -> {:ok, Enum.reverse(key)}
      :skip -> :skip
      error -> error
    end)
  end

  defp ensure_key(key) do
    if Enum.all?(key, &valid_key_component?/1),
      do: :ok,
      else: :skip
  end

  defp valid_key_component?(value) when is_nil(value) or is_boolean(value), do: true
  defp valid_key_component?(value) when is_number(value) or is_binary(value), do: true
  defp valid_key_component?(_), do: false

  defp evaluate_value(%{reducer: reducer, value: nil}, _body) when reducer in [nil, :_count],
    do: {:ok, :omit}

  defp evaluate_value(%{value: nil}, _body), do: {:ok, :omit}

  defp evaluate_value(%{value: expression}, body) do
    case Expression.evaluate(expression, body) do
      {:ok, value} -> {:ok, value}
      :missing -> {:ok, :omit}
      {:error, _} = error -> error
    end
  end
end
