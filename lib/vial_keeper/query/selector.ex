defmodule VialKeeper.Query.Selector do
  @moduledoc "The storage-neutral evaluator for canonical query predicates."

  alias VialKeeper.JSON.Pointer
  alias VialKeeper.Query.Predicate

  @safe_integer 9_007_199_254_740_991

  @type compiled ::
          :match_all
          | {:and, [compiled()]}
          | {:or, [compiled()]}
          | {:not, compiled()}
          | {:compiled_field, [binary()], [Predicate.field_predicate() | {:elem_match, compiled()}]}

  @spec matches?(map(), Predicate.t() | compiled()) ::
          {:ok, boolean()} | {:error, VialKeeper.Error.t()}
  def matches?(document, predicate) when is_map(document) do
    evaluate(document, predicate)
  end

  def matches?(_, _),
    do: {:error, VialKeeper.Error.invalid_request("selector predicate is invalid")}

  @doc "Parses each selector JSON Pointer once so evaluation can reuse tokens."
  @spec compile(Predicate.t() | compiled()) ::
          {:ok, compiled()} | {:error, VialKeeper.Error.t()}
  def compile(:match_all), do: {:ok, :match_all}

  def compile({:and, children}) when is_list(children), do: compile_children(:and, children)

  def compile({:or, children}) when is_list(children), do: compile_children(:or, children)

  def compile({:not, child}) do
    with {:ok, compiled} <- compile(child), do: {:ok, {:not, compiled}}
  end

  def compile({:compiled_field, tokens, predicates})
      when is_list(tokens) and is_list(predicates),
      do: {:ok, {:compiled_field, tokens, predicates}}

  def compile({:field, path, predicates}) when is_binary(path) and is_list(predicates) do
    case Pointer.parse(path) do
      {:ok, [_ | _] = tokens} ->
        with {:ok, predicates} <- compile_field_predicates(predicates) do
          {:ok, {:compiled_field, tokens, predicates}}
        end

      {:ok, []} ->
        {:error, VialKeeper.Error.invalid_request("selector path is invalid")}

      {:error, _} = error ->
        error
    end
  end

  def compile(_),
    do: {:error, VialKeeper.Error.invalid_request("selector predicate is invalid")}

  defp compile_children(operator, children) do
    Enum.reduce_while(children, {:ok, []}, fn child, {:ok, acc} ->
      case compile(child) do
        {:ok, compiled} -> {:cont, {:ok, [compiled | acc]}}
        {:error, _} = error -> {:halt, error}
      end
    end)
    |> case do
      {:ok, compiled} -> {:ok, {operator, Enum.reverse(compiled)}}
      error -> error
    end
  end

  defp compile_field_predicates(predicates) do
    Enum.reduce_while(predicates, {:ok, []}, fn
      {:elem_match, inner}, {:ok, acc} ->
        case compile(inner) do
          {:ok, compiled} -> {:cont, {:ok, [{:elem_match, compiled} | acc]}}
          {:error, _} = error -> {:halt, error}
        end

      predicate, {:ok, acc} ->
        {:cont, {:ok, [predicate | acc]}}
    end)
    |> case do
      {:ok, compiled} -> {:ok, Enum.reverse(compiled)}
      error -> error
    end
  end

  defp evaluate(_document, :match_all), do: {:ok, true}

  defp evaluate(document, {:and, predicates}),
    do: evaluate_all(document, predicates)

  defp evaluate(document, {:or, predicates}),
    do: evaluate_any(document, predicates)

  defp evaluate(document, {:not, predicate}) do
    case evaluate(document, predicate) do
      {:ok, value} -> {:ok, not value}
      {:error, _} = error -> error
    end
  end

  defp evaluate(document, {:compiled_field, tokens, predicates})
       when is_list(tokens) and tokens != [] do
    evaluate_field_predicates(Pointer.get_tokens(document, tokens), predicates)
  end

  defp evaluate(document, {:field, path, predicates}) when is_binary(path) do
    case compile({:field, path, predicates}) do
      {:ok, compiled} -> evaluate(document, compiled)
      {:error, _} = error -> error
    end
  end

  defp evaluate(_document, _predicate),
    do: {:error, VialKeeper.Error.invalid_request("selector predicate is invalid")}

  defp evaluate_all(_document, []), do: {:ok, true}

  defp evaluate_all(document, [predicate | rest]) do
    Enum.reduce_while([predicate | rest], {:ok, true}, fn child, {:ok, result} ->
      case evaluate(document, child) do
        {:ok, value} -> {:cont, {:ok, result and value}}
        {:error, _} = error -> {:halt, error}
      end
    end)
  end

  defp evaluate_any(_document, []), do: {:ok, false}

  defp evaluate_any(document, [predicate | rest]) do
    Enum.reduce_while([predicate | rest], {:ok, false}, fn child, {:ok, result} ->
      case evaluate(document, child) do
        {:ok, value} -> {:cont, {:ok, result or value}}
        {:error, _} = error -> {:halt, error}
      end
    end)
  end

  defp evaluate_field_predicates(value, predicates) do
    Enum.reduce_while(predicates, {:ok, true}, fn predicate, {:ok, result} ->
      case evaluate_field_predicate(value, predicate) do
        {:ok, predicate_result} -> {:cont, {:ok, result and predicate_result}}
        {:error, _} = error -> {:halt, error}
      end
    end)
  end

  defp evaluate_field_predicate(:missing, {:exists, expected}), do: {:ok, not expected}
  defp evaluate_field_predicate(:missing, _predicate), do: {:ok, false}

  defp evaluate_field_predicate({:ok, value}, {:eq, expected}),
    do: {:ok, Predicate.exact_equal?(value, expected)}

  defp evaluate_field_predicate({:ok, value}, {:ne, expected}),
    do: {:ok, not Predicate.exact_equal?(value, expected)}

  defp evaluate_field_predicate({:ok, value}, {operator, expected})
       when operator in [:gt, :gte, :lt, :lte] do
    case Predicate.ordered_compare(value, expected) do
      :incomparable -> {:ok, false}
      comparison -> {:ok, comparison_satisfies?(comparison, operator)}
    end
  end

  defp evaluate_field_predicate({:ok, value}, {:in, expected}),
    do: {:ok, Enum.any?(expected, &Predicate.exact_equal?(value, &1))}

  defp evaluate_field_predicate({:ok, value}, {:nin, expected}),
    do:
      {:ok,
       Predicate.scalar?(value) and Enum.all?(expected, &(not Predicate.exact_equal?(value, &1)))}

  defp evaluate_field_predicate({:ok, _value}, {:exists, expected}), do: {:ok, expected}

  defp evaluate_field_predicate({:ok, value}, {:type, expected}),
    do: {:ok, Predicate.json_type(value) == expected}

  defp evaluate_field_predicate({:ok, value}, {:begins_with, prefix}) when is_binary(value),
    do: {:ok, String.starts_with?(value, prefix)}

  defp evaluate_field_predicate({:ok, _value}, {:begins_with, _prefix}), do: {:ok, false}

  defp evaluate_field_predicate({:ok, value}, {:regex, regex}) when is_binary(value),
    do: VialKeeper.Query.Regex.match?(regex, value)

  defp evaluate_field_predicate({:ok, _value}, {:regex, _regex}), do: {:ok, false}

  defp evaluate_field_predicate({:ok, value}, {:all, expected}) when is_list(value) do
    {:ok,
     Enum.all?(expected, fn operand -> Enum.any?(value, &Predicate.exact_equal?(&1, operand)) end)}
  end

  defp evaluate_field_predicate({:ok, _value}, {:all, _expected}), do: {:ok, false}

  defp evaluate_field_predicate({:ok, values}, {:elem_match, predicate}) when is_list(values) do
    Enum.reduce_while(values, {:ok, false}, fn
      value, {:ok, false} when is_map(value) ->
        case evaluate(value, predicate) do
          {:ok, true} -> {:halt, {:ok, true}}
          {:ok, false} -> {:cont, {:ok, false}}
          {:error, _} = error -> {:halt, error}
        end

      _value, {:ok, false} ->
        {:cont, {:ok, false}}

      _value, {:ok, true} = result ->
        {:halt, result}
    end)
  end

  defp evaluate_field_predicate({:ok, _value}, {:elem_match, _predicate}), do: {:ok, false}

  defp evaluate_field_predicate({:ok, value}, {:size, expected}) when is_list(value),
    do: {:ok, length(value) == expected}

  defp evaluate_field_predicate({:ok, _value}, {:size, _expected}), do: {:ok, false}

  defp evaluate_field_predicate({:ok, _value}, {:mod, 0, _remainder}),
    do: {:error, VialKeeper.Error.invalid_request("$mod divisor must not be zero")}

  defp evaluate_field_predicate({:ok, value}, {:mod, divisor, remainder}) do
    case exact_integer(value) do
      {:ok, integer} -> {:ok, rem(integer, divisor) == remainder}
      :error -> {:ok, false}
    end
  end

  defp comparison_satisfies?(:lt, :gt), do: false
  defp comparison_satisfies?(:lt, :gte), do: false
  defp comparison_satisfies?(:lt, :lt), do: true
  defp comparison_satisfies?(:lt, :lte), do: true
  defp comparison_satisfies?(:eq, :gt), do: false
  defp comparison_satisfies?(:eq, :gte), do: true
  defp comparison_satisfies?(:eq, :lt), do: false
  defp comparison_satisfies?(:eq, :lte), do: true
  defp comparison_satisfies?(:gt, :gt), do: true
  defp comparison_satisfies?(:gt, :gte), do: true
  defp comparison_satisfies?(:gt, :lt), do: false
  defp comparison_satisfies?(:gt, :lte), do: false

  defp exact_integer(value) when is_integer(value) and abs(value) <= @safe_integer,
    do: {:ok, value}

  defp exact_integer(value) when is_float(value) do
    integer = trunc(value)

    if value == integer and abs(value) <= @safe_integer,
      do: {:ok, integer},
      else: :error
  end

  defp exact_integer(_value), do: :error
end
