defmodule VialKeeper.Storage.SQLite.QueryCompiler do
  @moduledoc "Validated hand-off from the storage-neutral query contract to SQLite."

  alias VialKeeper.JSON.Pointer
  alias VialKeeper.MapAccess
  alias VialKeeper.Query.PrefixBounds

  @doc """
  Compile a validated JSON Pointer into an SQLite `json_extract` / `json_type` path.

  Returns `{:error, %VialKeeper.Error{}}` for an internally-invalid pointer rather than
  raising `MatchError`. HTTP pointers are pre-validated by the `Normalizer`, so an error
  here signals storage-layer misuse, not client input.
  """
  @spec sqlite_path(binary()) :: {:ok, binary()} | {:error, VialKeeper.Error.t()}
  def sqlite_path(path) when is_binary(path) do
    case Pointer.parse(path) do
      {:ok, tokens} ->
        compiled = "$" <> Enum.map_join(tokens, &sqlite_path_token/1)

        {:ok, compiled}

      {:error, error} ->
        {:error, error}
    end
  end

  defp sqlite_path_token(token), do: ".\"" <> String.replace(token, "\"", "\"\"") <> "\""

  @doc """
  Compile a JSON Pointer into a `json_extract(winning_body_json, …)` expression.

  Returns `{:error, %VialKeeper.Error{}}` when the pointer cannot be compiled.
  """
  @spec json_expression(binary()) :: {:ok, binary()} | {:error, VialKeeper.Error.t()}
  def json_expression(path) when is_binary(path) do
    with {:ok, compiled} <- sqlite_path(path),
         do: {:ok, "json_extract(winning_body_json, #{quote_literal(compiled)})"}
  end

  @doc """
  Compile a structured index field into the SQLite expression-index column pair
  `(json_type(...), json_extract(...))`.

  Returns `{:error, %VialKeeper.Error{}}` when the field's pointer cannot be compiled.
  """
  @spec structured_expression(map() | binary()) ::
          {:ok, binary()} | {:error, VialKeeper.Error.t()}
  def structured_expression(field) do
    path = MapAccess.get(field, :path, field)

    with {:ok, compiled} <- sqlite_path(path) do
      quoted = quote_literal(compiled)

      {:ok, "json_type(winning_body_json, #{quoted}), json_extract(winning_body_json, #{quoted})"}
    end
  end

  @doc "SQL string literal quoting for compiled path fragments."
  @spec quote_literal(binary()) :: binary()
  def quote_literal(value) when is_binary(value),
    do: "'" <> String.replace(value, "'", "''") <> "'"

  @doc """
  Compiles the already-selected positive constraints from one plan scan.

  The planner, rather than this module, decides which predicates are safe and
  complete candidate constraints. This seam only translates those constraints
  to parameterized SQLite fragments.
  """
  @spec compile_scan(map(), [map()]) ::
          {:ok, [{binary(), term()}]} | {:error, VialKeeper.Error.t()}
  def compile_scan(scan, fields) when is_map(scan) and is_list(fields) do
    constraints = Map.get(scan, "constraints", [])

    if is_list(constraints),
      do: reduce_ok(constraints, [], &compile_scan_constraint(&1, &2, fields)),
      else: {:error, VialKeeper.Error.invalid_request("plan scan constraints must be an array")}
  end

  def compile_scan(_scan, _fields),
    do: {:error, VialKeeper.Error.invalid_request("plan scan must be an object")}

  defp compile_scalar_condition(path, "$eq", nil, "null") do
    with {:ok, type_sql} <- json_type_sql(path, "null"),
         do: {:ok, [{"(#{type_sql})", nil}]}
  end

  defp compile_scalar_condition(path, operator, value, type) do
    if type_matches?(value, type) do
      with {:ok, expression} <- json_expression(path),
           {:ok, type_sql} <- json_type_sql(path, type) do
        comparison = operator_sql(operator)
        {:ok, [{"(#{type_sql} AND #{expression} #{comparison} ?)", value}]}
      end
    else
      {:ok, []}
    end
  end

  @doc "SQLite `json_type` predicate for a structured index field type."
  @spec json_type_sql(binary(), binary()) ::
          {:ok, binary()} | {:error, VialKeeper.Error.t()}
  def json_type_sql(path, "string") do
    with {:ok, compiled} <- sqlite_path(path),
         do: {:ok, "json_type(winning_body_json, #{quote_literal(compiled)}) = 'text'"}
  end

  def json_type_sql(path, type) do
    with {:ok, compiled} <- sqlite_path(path) do
      quoted = quote_literal(compiled)

      {:ok,
       case type do
         "number" -> "json_type(winning_body_json, #{quoted}) IN ('integer', 'real')"
         "boolean" -> "json_type(winning_body_json, #{quoted}) IN ('true', 'false')"
         "null" -> "json_type(winning_body_json, #{quoted}) = 'null'"
         _ -> "json_type(winning_body_json, #{quoted}) = 'text'"
       end}
    end
  end

  defp operator_sql("$eq"), do: "="
  defp operator_sql("$gt"), do: ">"
  defp operator_sql("$gte"), do: ">="
  defp operator_sql("$lt"), do: "<"
  defp operator_sql("$lte"), do: "<="

  defp compile_scan_constraint(constraint, acc, fields) when is_map(constraint) do
    path = Map.get(constraint, "path")
    operator = Map.get(constraint, "operator")
    value = Map.get(constraint, "value")

    case Enum.find(fields, &(MapAccess.get(&1, :path) == path)) do
      nil ->
        {:error, VialKeeper.Error.invalid_request("plan scan references an unknown index field")}

      field ->
        with {:ok, compiled} <-
               compile_plan_constraint(path, operator, value, MapAccess.get(field, :type)) do
          {:ok, acc ++ compiled}
        end
    end
  end

  defp compile_scan_constraint(_constraint, _acc, _fields),
    do: {:error, VialKeeper.Error.invalid_request("plan scan constraint must be an object")}

  defp compile_plan_constraint(path, "$in", values, type) when is_list(values) do
    with {:ok, compiled} <-
           reduce_ok(values, [], fn value, inner_acc ->
             compile_in_value(path, type, value, inner_acc)
           end) do
      compile_in_conditions(compiled, [])
    end
  end

  defp compile_plan_constraint(path, "$beginsWith", prefix, "string") when is_binary(prefix) do
    with {:ok, bounds} <- PrefixBounds.bounds(prefix),
         {:ok, expression} <- json_expression(path),
         {:ok, type_sql} <- json_type_sql(path, "string") do
      lower = {"(#{type_sql} AND #{expression} >= ?)", bounds.lower}

      conditions =
        case bounds.upper do
          nil -> [lower]
          upper -> [lower, {"(#{type_sql} AND #{expression} < ?)", upper}]
        end

      {:ok, conditions}
    end
  end

  defp compile_plan_constraint(path, operator, value, type)
       when operator in ["$eq", "$gt", "$gte", "$lt", "$lte"] do
    compile_scalar_condition(path, operator, value, type)
  end

  defp compile_plan_constraint(_path, _operator, _value, _type),
    do: {:error, VialKeeper.Error.invalid_request("plan scan constraint is not pushdown-capable")}

  defp compile_in_value(path, type, value, inner_acc) do
    with {:ok, pairs} <- compile_scalar_condition(path, "$eq", value, type),
         do: {:ok, inner_acc ++ pairs}
  end

  defp compile_in_conditions([], acc), do: {:ok, acc}

  defp compile_in_conditions(compiled, acc) do
    {:ok,
     Enum.concat(acc, [
       {"(" <> Enum.map_join(compiled, " OR ", &elem(&1, 0)) <> ")",
        Enum.map(compiled, &elem(&1, 1))}
     ])}
  end

  defp reduce_ok(enumerable, acc, fun) do
    Enum.reduce_while(enumerable, {:ok, acc}, fn element, {:ok, inner} ->
      case fun.(element, inner) do
        {:ok, result} -> {:cont, {:ok, result}}
        {:error, error} -> {:halt, {:error, error}}
      end
    end)
  end

  defp type_matches?(value, "string"), do: is_binary(value)
  defp type_matches?(value, "number"), do: is_number(value) and not is_boolean(value)
  defp type_matches?(value, "boolean"), do: is_boolean(value)
  defp type_matches?(nil, "null"), do: true
  defp type_matches?(_, _), do: false
end
