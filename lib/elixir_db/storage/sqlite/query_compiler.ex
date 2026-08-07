defmodule ElixirDB.Storage.SQLite.QueryCompiler do
  @moduledoc "Validated hand-off from the storage-neutral query contract to SQLite."

  alias ElixirDB.JSON.Pointer
  alias ElixirDB.MapAccess
  alias ElixirDB.Query.Normalizer

  @spec compile(map()) :: {:ok, map()} | {:error, ElixirDB.Error.t()}
  def compile(request) when is_map(request), do: Normalizer.normalize(request)

  def compile(_), do: {:error, ElixirDB.Error.invalid_request("query must be an object")}

  @doc """
  Compile a validated JSON Pointer into an SQLite `json_extract` / `json_type` path.

  Returns `{:error, %ElixirDB.Error{}}` for an internally-invalid pointer rather than
  raising `MatchError`. HTTP pointers are pre-validated by the `Normalizer`, so an error
  here signals storage-layer misuse, not client input.
  """
  @spec sqlite_path(binary()) :: {:ok, binary()} | {:error, ElixirDB.Error.t()}
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

  Returns `{:error, %ElixirDB.Error{}}` when the pointer cannot be compiled.
  """
  @spec json_expression(binary()) :: {:ok, binary()} | {:error, ElixirDB.Error.t()}
  def json_expression(path) when is_binary(path) do
    with {:ok, compiled} <- sqlite_path(path),
         do: {:ok, "json_extract(winning_body_json, #{quote_literal(compiled)})"}
  end

  @doc """
  Compile a structured index field into the SQLite expression-index column pair
  `(json_type(...), json_extract(...))`.

  Returns `{:error, %ElixirDB.Error{}}` when the field's pointer cannot be compiled.
  """
  @spec structured_expression(map() | binary()) ::
          {:ok, binary()} | {:error, ElixirDB.Error.t()}
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
  Compile selector clauses against structured index fields into `{sql, param}` pairs.

  Returns `{:error, %ElixirDB.Error{}}` if any clause's pointer cannot be compiled.
  """
  @spec structured_conditions(map(), [map()]) ::
          {:ok, [{binary(), term()}]} | {:error, ElixirDB.Error.t()}
  def structured_conditions(selector, fields) do
    reduce_ok(selector, [], fn clause, acc -> compile_clause(clause, acc, fields) end)
  end

  defp compile_clause({"$and", clauses}, acc, fields) when is_list(clauses) do
    # The Normalizer produces $and clauses as a list of selector maps; recurse into each
    # clause (a map of {path, condition} pairs) the same way structured_conditions/2 does.
    reduce_ok(clauses, acc, fn clause, inner_acc -> compile_clause(clause, inner_acc, fields) end)
  end

  defp compile_clause(clause, acc, fields) when is_map(clause) do
    # A $and sub-clause (or a bare selector map) is a set of {path, condition} pairs.
    reduce_ok(clause, acc, fn pair, inner_acc -> compile_clause(pair, inner_acc, fields) end)
  end

  defp compile_clause({path, condition}, acc, fields) do
    case Enum.find(fields, fn field -> field["path"] == path end) do
      nil ->
        {:ok, acc}

      field ->
        with {:ok, compiled} <- field_condition(path, condition, field["type"]),
             do: {:ok, acc ++ compiled}
    end
  end

  @doc "Compile one scalar comparison against a typed JSON path."
  @spec scalar_condition(binary(), binary(), term(), binary()) ::
          {:ok, [{binary(), term()}]} | {:error, ElixirDB.Error.t()}
  def scalar_condition(path, operator, value, type) do
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
          {:ok, binary()} | {:error, ElixirDB.Error.t()}
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

  @doc "Map a selector comparison operator to SQLite."
  @spec operator_sql(binary()) :: binary()
  def operator_sql("$eq"), do: "="
  def operator_sql("$gt"), do: ">"
  def operator_sql("$gte"), do: ">="
  def operator_sql("$lt"), do: "<"
  def operator_sql("$lte"), do: "<="

  defp field_condition(path, condition, type) when is_map(condition) do
    reduce_ok(condition, [], fn
      {operator, value}, acc when operator in ["$eq", "$gt", "$gte", "$lt", "$lte"] ->
        with {:ok, compiled} <- scalar_condition(path, operator, value, type),
             do: {:ok, acc ++ compiled}

      {"$in", values}, acc when is_list(values) ->
        compile_in_operator(path, type, values, acc)

      _clause, acc ->
        {:ok, acc}
    end)
  end

  defp field_condition(path, value, type), do: scalar_condition(path, "$eq", value, type)

  defp compile_in_value(path, type, value, inner_acc) do
    with {:ok, pairs} <- scalar_condition(path, "$eq", value, type),
         do: {:ok, inner_acc ++ pairs}
  end

  defp compile_in_operator(path, type, values, acc) do
    with {:ok, compiled} <-
           reduce_ok(values, [], fn value, inner_acc ->
             compile_in_value(path, type, value, inner_acc)
           end),
         do: compile_in_conditions(compiled, acc)
  end

  defp compile_in_conditions([], acc), do: {:ok, acc}

  defp compile_in_conditions(compiled, acc) do
    {:ok,
     acc ++
       [
         {"(" <> Enum.map_join(compiled, " OR ", &elem(&1, 0)) <> ")",
          Enum.map(compiled, &elem(&1, 1))}
       ]}
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
