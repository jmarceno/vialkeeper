defmodule ElixirDB.Storage.SQLite.QueryCompiler do
  @moduledoc "Validated hand-off from the storage-neutral query contract to SQLite."

  alias ElixirDB.JSON.Pointer
  alias ElixirDB.Query.Normalizer

  @spec compile(map()) :: {:ok, map()} | {:error, ElixirDB.Error.t()}
  def compile(request) when is_map(request), do: Normalizer.normalize(request)

  def compile(_), do: {:error, ElixirDB.Error.invalid_request("query must be an object")}

  @doc "Compile a validated JSON Pointer into an SQLite `json_extract` / `json_type` path."
  @spec sqlite_path(binary()) :: binary()
  def sqlite_path(path) when is_binary(path) do
    {:ok, tokens} = Pointer.parse(path)

    Enum.reduce(tokens, "$", fn token, acc ->
      acc <> ".\"" <> String.replace(token, "\"", "\"\"") <> "\""
    end)
  end

  @doc "Compile a JSON Pointer into a `json_extract(winning_body_json, …)` expression."
  @spec json_expression(binary()) :: binary()
  def json_expression(path) when is_binary(path) do
    "json_extract(winning_body_json, #{quote_literal(sqlite_path(path))})"
  end

  @doc """
  Compile a structured index field into the SQLite expression-index column pair
  `(json_type(...), json_extract(...))`.
  """
  @spec structured_expression(map() | binary()) :: binary()
  def structured_expression(field) do
    path = field["path"] || field[:path] || field
    path = sqlite_path(path)

    "json_type(winning_body_json, #{quote_literal(path)}), json_extract(winning_body_json, #{quote_literal(path)})"
  end

  @doc "SQL string literal quoting for compiled path fragments."
  @spec quote_literal(binary()) :: binary()
  def quote_literal(value) when is_binary(value),
    do: "'" <> String.replace(value, "'", "''") <> "'"

  @doc """
  Compile selector clauses against structured index fields into `{sql, param}` pairs.
  """
  @spec structured_conditions(map(), [map()]) :: [{binary(), term()}]
  def structured_conditions(selector, fields) do
    Enum.flat_map(selector, fn
      {"$and", clauses} when is_list(clauses) ->
        Enum.flat_map(clauses, &structured_conditions(&1, fields))

      {path, condition} ->
        case Enum.find(fields, fn field -> field["path"] == path end) do
          nil -> []
          field -> field_condition(path, condition, field["type"])
        end
    end)
  end

  @doc "Compile one scalar comparison against a typed JSON path."
  @spec scalar_condition(binary(), binary(), term(), binary()) :: [{binary(), term()}]
  def scalar_condition(path, operator, value, type) do
    if type_matches?(value, type) do
      expression = json_expression(path)
      comparison = operator_sql(operator)
      type_sql = json_type_sql(path, type)
      [{"(#{type_sql} AND #{expression} #{comparison} ?)", value}]
    else
      []
    end
  end

  @doc "SQLite `json_type` predicate for a structured index field type."
  @spec json_type_sql(binary(), binary()) :: binary()
  def json_type_sql(path, "string"),
    do: "json_type(winning_body_json, #{quote_literal(sqlite_path(path))}) = 'text'"

  def json_type_sql(path, type) do
    quoted = quote_literal(sqlite_path(path))

    case type do
      "number" ->
        "json_type(winning_body_json, #{quoted}) IN ('integer', 'real')"

      "boolean" ->
        "json_type(winning_body_json, #{quoted}) IN ('true', 'false')"

      "null" ->
        "json_type(winning_body_json, #{quoted}) = 'null'"

      _ ->
        "json_type(winning_body_json, #{quoted}) = 'text'"
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
    Enum.flat_map(condition, fn
      {operator, value} when operator in ["$eq", "$gt", "$gte", "$lt", "$lte"] ->
        scalar_condition(path, operator, value, type)

      {"$in", values} when is_list(values) ->
        values = Enum.flat_map(values, &scalar_condition(path, "$eq", &1, type))

        case values do
          [] ->
            []

          _ ->
            [
              {"(" <> Enum.map_join(values, " OR ", &elem(&1, 0)) <> ")",
               Enum.map(values, &elem(&1, 1))}
            ]
        end

      _ ->
        []
    end)
  end

  defp field_condition(path, value, type), do: scalar_condition(path, "$eq", value, type)

  defp type_matches?(value, "string"), do: is_binary(value)
  defp type_matches?(value, "number"), do: is_number(value) and not is_boolean(value)
  defp type_matches?(value, "boolean"), do: is_boolean(value)
  defp type_matches?(nil, "null"), do: true
  defp type_matches?(_, _), do: false
end
