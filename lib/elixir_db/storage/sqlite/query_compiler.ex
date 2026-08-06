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
end
