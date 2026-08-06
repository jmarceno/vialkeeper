defmodule ElixirDB.Storage.SQLite.QueryCompiler do
  @moduledoc "Validated hand-off from the storage-neutral query contract to SQLite."

  alias ElixirDB.Query.Normalizer

  @spec compile(map()) :: {:ok, map()} | {:error, ElixirDB.Error.t()}
  def compile(request) when is_map(request), do: Normalizer.normalize(request)

  def compile(_), do: {:error, ElixirDB.Error.invalid_request("query must be an object")}
end
