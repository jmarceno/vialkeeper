defmodule ElixirDB.Federation do
  @moduledoc "Public facade for bounded stateless cross-database queries."

  alias ElixirDB.Federation.Executor

  @doc "Executes one explicit federated query."
  @spec query(map()) :: {:ok, map()} | {:error, ElixirDB.Error.t()}
  def query(request) when is_map(request), do: Executor.run(request)

  @doc "Alias for `query/1` for callers that use service-style naming."
  @spec execute(map()) :: {:ok, map()} | {:error, ElixirDB.Error.t()}
  def execute(request) when is_map(request), do: query(request)
end
