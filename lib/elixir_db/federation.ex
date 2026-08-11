defmodule ElixirDB.Federation do
  @moduledoc "Public facade for bounded stateless cross-database queries."

  alias ElixirDB.Federation.Executor
  alias ElixirDB.MapAccess
  alias ElixirDB.Observability.Instrumentation.Federation, as: FederationInstrumentation

  @doc "Executes one explicit federated query."
  @spec query(map()) :: {:ok, map()} | {:error, ElixirDB.Error.t()}
  def query(request) when is_map(request) do
    FederationInstrumentation.query(source_count(request), fn -> Executor.run(request) end)
  end

  @doc "Alias for `query/1` for callers that use service-style naming."
  @spec execute(map()) :: {:ok, map()} | {:error, ElixirDB.Error.t()}
  def execute(request) when is_map(request), do: query(request)

  defp source_count(request) do
    case MapAccess.get(request, :databases) do
      sources when is_list(sources) -> length(sources)
      _ -> nil
    end
  end
end
