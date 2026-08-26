defmodule VialKeeper.Federation do
  @moduledoc "Internal bounded cross-database query service used by HTTP routes; not a client API."

  alias VialKeeper.Federation.Executor
  alias VialKeeper.MapAccess
  alias VialKeeper.Observability.Instrumentation.Federation, as: FederationInstrumentation

  @doc "Executes one explicit federated query."
  @spec query(map()) :: {:ok, map()} | {:error, VialKeeper.Error.t()}
  def query(request) when is_map(request) do
    FederationInstrumentation.query(source_count(request), fn -> Executor.run(request) end)
  end

  @doc "Alias for `query/1` for callers that use service-style naming."
  @spec execute(map()) :: {:ok, map()} | {:error, VialKeeper.Error.t()}
  def execute(request) when is_map(request), do: query(request)

  defp source_count(request) do
    case MapAccess.get(request, :databases) do
      sources when is_list(sources) -> length(sources)
      _ -> nil
    end
  end
end
