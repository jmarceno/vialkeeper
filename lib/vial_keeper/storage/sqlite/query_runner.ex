defmodule VialKeeper.Storage.SQLite.QueryRunner do
  @moduledoc """
  SQLite adapter entry for query and explain.

  Candidate SQL, FTS5 MATCH, term-blob decoding, and index DDL remain in
  `VialKeeper.Storage.SQLite.IndexCandidates` and related SQLite modules. Shared
  filtering, ordering, cursors, projection, deadlines, and explain shaping live
  in `VialKeeper.Query.Executor` via `VialKeeper.Storage.Services.Query`.
  """

  alias VialKeeper.Storage.Services
  alias VialKeeper.Storage.SQLite.{Adapter, Connection}

  @doc """
  Clears process-local query candidate caches for a SQLite connection.

  Plan shaping no longer uses a connection-keyed plan cache; candidate SQL and
  body-term memoization live in `VialKeeper.JSON.StrictCache`.
  """
  @spec clear_cache(Connection.handle()) :: :ok
  def clear_cache(_conn) do
    Process.delete({:vial_keeper_json_strict_cache, :query_candidate_sql})
    Process.delete({:vial_keeper_json_strict_cache, :query_body_term})
    :ok
  end

  @doc "Executes a normalized query request against an open adapter."
  @spec execute(map(), map()) :: {:ok, map()} | {:error, VialKeeper.Error.t()}
  def execute(adapter, request), do: execute(adapter, request, nil)

  @spec execute(map(), map(), map() | nil) :: {:ok, map()} | {:error, VialKeeper.Error.t()}
  def execute(adapter, request, supplied_identity) do
    Services.execute_query(
      Adapter.to_context(adapter),
      request,
      supplied_identity || adapter_identity(adapter)
    )
  end

  @doc "Executes a bounded subscription snapshot query against an open adapter."
  @spec subscription_snapshot(map(), map()) :: {:ok, map()} | {:error, VialKeeper.Error.t()}
  def subscription_snapshot(adapter, request), do: subscription_snapshot(adapter, request, nil)

  @spec subscription_snapshot(map(), map(), map() | nil) ::
          {:ok, map()} | {:error, VialKeeper.Error.t()}
  def subscription_snapshot(adapter, request, supplied_identity) do
    Services.execute_subscription_snapshot(
      Adapter.to_context(adapter),
      request,
      supplied_identity || adapter_identity(adapter)
    )
  end

  @doc "Builds an explain payload for a query request."
  @spec explain(map(), map()) :: {:ok, map()} | {:error, VialKeeper.Error.t()}
  def explain(adapter, request) do
    Services.explain_query(
      Adapter.to_context(adapter),
      request,
      adapter_identity(adapter)
    )
  end

  defp adapter_identity(adapter) do
    case Adapter.identity(adapter) do
      {:ok, value} -> value
      _ -> %{current_sequence: 0, config: VialKeeper.Config.defaults()}
    end
  end
end
