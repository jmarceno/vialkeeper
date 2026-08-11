defmodule ElixirDB.Storage.SQLite.QueryRunner do
  @moduledoc """
  SQLite adapter entry for query and explain.

  Candidate SQL, FTS5 MATCH, term-blob decoding, and index DDL remain in
  `ElixirDB.Storage.SQLite.IndexCandidates` and related SQLite modules. Shared
  filtering, ordering, cursors, projection, deadlines, and explain shaping live
  in `ElixirDB.Query.Executor` via `ElixirDB.Storage.Services.Query`.
  """

  alias ElixirDB.Storage.Services
  alias ElixirDB.Storage.SQLite.{Adapter, Connection}

  @doc "Clears cached query plans owned by a SQLite connection process."
  @spec clear_cache(Connection.handle()) :: :ok
  def clear_cache(conn) do
    Process.delete({:elixir_db_sqlite_query_plan_cache, conn})
    :ok
  end

  @doc "Executes a normalized query request against an open adapter."
  @spec execute(map(), map()) :: {:ok, map()} | {:error, ElixirDB.Error.t()}
  def execute(adapter, request), do: execute(adapter, request, nil)

  @spec execute(map(), map(), map() | nil) :: {:ok, map()} | {:error, ElixirDB.Error.t()}
  def execute(adapter, request, supplied_identity) do
    Services.execute_query(
      Adapter.to_context(adapter),
      request,
      supplied_identity || adapter_identity(adapter)
    )
  end

  @doc "Executes a bounded subscription snapshot query against an open adapter."
  @spec subscription_snapshot(map(), map()) :: {:ok, map()} | {:error, ElixirDB.Error.t()}
  def subscription_snapshot(adapter, request), do: subscription_snapshot(adapter, request, nil)

  @spec subscription_snapshot(map(), map(), map() | nil) ::
          {:ok, map()} | {:error, ElixirDB.Error.t()}
  def subscription_snapshot(adapter, request, supplied_identity) do
    Services.execute_subscription_snapshot(
      Adapter.to_context(adapter),
      request,
      supplied_identity || adapter_identity(adapter)
    )
  end

  @doc "Builds an explain payload for a query request."
  @spec explain(map(), map()) :: {:ok, map()} | {:error, ElixirDB.Error.t()}
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
      _ -> %{current_sequence: 0, config: ElixirDB.Config.defaults()}
    end
  end
end
