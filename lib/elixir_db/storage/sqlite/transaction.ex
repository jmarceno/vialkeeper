defmodule ElixirDB.Storage.SQLite.Transaction do
  @moduledoc """
  SQLite implementation of the storage transaction port.

  Owns BEGIN/COMMIT/ROLLBACK text and Exqlite error translation. Callers receive
  only an opaque `BackendContext`. Write transactions use `BEGIN IMMEDIATE`.
  Snapshots use deferred `BEGIN` and never take the write lock.
  """
  @behaviour ElixirDB.Storage.Ports.Transaction

  alias ElixirDB.Observability.Instrumentation.SQLite
  alias ElixirDB.Storage.BackendContext
  alias ElixirDB.Storage.Ports.Errors
  alias ElixirDB.Storage.SQLite.{Adapter, Connection, Context}

  @snapshot_key :elixir_db_sqlite_snapshot

  @rescued_exceptions [
    ArgumentError,
    ArithmeticError,
    BadMapError,
    CaseClauseError,
    ErlangError,
    FunctionClauseError,
    KeyError,
    MatchError,
    Protocol.UndefinedError,
    RuntimeError,
    UndefinedFunctionError,
    WithClauseError
  ]

  @impl true
  def run(%BackendContext{} = context, fun) when is_function(fun, 1) do
    with {:ok, adapter} <- Context.unwrap(context) do
      run_on_adapter(adapter, fn updated_adapter ->
        fun.(Context.replace_ref(context, updated_adapter))
      end)
    end
  end

  @impl true
  def run_snapshot(%BackendContext{} = context, fun) when is_function(fun, 1) do
    with {:ok, adapter} <- Context.unwrap(context) do
      run_snapshot_on_adapter(adapter, fn updated_adapter ->
        fun.(Context.replace_ref(context, updated_adapter))
      end)
    end
  end

  @doc "Runs `fun` atomically against an open SQLite adapter handle."
  @spec run_on_adapter(Adapter.t(), (Adapter.t() -> {:ok, term()} | {:error, ElixirDB.Error.t()})) ::
          {:ok, term()} | {:error, ElixirDB.Error.t()}
  def run_on_adapter(%Adapter{conn: conn} = adapter, fun) when is_function(fun, 1) do
    with_transaction_rescue(conn, fn ->
      execute_transaction(adapter, fun, "BEGIN IMMEDIATE", _invalidate_cache? = true)
    end)
  end

  @doc "Runs `fun` inside one deferred SQLite snapshot on `adapter`."
  @spec run_snapshot_on_adapter(
          Adapter.t(),
          (Adapter.t() -> {:ok, term()} | {:error, ElixirDB.Error.t()})
        ) :: {:ok, term()} | {:error, ElixirDB.Error.t()}
  def run_snapshot_on_adapter(%Adapter{conn: conn} = adapter, fun) when is_function(fun, 1) do
    if Process.get({@snapshot_key, conn}) do
      {:error, ElixirDB.Error.internal_error("nested read snapshot is not allowed")}
    else
      Process.put({@snapshot_key, conn}, true)

      try do
        with_transaction_rescue(conn, fn ->
          execute_transaction(adapter, fun, "BEGIN", _invalidate_cache? = false)
        end)
      after
        Process.delete({@snapshot_key, conn})
      end
    end
  end

  defp with_transaction_rescue(conn, fun) do
    fun.()
  rescue
    exception in @rescued_exceptions ->
      rollback_and_reraise(conn, exception, __STACKTRACE__)
  end

  defp execute_transaction(%Adapter{conn: conn} = adapter, fun, begin_sql, invalidate_cache?) do
    case SQLite.trace_sqlite_phase(:transaction_begin, fn ->
           Connection.execute(conn, begin_sql)
         end) do
      :ok ->
        transaction_body(adapter, fun, invalidate_cache?)

      {:error, reason} ->
        {:error, Errors.normalize(reason)}
    end
  end

  defp transaction_body(%Adapter{conn: conn} = adapter, fun, invalidate_cache?) do
    case fun.(adapter) do
      {:ok, value} ->
        commit_transaction(conn, value, invalidate_cache?)

      {:error, error} ->
        _ =
          SQLite.trace_sqlite_phase(:transaction_rollback, fn ->
            Connection.execute(conn, "ROLLBACK")
          end)

        maybe_invalidate(conn, invalidate_cache?)
        {:error, Errors.normalize(error)}
    end
  end

  defp commit_transaction(conn, value, invalidate_cache?) do
    case SQLite.trace_sqlite_phase(:transaction_commit, fn ->
           Connection.execute(conn, "COMMIT")
         end) do
      :ok ->
        maybe_invalidate(conn, invalidate_cache?)
        {:ok, value}

      {:error, reason} ->
        maybe_invalidate(conn, invalidate_cache?)
        {:error, Errors.normalize(reason)}
    end
  end

  defp maybe_invalidate(conn, true), do: Adapter.invalidate_identity_cache(conn)
  defp maybe_invalidate(_conn, false), do: :ok

  defp rollback_and_reraise(conn, exception, stacktrace) do
    _ =
      SQLite.trace_sqlite_phase(:transaction_rollback, fn ->
        Connection.execute(conn, "ROLLBACK")
      end)

    Adapter.invalidate_identity_cache(conn)
    reraise exception, stacktrace
  end
end
