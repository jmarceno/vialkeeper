defmodule VialKeeper.Storage.SQLite.Transaction do
  @moduledoc """
  SQLite implementation of the storage transaction port.

  Owns BEGIN/COMMIT/ROLLBACK text and Exqlite error translation. Callers receive
  only an opaque `BackendContext`. Write transactions use `BEGIN IMMEDIATE`.
  Snapshots use deferred `BEGIN` and never take the write lock.
  """
  @behaviour VialKeeper.Storage.Ports.Transaction

  alias VialKeeper.Observability.Instrumentation.SQLite
  alias VialKeeper.Observability.Instrumentation.Mutation
  alias VialKeeper.Storage.BackendContext
  alias VialKeeper.Storage.Ports.Errors
  alias VialKeeper.Storage.SQLite.{Adapter, Connection, Context}

  @snapshot_key :vial_keeper_sqlite_snapshot

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
  @spec run_on_adapter(Adapter.t(), (Adapter.t() -> {:ok, term()} | {:error, VialKeeper.Error.t()})) ::
          {:ok, term()} | {:error, VialKeeper.Error.t()}
  def run_on_adapter(%Adapter{conn: conn} = adapter, fun) when is_function(fun, 1) do
    with_transaction_rescue(conn, fn ->
      execute_transaction(adapter, fun, "BEGIN IMMEDIATE", _invalidate_cache? = true)
    end)
  end

  @doc "Runs `fun` inside one deferred SQLite snapshot on `adapter`."
  @spec run_snapshot_on_adapter(
          Adapter.t(),
          (Adapter.t() -> {:ok, term()} | {:error, VialKeeper.Error.t()})
        ) :: {:ok, term()} | {:error, VialKeeper.Error.t()}
  def run_snapshot_on_adapter(%Adapter{conn: conn} = adapter, fun) when is_function(fun, 1) do
    if Process.get({@snapshot_key, conn}) do
      fun.(adapter)
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
    trace? = begin_sql == "BEGIN IMMEDIATE"

    case control(conn, begin_sql, :transaction_begin, trace?) do
      :ok ->
        transaction_body(adapter, fun, invalidate_cache?, trace?)

      {:error, reason} ->
        {:error, Errors.normalize(reason)}
    end
  end

  defp transaction_body(%Adapter{conn: conn} = adapter, fun, invalidate_cache?, trace?) do
    case fun.(adapter) do
      {:ok, value} ->
        commit_transaction(conn, value, invalidate_cache?, trace?)

      {:error, error} ->
        _ = control(conn, "ROLLBACK", :transaction_rollback, trace?)
        maybe_invalidate(conn, invalidate_cache?)
        {:error, Errors.normalize(error)}
    end
  end

  defp commit_transaction(conn, value, invalidate_cache?, trace?) do
    case control(conn, "COMMIT", :transaction_commit, trace?) do
      :ok ->
        maybe_invalidate(conn, invalidate_cache?)
        {:ok, value}

      {:error, reason} ->
        _ = control(conn, "ROLLBACK", :transaction_rollback, trace?)
        maybe_invalidate(conn, invalidate_cache?)
        {:error, Errors.normalize(reason)}
    end
  end

  defp control(conn, sql, :transaction_begin = phase, true) do
    Mutation.phase(:transaction_begin, fn ->
      SQLite.trace_sqlite_phase(phase, fn -> Connection.exec(conn, sql) end)
    end)
  end

  defp control(conn, sql, :transaction_commit = phase, true) do
    Mutation.phase(:transaction_commit, fn ->
      SQLite.trace_sqlite_phase(phase, fn -> Connection.exec(conn, sql) end)
    end)
  end

  defp control(conn, sql, phase, true),
    do: SQLite.trace_sqlite_phase(phase, fn -> Connection.exec(conn, sql) end)

  defp control(conn, sql, _phase, false), do: Connection.exec(conn, sql)

  defp maybe_invalidate(conn, true), do: Adapter.invalidate_identity_cache(conn)
  defp maybe_invalidate(_conn, false), do: :ok

  defp rollback_and_reraise(conn, exception, stacktrace) do
    _ = Connection.exec(conn, "ROLLBACK")
    Adapter.invalidate_identity_cache(conn)
    reraise exception, stacktrace
  end
end
