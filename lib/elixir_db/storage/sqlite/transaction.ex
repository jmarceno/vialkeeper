defmodule ElixirDB.Storage.SQLite.Transaction do
  @moduledoc """
  SQLite implementation of the storage transaction port.

  Owns BEGIN/COMMIT/ROLLBACK text and Exqlite error translation. Callers receive
  only an opaque `BackendContext`.
  """
  @behaviour ElixirDB.Storage.Ports.Transaction

  alias ElixirDB.Observability.Instrumentation.SQLite
  alias ElixirDB.Storage.BackendContext
  alias ElixirDB.Storage.Ports.Errors
  alias ElixirDB.Storage.SQLite.{Adapter, Connection, Context}

  @impl true
  def run(%BackendContext{} = context, fun) when is_function(fun, 1) do
    with {:ok, adapter} <- Context.unwrap(context) do
      run_on_adapter(adapter, fn updated_adapter ->
        fun.(Context.replace_ref(context, updated_adapter))
      end)
    end
  end

  @doc "Runs `fun` atomically against an open SQLite adapter handle."
  @spec run_on_adapter(Adapter.t(), (Adapter.t() -> {:ok, term()} | {:error, ElixirDB.Error.t()})) ::
          {:ok, term()} | {:error, ElixirDB.Error.t()}
  def run_on_adapter(%Adapter{conn: conn} = adapter, fun) when is_function(fun, 1) do
    case SQLite.trace_sqlite_phase(:transaction_begin, fn ->
           Connection.execute(conn, "BEGIN IMMEDIATE")
         end) do
      :ok ->
        transaction_body(adapter, fun)

      {:error, reason} ->
        {:error, Errors.normalize(reason)}
    end
  rescue
    exception in [
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
    ] ->
      _ =
        SQLite.trace_sqlite_phase(:transaction_rollback, fn ->
          Connection.execute(conn, "ROLLBACK")
        end)

      Adapter.invalidate_identity_cache(conn)
      reraise exception, __STACKTRACE__
  end

  defp transaction_body(%Adapter{conn: conn} = adapter, fun) do
    case fun.(adapter) do
      {:ok, value} ->
        commit_transaction(conn, value)

      {:error, error} ->
        _ =
          SQLite.trace_sqlite_phase(:transaction_rollback, fn ->
            Connection.execute(conn, "ROLLBACK")
          end)

        Adapter.invalidate_identity_cache(conn)
        {:error, Errors.normalize(error)}
    end
  end

  defp commit_transaction(conn, value) do
    case SQLite.trace_sqlite_phase(:transaction_commit, fn ->
           Connection.execute(conn, "COMMIT")
         end) do
      :ok ->
        Adapter.invalidate_identity_cache(conn)
        {:ok, value}

      {:error, reason} ->
        Adapter.invalidate_identity_cache(conn)
        {:error, Errors.normalize(reason)}
    end
  end
end
