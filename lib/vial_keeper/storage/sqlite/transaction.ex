defmodule VialKeeper.Storage.SQLite.Transaction do
  @moduledoc """
  SQLite implementation of the storage transaction port.

  Owns BEGIN/COMMIT/ROLLBACK text and SQLite driver error translation. Callers receive
  only an opaque `BackendContext`. Serial write transactions (`run/2`) use
  `BEGIN IMMEDIATE`, which takes the write lock up front and may run DDL.
  Concurrent write transactions (`run_concurrent/2`) open with the driver's
  `begin_concurrent/0` statement: `BEGIN IMMEDIATE` again on SQLite,
  `BEGIN CONCURRENT` on engines with row-level conflicts. Snapshots use
  deferred `BEGIN` and never take the write lock.

  A serial `run/2` retries a `:write_conflict` up to the driver's
  `serial_conflict_retries/0` times; `run_concurrent/2` returns the conflict
  to its caller, the writer pool, which retries the whole command.
  """
  @behaviour VialKeeper.Storage.Ports.Transaction

  alias VialKeeper.Observability.Instrumentation.Mutation
  alias VialKeeper.Observability.Instrumentation.SQLite
  alias VialKeeper.Storage.BackendContext
  alias VialKeeper.Storage.Ports.Errors
  alias VialKeeper.Storage.SQLite.{Adapter, Changes, Connection, Context, RetentionRecords}

  # quality:reason rollback then reraise is the only control flow after a failed write
  @dialyzer {:nowarn_function, rollback_and_reraise: 3}

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
        fun.(rebind(context, adapter, updated_adapter))
      end)
    end
  end

  @impl true
  def run_concurrent(%BackendContext{} = context, fun) when is_function(fun, 1) do
    with {:ok, adapter} <- Context.unwrap(context) do
      run_concurrent_on_adapter(adapter, fn updated_adapter ->
        fun.(rebind(context, adapter, updated_adapter))
      end)
    end
  end

  @impl true
  def run_snapshot(%BackendContext{} = context, fun) when is_function(fun, 1) do
    with {:ok, adapter} <- Context.unwrap(context) do
      run_snapshot_on_adapter(adapter, fn updated_adapter ->
        fun.(rebind(context, adapter, updated_adapter))
      end)
    end
  end

  # The transaction body normally receives the very adapter the context holds;
  # storing it back would be a handle-server round trip that changes nothing.
  defp rebind(context, adapter, adapter), do: Context.mirror_identity(context, adapter)
  defp rebind(context, _adapter, updated_adapter), do: Context.replace_ref(context, updated_adapter)

  @doc "Runs `fun` atomically against an open SQLite adapter handle."
  @spec run_on_adapter(Adapter.t(), (Adapter.t() -> {:ok, term()} | {:error, VialKeeper.Error.t()})) ::
          {:ok, term()} | {:error, VialKeeper.Error.t()}
  def run_on_adapter(%Adapter{driver: driver} = adapter, fun) when is_function(fun, 1),
    do: run_retrying(adapter, fun, 1, driver.serial_conflict_retries())

  @doc "Runs one concurrent write attempt on `adapter`; a conflict returns `:write_conflict`."
  @spec run_concurrent_on_adapter(
          Adapter.t(),
          (Adapter.t() -> {:ok, term()} | {:error, VialKeeper.Error.t()})
        ) :: {:ok, term()} | {:error, VialKeeper.Error.t()}
  def run_concurrent_on_adapter(%Adapter{driver: driver} = adapter, fun)
      when is_function(fun, 1),
      do: run_write(adapter, fun, driver.begin_concurrent())

  defp run_write(%Adapter{conn: conn} = adapter, fun, begin_sql) do
    :ok = Connection.clear_conflict(conn)

    result =
      with_transaction_rescue(conn, fn ->
        execute_transaction(adapter, fun, begin_sql, _invalidate_cache? = true)
      end)

    conflict_result(result, Connection.take_conflict(conn))
  end

  # A body may wrap the statement's conflict in its own error; the whole
  # transaction rolled back either way, so it reports the conflict.
  defp conflict_result({:error, %VialKeeper.Error{}}, true),
    do: {:error, Errors.normalize(:write_conflict)}

  defp conflict_result(result, _conflicted?), do: result

  @max_backoff_ms 50

  # Same schedule as the writer pool: `min(50, 2^attempt)` ms plus up to 1 ms
  # of jitter between attempts.
  defp run_retrying(adapter, fun, attempt, retries) do
    case run_write(adapter, fun, "BEGIN IMMEDIATE") do
      {:error, %VialKeeper.Error{code: :write_conflict}} when attempt <= retries ->
        Process.sleep(min(@max_backoff_ms, Integer.pow(2, attempt)) + :rand.uniform(2) - 1)
        run_retrying(adapter, fun, attempt + 1, retries)

      result ->
        result
    end
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
    trace? = begin_sql != "BEGIN"

    case control(conn, begin_sql, :transaction_begin, trace?) do
      :ok ->
        transaction_body(adapter, fun, invalidate_cache?, trace?)

      {:error, reason} ->
        {:error, Errors.normalize(reason)}
    end
  end

  defp transaction_body(%Adapter{conn: conn} = adapter, fun, invalidate_cache?, trace?) do
    case body_result(adapter, fun, trace?) do
      {:ok, value} ->
        commit_transaction(conn, value, invalidate_cache?, trace?)

      {:error, error} ->
        _ = control(conn, "ROLLBACK", :transaction_rollback, trace?)
        forget_rolled_back(conn, invalidate_cache?)
        {:error, Errors.normalize(error)}
    end
  end

  # Write transactions hold the write lock for their whole body, which lets
  # point statements step inline (see `Connection.point_query/3`).
  defp body_result(%Adapter{conn: conn} = adapter, fun, true),
    do: Connection.in_write_transaction(conn, fn -> fun.(adapter) end)

  defp body_result(adapter, fun, false), do: fun.(adapter)

  defp commit_transaction(conn, value, invalidate_cache?, trace?) do
    case control(conn, "COMMIT", :transaction_commit, trace?) do
      :ok ->
        maybe_invalidate(conn, invalidate_cache?)
        {:ok, value}

      {:error, reason} ->
        _ = control(conn, "ROLLBACK", :transaction_rollback, trace?)
        forget_rolled_back(conn, invalidate_cache?)
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

  # A rolled-back write may have set state that writer-side caches remembered.
  defp forget_rolled_back(conn, invalidate_cache?) do
    maybe_invalidate(conn, invalidate_cache?)
    _ = Changes.forget_high_water(conn)
    RetentionRecords.forget_pending_local_causal(conn)
  end

  defp rollback_and_reraise(conn, exception, stacktrace) do
    _ = Connection.exec(conn, "ROLLBACK")
    Adapter.invalidate_identity_cache(conn)
    _ = Changes.forget_high_water(conn)
    RetentionRecords.forget_pending_local_causal(conn)
    reraise exception, stacktrace
  end
end
