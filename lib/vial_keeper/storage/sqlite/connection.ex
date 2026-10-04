defmodule VialKeeper.Storage.SQLite.Connection do
  @moduledoc "Private SQLite connection and statement execution primitives."
  require VialKeeper.Probe

  alias Exqlite.Sqlite3
  alias VialKeeper.Probe
  alias VialKeeper.Storage.SQLite.Statements

  @type handle :: reference()

  # Rows are fetched in chunks: each ExQLite call is a dirty-scheduler hop, so
  # one call per chunk instead of one per row (plus a final call to observe
  # completion) is the dominant saving for small and large results alike.
  @fetch_chunk_rows 100

  @write_transaction_key :vial_keeper_sqlite_write_transaction

  @doc "Rows fetched per ExQLite call; benchmarks replay with the same size."
  @spec fetch_chunk_rows() :: pos_integer()
  def fetch_chunk_rows, do: @fetch_chunk_rows

  @spec open(binary(), keyword()) :: {:ok, handle()} | {:error, term()}
  def open(path, opts \\ []) do
    Sqlite3.open(path, opts)
  end

  @spec close(handle() | nil) :: :ok | {:error, term()}
  def close(nil), do: :ok

  def close(handle) do
    # Exqlite documents cancel/1 as part of connection teardown: it wakes a
    # connection blocked in SQLite's busy handler before statements or the
    # database handle are finalized. This matters for short-lived contenders
    # such as the file-lease process, which must not leave a journal/lock
    # behind for the next owner.
    _ = Sqlite3.cancel(handle)
    Statements.release_all(handle)
    Sqlite3.close(handle)
  end

  @spec interrupt(handle()) :: :ok | {:error, term()}
  def interrupt(handle), do: Sqlite3.cancel(handle)

  @spec execute(handle(), iodata(), list()) :: :ok | {:error, term()}
  def execute(conn, sql, params \\ []) do
    case run(conn, sql, params, false) do
      {:ok, _rows} -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  @doc """
  Runs parameterless control SQL without the prepared-statement cache.

  Transaction `BEGIN`/`COMMIT`/`ROLLBACK` must not be cached prepared
  statements; SQLite treats those as connection state changes.
  """
  @spec exec(handle(), binary()) :: :ok | {:error, term()}
  def exec(conn, sql) when is_binary(sql) do
    Probe.measure :sqlite_exec do
      Sqlite3.execute(conn, sql)
    end
  end

  @spec query(handle(), iodata(), list()) :: {:ok, [list()]} | {:error, term()}
  def query(conn, sql, params \\ []), do: run(conn, sql, params, true)

  @doc """
  Like `query/3`, for a point statement: a key lookup or single-row write whose
  work is bounded.

  Inside this process's write transaction on `conn` the statement steps on the
  calling scheduler. The writer already holds the write lock there, so the
  statement cannot wait in the busy handler, and SQLite's work (about a
  microsecond) is smaller than a dirty-scheduler hop. Elsewhere it runs exactly
  like `query/3`.
  """
  @spec point_query(handle(), iodata(), list()) :: {:ok, [list()]} | {:error, term()}
  def point_query(conn, sql, params \\ []), do: run(conn, sql, params, true, stepper(conn))

  @doc "Like `execute/3`, for a point statement (see `point_query/3`)."
  @spec point_execute(handle(), iodata(), list()) :: :ok | {:error, term()}
  def point_execute(conn, sql, params \\ []) do
    case run(conn, sql, params, false, stepper(conn)) do
      {:ok, _rows} -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  @doc """
  Runs `fun` as this process's write transaction body on `conn`.

  Only the transaction owner calls this, after `BEGIN IMMEDIATE` succeeded and
  before `COMMIT` or `ROLLBACK`; point statements run inline while it lasts.
  """
  @spec in_write_transaction(handle(), (-> result)) :: result when result: term()
  def in_write_transaction(conn, fun) when is_function(fun, 0) do
    Process.put({@write_transaction_key, conn}, true)

    try do
      fun.()
    after
      Process.delete({@write_transaction_key, conn})
    end
  end

  defp stepper(conn) do
    case Process.get({@write_transaction_key, conn}) do
      true -> :inline
      nil -> :dirty
    end
  end

  @spec pragma(handle(), binary()) :: {:ok, [list()]} | {:error, term()}
  def pragma(conn, statement), do: query(conn, "PRAGMA " <> statement)

  @doc """
  Checkpoints a disk WAL into the main database file and truncates the sidecar.

  Closed portable bundles must not retain `-wal`/`-shm` files. Memory databases
  and non-WAL connections treat a checkpoint error as a no-op.
  """
  @spec checkpoint(handle()) :: :ok
  def checkpoint(conn) do
    Statements.release_all(conn)

    case query(conn, "PRAGMA wal_checkpoint(TRUNCATE)") do
      {:ok, _} -> :ok
      {:error, _reason} -> :ok
    end
  end

  defp run(conn, sql, params, collect_rows, stepper \\ :dirty) do
    sql = IO.iodata_to_binary(sql)

    with {:ok, statement} <- Statements.checkout(conn, sql) do
      result = bind_and_fetch(conn, statement, params, collect_rows, stepper)
      :ok = Statements.checkin(conn)
      result
    end
  end

  defp bind_and_fetch(conn, statement, params, collect_rows, stepper) do
    with :ok <- bind(statement, params) do
      Probe.measure :sqlite_step do
        fetch(conn, statement, collect_rows, [], stepper)
      end
    end
  end

  defp bind(statement, params) do
    Probe.measure :sqlite_bind do
      if Enum.all?(params, &plain_param?/1),
        do: bind_plain(statement, params),
        else: Sqlite3.bind(statement, params)
    end
  end

  # Parameters of the types storage binds go straight to ExQLite's typed bind
  # calls. `Sqlite3.bind/2` would look up `:exqlite` type extensions in the
  # application environment once per parameter; VialKeeper configures none, so
  # for these types the result is identical. Anything else (dates, atoms,
  # iodata) still goes through `Sqlite3.bind/2`.
  defp plain_param?(value)
       when is_integer(value) or is_float(value) or is_binary(value) or is_nil(value),
       do: true

  defp plain_param?({:blob, value}) when is_binary(value), do: true
  defp plain_param?(_value), do: false

  defp bind_plain(statement, params) do
    param_count = length(params)

    case Sqlite3.bind_parameter_count(statement) do
      ^param_count ->
        bind_plain(statement, params, 1)

      {:error, _reason} = error ->
        error

      count ->
        raise ArgumentError, "expected #{count} arguments, got #{param_count}"
    end
  end

  defp bind_plain(_statement, [], _index), do: :ok

  defp bind_plain(statement, [value | rest], index) do
    :ok = bind_value(statement, index, value)
    bind_plain(statement, rest, index + 1)
  end

  defp bind_value(statement, index, value) when is_integer(value),
    do: Sqlite3.bind_integer(statement, index, value)

  defp bind_value(statement, index, value) when is_float(value),
    do: Sqlite3.bind_float(statement, index, value)

  defp bind_value(statement, index, value) when is_binary(value),
    do: Sqlite3.bind_text(statement, index, value)

  defp bind_value(statement, index, nil), do: Sqlite3.bind_null(statement, index)

  defp bind_value(statement, index, {:blob, value}),
    do: Sqlite3.bind_blob(statement, index, value)

  defp fetch(conn, statement, collect_rows, chunks, stepper) do
    case step(stepper, conn, statement) do
      {:done, rows} ->
        {:ok, collected(rows, chunks, collect_rows)}

      {:rows, rows} ->
        fetch(conn, statement, collect_rows, collect(rows, chunks, collect_rows), stepper)

      {:error, reason} ->
        {:error, reason}

      other ->
        {:error, other}
    end
  end

  defp step(:dirty, conn, statement), do: Sqlite3.multi_step(conn, statement, @fetch_chunk_rows)

  defp step(:inline, conn, statement),
    do: Sqlite3.multi_step_inline(conn, statement, @fetch_chunk_rows)

  defp collect(_rows, chunks, false), do: chunks
  defp collect(rows, chunks, true), do: [rows | chunks]

  defp collected(_rows, _chunks, false), do: []
  defp collected(rows, [], true), do: rows
  defp collected(rows, chunks, true), do: Enum.concat(Enum.reverse([rows | chunks]))
end
