defmodule VialKeeper.Storage.SQLite.Connection do
  @moduledoc "Private SQLite connection and statement execution primitives."
  require VialKeeper.Probe

  alias VialKeeper.Probe
  alias VialKeeper.Storage.SQLite.Native

  @type handle :: Native.conn()
  @type open_mode :: :readonly | :readwrite | :create

  @write_transaction_key :vial_keeper_sqlite_write_transaction

  # https://www.sqlite.org/c3ref/c_open_autoproxy.html
  @open_flags %{readonly: 0x1, readwrite: 0x2, create: 0x4}

  @doc """
  Opens `path`, a file name or `file:` URI.

  `mode:` lists open modes; the default is `[:readwrite, :create]`.
  """
  @spec open(binary(), mode: [open_mode()]) :: {:ok, handle()} | {:error, term()}
  def open(path, opts \\ []) do
    flags =
      opts
      |> Keyword.get(:mode, [:readwrite, :create])
      |> Enum.reduce(0, &Bitwise.bor(&2, Map.fetch!(@open_flags, &1)))

    Native.open(path, flags)
  end

  @spec close(handle() | nil) :: :ok | {:error, term()}
  def close(nil), do: :ok

  def close(handle) do
    # Cancelling first wakes a connection blocked in the busy handler before
    # the database handle closes. This matters for short-lived contenders such
    # as the file-lease process, which must not leave a journal/lock behind for
    # the next owner.
    :ok = Native.cancel(handle)
    Native.close(handle)
  end

  @spec interrupt(handle()) :: :ok
  def interrupt(handle), do: Native.cancel(handle)

  @doc "Sets how long statements wait for another connection's lock."
  @spec set_busy_timeout(handle(), non_neg_integer()) :: :ok
  def set_busy_timeout(handle, timeout_ms), do: Native.set_busy_timeout(handle, timeout_ms)

  @spec last_insert_rowid(handle()) :: {:ok, integer()} | {:error, term()}
  def last_insert_rowid(handle), do: Native.last_insert_rowid(handle)

  @doc "Returns the main database as an in-memory image."
  @spec serialize(handle()) :: {:ok, binary()} | {:error, term()}
  def serialize(handle), do: Native.serialize(handle)

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
      Native.execute(conn, sql)
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

  @doc "True inside this process's write transaction body on `conn`."
  @spec in_write_transaction?(handle()) :: boolean()
  def in_write_transaction?(conn), do: Process.get({@write_transaction_key, conn}) == true

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
    case query(conn, "PRAGMA wal_checkpoint(TRUNCATE)") do
      {:ok, _} -> :ok
      {:error, _reason} -> :ok
    end
  end

  defp run(conn, sql, params, collect_rows, stepper \\ :dirty) do
    sql = IO.iodata_to_binary(sql)
    params = normalize_params(params)

    Probe.measure :sqlite_step do
      case step(stepper, conn, sql, params) do
        {:ok, rows} when collect_rows -> {:ok, rows}
        {:ok, _rows} -> {:ok, []}
        {:error, reason} -> {:error, reason}
      end
    end
  end

  defp step(:dirty, conn, sql, params), do: Native.query(conn, sql, params)

  defp step(:inline, conn, sql, params) do
    case Native.query_inline(conn, sql, params) do
      :contended -> Native.query(conn, sql, params)
      result -> result
    end
  end

  # Storage binds integers, floats, binaries, nil and `{:blob, binary}`, which
  # go to the NIF as they are. Anything else is converted the way SQLite
  # drivers conventionally store it.
  defp normalize_params(params) do
    if Enum.all?(params, &plain_param?/1), do: params, else: Enum.map(params, &normalize_param/1)
  end

  defp plain_param?(value)
       when is_integer(value) or is_float(value) or is_binary(value) or is_nil(value),
       do: true

  defp plain_param?({:blob, value}) when is_binary(value), do: true
  defp plain_param?(_value), do: false

  defp normalize_param(%Date{} = value), do: Date.to_iso8601(value)
  defp normalize_param(%Time{} = value), do: Time.to_iso8601(value)
  defp normalize_param(%NaiveDateTime{} = value), do: NaiveDateTime.to_iso8601(value)

  defp normalize_param(%DateTime{time_zone: "Etc/UTC"} = value),
    do: value |> DateTime.to_naive() |> NaiveDateTime.to_iso8601()

  defp normalize_param({:blob, value}) when is_list(value), do: {:blob, IO.iodata_to_binary(value)}
  defp normalize_param(value) when is_list(value), do: IO.iodata_to_binary(value)
  defp normalize_param(value) when is_atom(value), do: Atom.to_string(value)
  defp normalize_param(value), do: value
end
