defmodule VialKeeper.Storage.SQLite.Native do
  @moduledoc """
  SQLite driver NIF (`native/vial_sqlite`, Rust over `rusqlite` with bundled
  SQLite).

  Each statement is one call: prepare from the connection's statement cache,
  bind, step to completion and return every row. `query/3` runs on a dirty IO
  scheduler. `query_inline/3` does the same work on the calling scheduler and
  is only for bounded statements whose dirty hop costs more than SQLite's work;
  it returns `:contended` without running anything when another caller holds
  the connection.

  Only `VialKeeper.Storage.SQLite.Connection` calls this module.
  """
  use Rustler, otp_app: :vial_keeper, crate: :vial_sqlite, path: "native/vial_sqlite"

  @type conn :: reference()
  @type value :: integer() | float() | binary() | nil
  @type param :: value() | {:blob, binary()}
  @type reason :: binary() | :closed

  @doc "Opens `path` (a file name or `file:` URI) with SQLite open `flags`."
  @spec open(binary(), integer()) :: {:ok, conn()} | {:error, reason()}
  def open(_path, _flags), do: :erlang.nif_error(:nif_not_loaded)

  @spec close(conn()) :: :ok | {:error, reason()}
  def close(_conn), do: :erlang.nif_error(:nif_not_loaded)

  @doc "Wakes a caller waiting on a lock and interrupts the running statement."
  @spec cancel(conn()) :: :ok
  def cancel(_conn), do: :erlang.nif_error(:nif_not_loaded)

  @doc "Sets how long a statement waits for another connection's lock (default 2000 ms)."
  @spec set_busy_timeout(conn(), integer()) :: :ok
  def set_busy_timeout(_conn, _timeout_ms), do: :erlang.nif_error(:nif_not_loaded)

  @doc "Runs SQL text that may contain several statements; returns no rows."
  @spec execute(conn(), binary()) :: :ok | {:error, reason()}
  def execute(_conn, _sql), do: :erlang.nif_error(:nif_not_loaded)

  @spec query(conn(), binary(), [param()]) :: {:ok, [[value()]]} | {:error, reason()}
  def query(_conn, _sql, _params), do: :erlang.nif_error(:nif_not_loaded)

  @spec query_inline(conn(), binary(), [param()]) ::
          {:ok, [[value()]]} | {:error, reason()} | :contended
  def query_inline(_conn, _sql, _params), do: :erlang.nif_error(:nif_not_loaded)

  @spec last_insert_rowid(conn()) :: {:ok, integer()} | {:error, reason()}
  def last_insert_rowid(_conn), do: :erlang.nif_error(:nif_not_loaded)

  @doc "Returns the main database as an in-memory image."
  @spec serialize(conn()) :: {:ok, binary()} | {:error, reason()}
  def serialize(_conn), do: :erlang.nif_error(:nif_not_loaded)
end
