defmodule VialKeeper.Storage.SQLite.Statements do
  @moduledoc """
  Prepared-statement cache owned by the process that holds the SQLite connection.

  Prepared statements are owned and reused by the database owner process (via
  the connection handle it serializes through).

  A cached statement is handed out ready to bind: ExQLite resets a statement
  itself whenever stepping ends (done, busy, or error), so a statement whose
  previous run finished needs no separate reset call. `checkout/2` marks the
  statement active and `checkin/1` clears the mark once its run has finished.
  A run that never checks in (an exception escaped mid-run) leaves the mark
  behind, and the next checkout on that connection resets that statement
  before anything else touches the connection.
  """
  require VialKeeper.Probe

  alias Exqlite.Sqlite3
  alias VialKeeper.Probe

  @cache_key :vial_keeper_sqlite_statement_cache
  @active_key :vial_keeper_sqlite_active_statement

  @type handle :: reference()

  @doc """
  Returns a prepared statement for `sql`, ready to bind and step.

  Pair every successful checkout with `checkin/1` once the statement has been
  stepped to completion.
  """
  @spec checkout(handle(), binary()) :: {:ok, reference()} | {:error, term()}
  def checkout(conn, sql) when is_binary(sql) do
    :ok = settle_abandoned(conn)
    cache = Process.get({@cache_key, conn}, %{})

    case Map.fetch(cache, sql) do
      {:ok, statement} ->
        activate(conn, statement)

      :error ->
        prepared =
          Probe.measure :sqlite_prepare do
            Sqlite3.prepare(conn, sql)
          end

        with {:ok, statement} <- prepared do
          Process.put({@cache_key, conn}, Map.put(cache, sql, statement))
          activate(conn, statement)
        end
    end
  end

  @doc "Marks the statement checked out on `conn` as finished."
  @spec checkin(handle()) :: :ok
  def checkin(conn) do
    _ = Process.delete({@active_key, conn})
    :ok
  end

  @spec release_all(handle()) :: :ok
  def release_all(conn) do
    cache = Process.get({@cache_key, conn}, %{})

    Enum.each(cache, fn {_sql, statement} ->
      _ = Sqlite3.release(conn, statement)
    end)

    Process.delete({@cache_key, conn})
    _ = Process.delete({@active_key, conn})
    :ok
  end

  @spec cached_count(handle()) :: non_neg_integer()
  def cached_count(conn) do
    map_size(Process.get({@cache_key, conn}, %{}))
  end

  defp activate(conn, statement) do
    _ = Process.put({@active_key, conn}, statement)
    {:ok, statement}
  end

  # A statement abandoned mid-run may still hold an open read cursor (and with
  # it a read transaction). Reset it before the connection is used again.
  defp settle_abandoned(conn) do
    case Process.delete({@active_key, conn}) do
      nil ->
        :ok

      statement ->
        _ =
          Probe.measure :sqlite_reset do
            Sqlite3.reset(statement)
          end

        :ok
    end
  end
end
