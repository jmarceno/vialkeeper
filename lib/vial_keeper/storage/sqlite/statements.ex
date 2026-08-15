defmodule VialKeeper.Storage.SQLite.Statements do
  @moduledoc """
  Prepared-statement cache owned by the process that holds the SQLite connection.

  Prepared statements are owned and reused by the database owner process (via
  the connection handle it serializes through).
  """
  alias Exqlite.Sqlite3

  @cache_key :vial_keeper_sqlite_statement_cache

  @type handle :: reference()

  @spec checkout(handle(), binary()) :: {:ok, reference(), :new | :cached} | {:error, term()}
  def checkout(conn, sql) when is_binary(sql) do
    cache = Process.get({@cache_key, conn}, %{})

    case Map.fetch(cache, sql) do
      {:ok, statement} ->
        {:ok, statement, :cached}

      :error ->
        with {:ok, statement} <- Sqlite3.prepare(conn, sql) do
          Process.put({@cache_key, conn}, Map.put(cache, sql, statement))
          {:ok, statement, :new}
        end
    end
  end

  @spec release_all(handle()) :: :ok
  def release_all(conn) do
    cache = Process.get({@cache_key, conn}, %{})

    Enum.each(cache, fn {_sql, statement} ->
      _ = Sqlite3.release(conn, statement)
    end)

    Process.delete({@cache_key, conn})
    :ok
  end

  @spec cached_count(handle()) :: non_neg_integer()
  def cached_count(conn) do
    map_size(Process.get({@cache_key, conn}, %{}))
  end
end
