defmodule VialKeeper.Runtime.CatalogIndex do
  @moduledoc """
  Transient ETS accelerator for catalog routing lookups.

  Authoritative registration remains in `DatabaseCatalog` and `registrations.json`.
  This table is rebuilt from catalog open/close transitions and is safe to lose
  on crash: callers fall back to the catalog GenServer.
  """

  @table :vial_keeper_catalog_index

  @type status :: :open | :closing

  @doc "Creates the named routing table. Idempotent for catalog restarts."
  @spec setup() :: :ok
  def setup do
    case :ets.whereis(@table) do
      :undefined ->
        _ =
          :ets.new(@table, [
            :named_table,
            :set,
            :public,
            read_concurrency: true,
            write_concurrency: :auto
          ])

        :ok

      _tid ->
        :ok
    end
  end

  @doc "Records that `uuid` is open with `kind`."
  @spec put_open(binary(), atom()) :: :ok
  def put_open(uuid, kind) when is_binary(uuid) and is_atom(kind) do
    true = :ets.insert(@table, {uuid, kind, :open})
    :ok
  end

  @doc "Marks `uuid` as closing so new commands fail without a catalog call."
  @spec mark_closing(binary()) :: :ok
  def mark_closing(uuid) when is_binary(uuid) do
    case lookup(uuid) do
      {:ok, kind, _status} ->
        true = :ets.insert(@table, {uuid, kind, :closing})
        :ok

      :error ->
        :ok
    end
  end

  @doc "Drops `uuid` from the routing table."
  @spec delete(binary()) :: :ok
  def delete(uuid) when is_binary(uuid) do
    true = :ets.delete(@table, uuid)
    :ok
  end

  @doc "Returns `{kind, status}` for an indexed database, or `:error` when absent."
  @spec fetch(binary()) :: {:ok, atom(), status()} | :error
  def fetch(uuid) when is_binary(uuid), do: lookup(uuid)

  defp lookup(uuid) do
    case :ets.lookup(@table, uuid) do
      [{^uuid, kind, status}] -> {:ok, kind, status}
      [] -> :error
    end
  rescue
    ArgumentError -> :error
  end
end
