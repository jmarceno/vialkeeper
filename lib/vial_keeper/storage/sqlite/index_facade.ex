defmodule VialKeeper.Storage.SQLite.IndexFacade do
  @moduledoc "Shared adapter-level index facade operations."

  defmacro __using__(_opts) do
    quote do
      alias VialKeeper.Error
      alias VialKeeper.Storage.SQLite.{Adapter, Connection, Indexes}

      @doc false
      @spec create(Adapter.t(), map()) :: {:ok, map()} | {:error, Error.t()}
      def create(adapter, definition),
        do: Adapter.create_index(adapter, definition)

      @spec delete(Adapter.t(), binary()) :: {:ok, map()} | {:error, Error.t()}
      def delete(adapter, id),
        do: Adapter.delete_index(adapter, id)

      @spec rebuild(Adapter.t(), binary()) :: {:ok, map()} | {:error, Error.t()}
      def rebuild(adapter, id),
        do: Adapter.rebuild_index(adapter, id)

      @spec create_physical(Connection.handle(), binary(), map()) ::
              {:ok, map()} | {:error, Error.t()}
      def create_physical(conn, index_id, definition),
        do: Indexes.create(conn, index_id, definition)

      @spec drop(Connection.handle(), map()) :: :ok | {:error, Error.t()}
      def drop(conn, metadata), do: Indexes.drop(conn, metadata)

      @spec integrity(Connection.handle(), map()) :: {:ok, map()} | {:error, Error.t()}
      def integrity(conn, metadata), do: Indexes.integrity(conn, metadata)
    end
  end
end
