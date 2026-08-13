defmodule ElixirDB.ShadowSource do
  @moduledoc "Opens and tears down ordinary source databases for shadow tests."

  alias ElixirDB.Runtime.DatabaseCatalog
  alias ElixirDB.Shadow.RouteTable

  @spec open!(binary()) :: {binary(), binary()}
  def open!(prefix) when is_binary(prefix) do
    source_uuid = ElixirDB.UUID.v4()
    path = "#{prefix}-#{System.unique_integer([:positive])}.elixirdb"
    {:ok, _} = DatabaseCatalog.create(path, %{database_uuid: source_uuid})
    {:ok, _} = DatabaseCatalog.open(source_uuid)
    {source_uuid, path}
  end

  @spec close!(binary(), binary() | nil) :: :ok
  def close!(source_uuid, path) when is_binary(source_uuid) do
    RouteTable.delete(source_uuid)
    _ = DatabaseCatalog.close(source_uuid)
    _ = DatabaseCatalog.unregister(source_uuid)

    if is_binary(path) do
      ElixirDB.TempDatabase.cleanup(Path.join(ElixirDB.Config.database_root(), path))
    end

    :ok
  end
end
