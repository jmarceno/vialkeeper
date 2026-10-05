defmodule VialKeeper.StorageAdapter.ReadPoolConnectionTest do
  use VialKeeper.Storage.Contracts.Physical.ReadPoolConnection,
    adapter: VialKeeper.Storage.SQLite.Adapter

  @moduletag :sqlite_physical
end
