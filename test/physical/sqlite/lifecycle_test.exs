defmodule VialKeeper.StorageAdapter.LifecycleTest do
  use VialKeeper.Storage.Contracts.Physical.Lifecycle, adapter: VialKeeper.Storage.SQLite.Adapter

  @moduletag :sqlite_physical
end
