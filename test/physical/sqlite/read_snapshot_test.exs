defmodule VialKeeper.StorageAdapter.ReadSnapshotTest do
  use VialKeeper.Storage.Contracts.Physical.ReadSnapshot, adapter: VialKeeper.Storage.SQLite.Adapter

  @moduletag :sqlite_physical
end
