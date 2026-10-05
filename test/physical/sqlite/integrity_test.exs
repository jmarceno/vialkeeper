defmodule VialKeeper.StorageAdapter.IntegrityTest do
  use VialKeeper.Storage.Contracts.Physical.Integrity, adapter: VialKeeper.Storage.SQLite.Adapter

  @moduletag :sqlite_physical
end
