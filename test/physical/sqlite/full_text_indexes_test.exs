defmodule VialKeeper.StorageAdapter.FullTextIndexesTest do
  use VialKeeper.Storage.Contracts.Physical.FullTextIndexes,
    adapter: VialKeeper.Storage.SQLite.Adapter

  @moduletag :sqlite_physical
end
