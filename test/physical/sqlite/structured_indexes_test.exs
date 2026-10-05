defmodule VialKeeper.StorageAdapter.StructuredIndexesTest do
  use VialKeeper.Storage.Contracts.Physical.StructuredIndexes,
    adapter: VialKeeper.Storage.SQLite.Adapter

  @moduletag :sqlite_physical
end
