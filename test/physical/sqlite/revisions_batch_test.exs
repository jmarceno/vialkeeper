defmodule VialKeeper.StorageAdapter.RevisionsBatchTest do
  use VialKeeper.Storage.Contracts.Physical.RevisionsBatch,
    adapter: VialKeeper.Storage.SQLite.Adapter

  @moduletag :sqlite_physical
end
