defmodule VialKeeper.StorageAdapter.CompactionTest do
  use VialKeeper.Storage.Contracts.Physical.Compaction, adapter: VialKeeper.Storage.SQLite.Adapter

  @moduletag :sqlite_physical
end
