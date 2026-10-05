defmodule VialKeeper.StorageAdapter.FormatMigrationTest do
  use VialKeeper.Storage.Contracts.Physical.FormatMigration,
    adapter: VialKeeper.Storage.SQLite.Adapter

  @moduletag :sqlite_physical
end
