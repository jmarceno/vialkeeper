defmodule VialKeeper.Storage.Turso.FormatMigrationTest do
  use VialKeeper.Storage.Contracts.Physical.FormatMigration,
    adapter: VialKeeper.Storage.Turso.Adapter

  @moduletag :turso_physical
end
