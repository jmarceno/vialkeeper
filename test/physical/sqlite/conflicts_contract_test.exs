defmodule VialKeeper.Storage.SQLite.ConflictsContractTest do
  use VialKeeper.Storage.Contracts.Conflicts, adapter: VialKeeper.Storage.SQLite.Adapter

  @moduletag :sqlite_physical
end
