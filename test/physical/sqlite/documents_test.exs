defmodule VialKeeper.StorageAdapter.DocumentsTest do
  use VialKeeper.Storage.Contracts.Physical.Documents, adapter: VialKeeper.Storage.SQLite.Adapter

  @moduletag :sqlite_physical
end
