defmodule VialKeeper.StorageAdapter.CorruptOpenTest do
  use VialKeeper.Storage.Contracts.Physical.CorruptOpen, adapter: VialKeeper.Storage.SQLite.Adapter

  @moduletag :sqlite_physical
end
