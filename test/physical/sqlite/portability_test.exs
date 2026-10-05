defmodule VialKeeper.StorageAdapter.PortabilityTest do
  use VialKeeper.Storage.Contracts.Physical.Portability, adapter: VialKeeper.Storage.SQLite.Adapter

  @moduletag :sqlite_physical
end
