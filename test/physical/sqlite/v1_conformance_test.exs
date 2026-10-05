defmodule VialKeeper.StorageAdapter.V1ConformanceTest do
  use VialKeeper.Storage.Contracts.Physical.V1Conformance,
    adapter: VialKeeper.Storage.SQLite.Adapter

  @moduletag :sqlite_physical
end
