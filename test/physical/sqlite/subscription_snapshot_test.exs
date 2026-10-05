defmodule VialKeeper.StorageAdapter.SubscriptionSnapshotTest do
  use VialKeeper.Storage.Contracts.Physical.SubscriptionSnapshot,
    adapter: VialKeeper.Storage.SQLite.Adapter

  @moduletag :sqlite_physical
end
