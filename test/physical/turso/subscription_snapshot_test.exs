defmodule VialKeeper.Storage.Turso.SubscriptionSnapshotTest do
  use VialKeeper.Storage.Contracts.Physical.SubscriptionSnapshot,
    adapter: VialKeeper.Storage.Turso.Adapter

  @moduletag :turso_physical
end
