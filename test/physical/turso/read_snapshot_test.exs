defmodule VialKeeper.Storage.Turso.ReadSnapshotTest do
  use VialKeeper.Storage.Contracts.Physical.ReadSnapshot, adapter: VialKeeper.Storage.Turso.Adapter

  @moduletag :turso_physical
end
