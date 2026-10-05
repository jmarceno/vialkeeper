defmodule VialKeeper.StorageAdapter.CheckpointsTest do
  use VialKeeper.Storage.Contracts.Physical.Checkpoints, adapter: VialKeeper.Storage.SQLite.Adapter

  @moduletag :sqlite_physical
end
