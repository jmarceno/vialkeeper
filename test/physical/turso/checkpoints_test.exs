defmodule VialKeeper.Storage.Turso.CheckpointsTest do
  use VialKeeper.Storage.Contracts.Physical.Checkpoints, adapter: VialKeeper.Storage.Turso.Adapter

  @moduletag :turso_physical
end
