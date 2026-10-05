defmodule VialKeeper.Storage.Turso.CompactionTest do
  use VialKeeper.Storage.Contracts.Physical.Compaction, adapter: VialKeeper.Storage.Turso.Adapter

  @moduletag :turso_physical
end
