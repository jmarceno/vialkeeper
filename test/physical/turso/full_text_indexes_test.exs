defmodule VialKeeper.Storage.Turso.FullTextIndexesTest do
  use VialKeeper.Storage.Contracts.Physical.FullTextIndexes,
    adapter: VialKeeper.Storage.Turso.Adapter

  @moduletag :turso_physical
end
