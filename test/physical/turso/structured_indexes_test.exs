defmodule VialKeeper.Storage.Turso.StructuredIndexesTest do
  use VialKeeper.Storage.Contracts.Physical.StructuredIndexes,
    adapter: VialKeeper.Storage.Turso.Adapter

  @moduletag :turso_physical
end
