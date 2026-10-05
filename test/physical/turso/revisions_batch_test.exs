defmodule VialKeeper.Storage.Turso.RevisionsBatchTest do
  use VialKeeper.Storage.Contracts.Physical.RevisionsBatch,
    adapter: VialKeeper.Storage.Turso.Adapter

  @moduletag :turso_physical
end
