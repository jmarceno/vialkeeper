defmodule VialKeeper.Storage.Turso.IntegrityTest do
  use VialKeeper.Storage.Contracts.Physical.Integrity, adapter: VialKeeper.Storage.Turso.Adapter

  @moduletag :turso_physical
end
