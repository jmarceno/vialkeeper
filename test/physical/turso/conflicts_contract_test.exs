defmodule VialKeeper.Storage.Turso.ConflictsContractTest do
  use VialKeeper.Storage.Contracts.Conflicts, adapter: VialKeeper.Storage.Turso.Adapter

  @moduletag :turso_physical
end
