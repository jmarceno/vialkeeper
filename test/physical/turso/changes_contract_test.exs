defmodule VialKeeper.Storage.Turso.ChangesContractTest do
  use VialKeeper.Storage.Contracts.Changes, adapter: VialKeeper.Storage.Turso.Adapter

  @moduletag :turso_physical
end
