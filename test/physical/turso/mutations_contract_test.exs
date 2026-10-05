defmodule VialKeeper.Storage.Turso.MutationsContractTest do
  use VialKeeper.Storage.Contracts.Mutations,
    adapter: VialKeeper.Storage.Turso.Adapter,
    physical: true

  @moduletag :turso_physical
end
