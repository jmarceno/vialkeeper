defmodule VialKeeper.Storage.Turso.DerivedContractTest do
  use VialKeeper.Storage.Contracts.Derived, adapter: VialKeeper.Storage.Turso.Adapter

  @moduletag :turso_physical
end
