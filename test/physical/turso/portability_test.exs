defmodule VialKeeper.Storage.Turso.PortabilityTest do
  use VialKeeper.Storage.Contracts.Physical.Portability, adapter: VialKeeper.Storage.Turso.Adapter

  @moduletag :turso_physical
end
