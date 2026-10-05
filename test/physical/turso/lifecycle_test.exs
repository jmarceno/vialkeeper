defmodule VialKeeper.Storage.Turso.LifecycleTest do
  use VialKeeper.Storage.Contracts.Physical.Lifecycle, adapter: VialKeeper.Storage.Turso.Adapter

  @moduletag :turso_physical
end
