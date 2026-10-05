defmodule VialKeeper.Storage.Turso.ReadPoolConnectionTest do
  use VialKeeper.Storage.Contracts.Physical.ReadPoolConnection,
    adapter: VialKeeper.Storage.Turso.Adapter

  @moduletag :turso_physical
end
