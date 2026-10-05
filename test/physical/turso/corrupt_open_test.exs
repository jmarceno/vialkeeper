defmodule VialKeeper.Storage.Turso.CorruptOpenTest do
  use VialKeeper.Storage.Contracts.Physical.CorruptOpen, adapter: VialKeeper.Storage.Turso.Adapter

  @moduletag :turso_physical
end
