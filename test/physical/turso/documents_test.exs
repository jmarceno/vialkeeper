defmodule VialKeeper.Storage.Turso.DocumentsTest do
  use VialKeeper.Storage.Contracts.Physical.Documents, adapter: VialKeeper.Storage.Turso.Adapter

  @moduletag :turso_physical
end
