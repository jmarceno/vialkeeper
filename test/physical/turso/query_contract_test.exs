defmodule VialKeeper.Storage.Turso.QueryContractTest do
  use VialKeeper.Storage.Contracts.Query, adapter: VialKeeper.Storage.Turso.Adapter

  @moduletag :turso_physical
end
