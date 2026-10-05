defmodule VialKeeper.Storage.Turso.RevisionTransferContractTest do
  use VialKeeper.Storage.Contracts.RevisionTransfer,
    adapter: VialKeeper.Storage.Turso.Adapter

  @moduletag :turso_physical
end
