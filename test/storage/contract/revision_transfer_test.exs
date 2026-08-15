defmodule VialKeeper.Storage.Contract.MemoryRevisionTransferTest do
  use VialKeeper.Storage.Contracts.RevisionTransfer,
    adapter: VialKeeper.Storage.Memory.Adapter
end
