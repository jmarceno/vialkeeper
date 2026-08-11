defmodule ElixirDB.Storage.Contract.MemoryRevisionTransferTest do
  use ElixirDB.Storage.Contracts.RevisionTransfer,
    adapter: ElixirDB.Storage.Memory.Adapter
end
