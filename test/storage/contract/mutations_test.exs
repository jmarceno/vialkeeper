defmodule ElixirDB.Storage.Contract.MemoryMutationsTest do
  use ElixirDB.Storage.Contracts.Mutations, adapter: ElixirDB.Storage.Memory.Adapter
end
