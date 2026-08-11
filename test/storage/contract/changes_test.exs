defmodule ElixirDB.Storage.Contract.MemoryChangesTest do
  use ElixirDB.Storage.Contracts.Changes, adapter: ElixirDB.Storage.Memory.Adapter
end
