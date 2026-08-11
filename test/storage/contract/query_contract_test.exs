defmodule ElixirDB.Storage.Contract.MemoryQueryTest do
  use ElixirDB.Storage.Contracts.Query, adapter: ElixirDB.Storage.Memory.Adapter
end
