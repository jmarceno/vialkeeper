defmodule ElixirDB.Storage.Contract.MemoryDerivedTest do
  use ElixirDB.Storage.Contracts.Derived, adapter: ElixirDB.Storage.Memory.Adapter
end
