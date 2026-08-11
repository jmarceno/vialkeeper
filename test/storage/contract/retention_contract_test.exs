defmodule ElixirDB.Storage.Contract.MemoryRetentionTest do
  use ElixirDB.Storage.Contracts.Retention, adapter: ElixirDB.Storage.Memory.Adapter
end
