defmodule ElixirDB.Storage.Contract.MemoryConflictsTest do
  use ElixirDB.Storage.Contracts.Conflicts, adapter: ElixirDB.Storage.Memory.Adapter
end
