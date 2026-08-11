defmodule ElixirDB.Storage.Contract.MemoryViewsTest do
  use ElixirDB.Storage.Contracts.Views, adapter: ElixirDB.Storage.Memory.Adapter
end
