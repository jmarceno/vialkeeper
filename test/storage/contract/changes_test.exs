defmodule VialKeeper.Storage.Contract.MemoryChangesTest do
  use VialKeeper.Storage.Contracts.Changes, adapter: VialKeeper.Storage.Memory.Adapter
end
