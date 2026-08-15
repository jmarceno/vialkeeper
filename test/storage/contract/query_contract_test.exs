defmodule VialKeeper.Storage.Contract.MemoryQueryTest do
  use VialKeeper.Storage.Contracts.Query, adapter: VialKeeper.Storage.Memory.Adapter
end
